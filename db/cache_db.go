package db

import (
	listtype "container/list"
	"fmt"
	"sort"
	"sync"
)

// TODO: include async batch writes

type CacheDB struct {
	db           DB
	mtx          sync.Mutex
	cache        map[string][]byte
	dirty        map[string]bool
	lruNodes     map[string]*listtype.Element
	lru          *listtype.List
	maxCacheSize int
	itrCtr       int
	itrCtrMtx    sync.Mutex
}

func init() {
	println("REGISTERING CACHE DB")
	dbCreator := func(name string, dir string) (DB, error) {
		return NewCacheDB(name, dir, "goleveldb")
	}
	registerDBCreator(CacheDBBackend, dbCreator, false)
}

// DB that implements an LRU caching mechanism
// Uses memBatch for batching operations
func NewCacheDB(name string, dir string, dbBackendType DBBackendType) (*CacheDB, error) {
	return &CacheDB{
		db:           NewDB(name, dbBackendType, dir),
		cache:        make(map[string][]byte),
		dirty:        make(map[string]bool),
		lruNodes:     make(map[string]*listtype.Element),
		lru:          listtype.New(),
		maxCacheSize: 100,
		itrCtr:       0}, nil
}

// DB first checks the cache.
// If it is not there, it checks the concurrent flush cache, which is used for asynchronous updates to the backend.
// Finally, if the concurrent flush cache does not have it, then check the backend DB
// Note that a nil as a value represents a delete

// Implements DB
func (db *CacheDB) Get(key []byte) []byte {
	return db._GetWithOpts(key, false)
}

// Allows one to retreive data while skipping the cache mutation step. This is important in the iterator implementation,
// and if you want accesses that do not reference the cache
func (db *CacheDB) _GetWithOpts(key []byte, skipCacheMutation bool) []byte {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	key = nonNilBytes(key)
	k := string(key)

	// Check cache
	if val, ok := db._GetFromCache(k, skipCacheMutation); ok {
		return val
	}

	// Check actual db
	return db.db.Get(key)
}

// Implements DB
func (db *CacheDB) Has(key []byte) bool {
	key = nonNilBytes(key)
	k := string(key)

	// Check cache
	if val, ok := db._GetFromCache(k, false); ok && val != nil  {
		return true
	}

	// Check actual db
	return db.db.Has(key)
}

// Implements DB
func (db *CacheDB) Set(key []byte, val []byte) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	db.SetSync(key, val)
}

// Implements DB
func (db *CacheDB) SetSync(key []byte, val []byte) {
	db.SetNoLock(key, val)
}

// Implements atomicSetDeleter
func (db *CacheDB) SetNoLock(key, val []byte) {
	db.SetNoLockSync(key, val)
}

// Implements atomicSetDeleter
func (db *CacheDB) SetNoLockSync(key, val []byte) {
	key = nonNilBytes(key)
	val = nonNilBytes(val)

	k := string(key)
	if elem, ok := db.lruNodes[k]; ok {
		db.lru.MoveToFront(elem)
	} else {
		db.lruNodes[k] = db.lru.PushFront(k)
	}
	db.cache[k] = val
	db.dirty[k] = true
	db._FlushCache()
}

// First set value to nil in cache
// Then wait for propagation down to lowest level

// Implements DB
func (db *CacheDB) Delete(key []byte) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	db.DeleteNoLock(key)
}

// Implements DB
func (db *CacheDB) DeleteSync(key []byte) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	db.DeleteNoLock(key)
}

// Implements atomicSetDeleter
func (db *CacheDB) DeleteNoLock(key []byte) {
	db.DeleteNoLockSync(key)
}

// Implements atomicSetDeleter
func (db *CacheDB) DeleteNoLockSync(key []byte) {
	key = nonNilBytes(key)

	k := string(key)
	if elem, ok := db.lruNodes[k]; ok {
		db.lru.MoveToFront(elem)
	} else {
		db.lruNodes[k] = db.lru.PushFront(k)
	}
	db.cache[k] = nil
	db.dirty[k] = true
	db._FlushCache()
}

// Implements DB
func (db *CacheDB) Iterator(start, end []byte) Iterator {
	return newCacheDBIterator(db, db.GetSortedCachedKeys(start, end), start, end, false)
}

// Implements DB
func (db *CacheDB) ReverseIterator(start, end []byte) Iterator {
	return newCacheDBIterator(db, db.GetSortedCachedKeys(start, end), start, end, true)
}

// Implements DB
func (db *CacheDB) Close() {
	// Write everything in cache to the backend.
	db.maxCacheSize = 0
	db._FlushCache()
	db.db.Close()
}

// Implements DB
func (db *CacheDB) NewBatch() Batch {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	return &memBatch{db, nil}
}

func (db *CacheDB) Print() {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	println("Cached KV Pairs:")
	for key, value := range db.cache {
		fmt.Printf("[%X]:\t[%X]\n", []byte(key), value)
	}
	println ("Not Cached KV Pairs:")
	db.db.Print()
}

func (db *CacheDB) Stats() map[string]string {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	stats := make(map[string]string)
	stats["database.type"] = "cacheDB"
	return stats
}

// Implements atomicSetDeleter
func (db *CacheDB) Mutex() *sync.Mutex {
	return &(db.mtx)
}

func (db *CacheDB) GetSortedCachedKeys(start, end []byte) []string {
	keys := []string{}
	for key := range db.cache {
		inDomain := IsKeyInDomain([]byte(key), start, end)
		if inDomain {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// Mutate iterator count. DO NOT USE UNLESS YOU KNOW WHAT YOU ARE DOING! Can lead to large unintended memory leaks
func (db *CacheDB) _ChangeIterCount(numIters int) {
	db.itrCtrMtx.Lock()
	db.itrCtr += numIters
	db.itrCtrMtx.Unlock()
}

// Attempt to retrieve value from cache, and mutate LRU if found.
func (db *CacheDB) _GetFromCache(k string, skipCacheMutation bool) ([]byte, bool) {
	var val []byte
	if elem, ok := db.lruNodes[k]; ok {
		db.lru.MoveToFront(elem)
		val = db.cache[k]
		//if val == nil {
		//	return val, false
		//} else {
		return val, true
		//}
	}
	return val, false
}
func (db *CacheDB) _FlushCache() {
	// TODO
	// Prune the LRU cache
	// Note that if db.itrCtr > 0, pruning should not occur and no flushing should occur.
	db.itrCtrMtx.Lock()
	defer db.itrCtrMtx.Unlock()
	if db.itrCtr > 0 {
		return
	} else {
		// Create batch for underlying db
		batch := db.db.NewBatch()
		// While cache size is exceeded
		for len(db.cache) > db.maxCacheSize {
			lastElem := db.lru.Back()
			k := lastElem.Value.(string)

			// Check if value is dirty and if so perform write
			if db.dirty[k] {
				if db.cache[k] == nil {
					batch.Delete([]byte(k))
				} else {
					batch.Set([]byte(k), db.cache[k])
				}
			}
			// Otherwise remove from cache
			delete(db.cache, k)
			delete(db.dirty, k)
			delete(db.lruNodes, k)
			db.lru.Remove(lastElem)
			batch.Write()
		}
	}
}

type cacheDBIterator struct {
	memKeys           []string
	curMem            int
	backendDB         *CacheDB
	backendDBIterator Iterator
	isReverse         bool
	start             []byte
	end               []byte
	closed            bool
}

// Implements Iterator
// Note that when an iterator is alive, the cache should not flush anything to disk or it may interfere with the
// backendDBIterator.
func newCacheDBIterator(db *CacheDB, memKeys []string, start, end []byte, isReverse bool) (*cacheDBIterator) {
	var backendDBIterator Iterator

	// Mark iterators as alive.
	db._ChangeIterCount(+1)

	// Reverse memKeys if needed and set backend iterator to expected order.
	if isReverse {
		nkeys := len(memKeys)
		for i := 0; i < nkeys/2; i++ {
			temp := memKeys[i]
			memKeys[i] = memKeys[nkeys-i-1]
			memKeys[nkeys-i-1] = temp
		}
		backendDBIterator = db.db.ReverseIterator(start, end)
	} else {
		backendDBIterator = db.db.Iterator(start, end)
	}
	return &cacheDBIterator{
		memKeys:           memKeys,
		curMem:            0,
		backendDB:         db,
		backendDBIterator: backendDBIterator,
		isReverse:         isReverse,
		start:             start,
		end:               end,
		closed:            false,
	}
}

// Implements Iterator
func (itr *cacheDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Implements Iterator
func (itr *cacheDBIterator) Valid() bool {
	// Either there are more memKeys, or there are more values in the backend iterator, or both
	valid := (0 <= itr.curMem && itr.curMem < len(itr.memKeys)) || itr.backendDBIterator.Valid()
	return valid
}

// Implements Iterator
func (itr *cacheDBIterator) Next() {
	itr.assertIsValid()
	// Choose which source is the current source of iterator, and increment it
	if itr.isMemCurr() {
		itr.curMem++
	} else {
		itr.backendDBIterator.Next()
	}
}

// Implements Iterator
func (itr *cacheDBIterator) Key() []byte {
	itr.assertIsValid()
	if itr.isMemCurr() {
		return []byte(itr.memKeys[itr.curMem])
	} else {
		return itr.backendDBIterator.Key()
	}
}

// Implements Iterator
func (itr *cacheDBIterator) Value() []byte {
	itr.assertIsValid()
	curKey := itr.Key()
	return itr.backendDB._GetWithOpts(curKey, true)
}

// Implements Iterator
func (itr *cacheDBIterator) Close() {
	if !itr.closed {
		itr.closed = true
		itr.backendDB._ChangeIterCount(-1)
	}
}

// Returns whether the current value is stored in memory or in iterator
func (itr *cacheDBIterator) isMemCurr() bool {
	// If itr.backendDBIterator is not valid, then itr.memKeys must be the one storing the current key
	if !itr.backendDBIterator.Valid() {
		return true

		// If itr.memKeys is not valid, must be itr.backendDBIterator that is valid.
	} else if itr.curMem >= len(itr.memKeys) {
		return false

		// If both valid, then compare
	} else if itr.isReverse {

		// If itr is reversed, larger of two must be iterated first
		return itr.memKeys[itr.curMem] > string(itr.backendDBIterator.Key())
	} else {

		// If itr is not reversed, smaller of two must be iterated first
		return itr.memKeys[itr.curMem] < string(itr.backendDBIterator.Key())
	}
}

func (itr *cacheDBIterator) assertIsValid() {
	if !itr.Valid() {
		panic("cacheDBIterator is invalid")
	}
}
