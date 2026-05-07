#include <Storages/MergeTree/UniqueKey/UniqueKeyIndexCache.h>

#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>

#if USE_ROCKSDB
#include <rocksdb/advanced_cache.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#endif

#include <atomic>
#include <cstring>
#include <functional>

namespace ProfileEvents
{
    extern const Event UniqueKeyIndexCacheLookupMicroseconds;
    extern const Event UniqueKeyIndexCacheHits;
    extern const Event UniqueKeyIndexCacheMisses;
}

namespace DB
{

UInt128 uniqueKeyIndexHashKey(const char * data, size_t size)
{
    SipHash hash;
    hash.update(size);
    hash.update(data, size);
    return hash.get128();
}

UniqueKeyIndexCacheEntry::~UniqueKeyIndexCacheEntry()
{
#if USE_ROCKSDB
    /// RocksDB: each entry's deleter is responsible for destroying `obj` when
    /// the cache evicts it. `helper->del_cb` may be nullptr for
    /// `kNoopCacheItemHelper`-style entries; guard accordingly.
    if (helper && helper->del_cb && obj)
    {
        helper->del_cb(obj, reinterpret_cast<ROCKSDB_NAMESPACE::MemoryAllocator *>(allocator));
    }
#endif
}

#if USE_ROCKSDB

namespace
{

/// `Handle*` returned by Lookup / Insert(with non-null handle). Heap-allocated;
/// freed when its ref count hits zero in `Release`. Holds a shared_ptr to the
/// cached entry so the entry stays alive for as long as the caller pins it,
/// even if the cache itself evicts the entry under capacity pressure.
struct HandlePin
{
    std::shared_ptr<UniqueKeyIndexCacheEntry> entry;
    UInt128 key{};
    std::atomic<int32_t> refs{1};
};

HandlePin * asPin(ROCKSDB_NAMESPACE::Cache::Handle * h)
{
    return reinterpret_cast<HandlePin *>(h);
}

ROCKSDB_NAMESPACE::Cache::Handle * fromPin(HandlePin * p)
{
    return reinterpret_cast<ROCKSDB_NAMESPACE::Cache::Handle *>(p);
}

UInt128 keyOf(const ROCKSDB_NAMESPACE::Slice & key)
{
    return uniqueKeyIndexHashKey(key.data(), key.size());
}

}

UniqueKeyIndexCache::UniqueKeyIndexCache(
    const String & cache_policy,
    CurrentMetrics::Metric size_in_bytes_metric,
    CurrentMetrics::Metric count_metric,
    size_t max_size_in_bytes,
    double size_ratio)
    : backing(std::make_shared<UniqueKeyIndexCacheBacking>(
        cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, size_ratio))
{
}

UniqueKeyIndexCache::~UniqueKeyIndexCache() = default;

ROCKSDB_NAMESPACE::Status UniqueKeyIndexCache::Insert(
    const ROCKSDB_NAMESPACE::Slice & key,
    ObjectPtr obj,
    const CacheItemHelper * helper,
    size_t charge,
    Handle ** handle,
    Priority /*priority*/,
    const ROCKSDB_NAMESPACE::Slice & /*compressed*/,
    ROCKSDB_NAMESPACE::CompressionType /*type*/)
{
    /// strict_capacity_limit is honored conservatively: if the requested charge
    /// already exceeds the cache's max, refuse. CacheBase itself allows going
    /// over the limit briefly during eviction which is fine for BLOCK cache use.
    if (strict_capacity_limit.load(std::memory_order_relaxed))
    {
        const size_t max = backing->maxSizeInBytes();
        if (max != 0 && charge > max)
            return ROCKSDB_NAMESPACE::Status::MemoryLimit();
    }

    auto entry = std::make_shared<UniqueKeyIndexCacheEntry>();
    entry->obj = obj;
    entry->helper = helper;
    entry->charge = charge;

    /// Time the `CacheBase` mutex-protected insert path. Sibling of the
    /// `Lookup` timing; both feed `UniqueKeyIndexCacheLookupMicroseconds`.
    {
        Stopwatch insert_watch;
        backing->set(keyOf(key), entry);
        ProfileEvents::increment(
            ProfileEvents::UniqueKeyIndexCacheLookupMicroseconds,
            insert_watch.elapsedMicroseconds());
    }

    if (handle)
    {
        auto * pin = new HandlePin{entry, keyOf(key), std::atomic<int32_t>{1}};
        *handle = fromPin(pin);
    }

    return ROCKSDB_NAMESPACE::Status::OK();
}

ROCKSDB_NAMESPACE::Cache::Handle *
UniqueKeyIndexCache::CreateStandalone(
    const ROCKSDB_NAMESPACE::Slice & /*key*/,
    ObjectPtr obj,
    const CacheItemHelper * helper,
    size_t charge,
    bool /*allow_uncharged*/)
{
    /// Standalone entries are detached from the shared table; only the returned
    /// handle keeps them alive. Since CacheBase doesn't support that directly,
    /// we implement standalone as "heap-allocated entry that lives in the pin
    /// only" — simple and sufficient for RocksDB's memory-charging use case.
    auto entry = std::make_shared<UniqueKeyIndexCacheEntry>();
    entry->obj = obj;
    entry->helper = helper;
    entry->charge = charge;
    /// Standalone has no shared-table key; an erase-on-release would be a no-op.
    auto * pin = new HandlePin{entry, UInt128{}, std::atomic<int32_t>{1}};
    return fromPin(pin);
}

ROCKSDB_NAMESPACE::Cache::Handle *
UniqueKeyIndexCache::Lookup(
    const ROCKSDB_NAMESPACE::Slice & key,
    const CacheItemHelper * /*helper*/,
    CreateContext * /*create_context*/,
    Priority /*priority*/,
    ROCKSDB_NAMESPACE::Statistics * /*stats*/)
{
    /// Time the `CacheBase` mutex-protected lookup path. The mutex is the
    /// single serialization point for every block-cache read served by
    /// this adapter.
    Stopwatch lookup_watch;
    auto entry = backing->get(keyOf(key));
    ProfileEvents::increment(
        ProfileEvents::UniqueKeyIndexCacheLookupMicroseconds,
        lookup_watch.elapsedMicroseconds());
    if (!entry)
    {
        ProfileEvents::increment(ProfileEvents::UniqueKeyIndexCacheMisses);
        return nullptr;
    }
    ProfileEvents::increment(ProfileEvents::UniqueKeyIndexCacheHits);
    auto * pin = new HandlePin{entry, keyOf(key), std::atomic<int32_t>{1}};
    return fromPin(pin);
}

bool UniqueKeyIndexCache::Ref(Handle * handle)
{
    if (!handle)
        return false;
    asPin(handle)->refs.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool UniqueKeyIndexCache::Release(Handle * handle, bool erase_if_last_ref)
{
    if (!handle)
        return false;
    auto * pin = asPin(handle);
    /// fetch_sub returns the old value; we're the last ref when it returns 1.
    const bool last_ref = pin->refs.fetch_sub(1, std::memory_order_acq_rel) == 1;
    if (!last_ref)
        return false;

    /// Per RocksDB `Cache::Release` contract, the return value is true iff
    /// the cache entry was erased from the shared table. Standalone pins
    /// (`pin->key == 0`) and `erase_if_last_ref == false` always return
    /// false; the pin's local memory is reclaimed regardless.
    ///
    /// Identity-aware erase: a concurrent `Insert` of the same key replaces
    /// the table resident with a new shared_ptr while leaving us pinning
    /// the old entry. Removing by key alone would erase the newer entry —
    /// the predicate overload of `CacheBase::remove` lets us remove ONLY
    /// when the table still holds the same shared_ptr we pinned.
    bool erased = false;
    if (erase_if_last_ref && pin->key != UInt128{})
    {
        const auto & pinned = pin->entry;
        const size_t before = backing->count();
        std::function<bool(const UInt128 &, const std::shared_ptr<UniqueKeyIndexCacheEntry> &)>
            pred = [&](const UInt128 & k, const std::shared_ptr<UniqueKeyIndexCacheEntry> & v)
            {
                return k == pin->key && v == pinned;
            };
        backing->remove(pred);
        erased = backing->count() < before;
    }
    delete pin;
    return erased;
}

ROCKSDB_NAMESPACE::Cache::ObjectPtr UniqueKeyIndexCache::Value(Handle * handle)
{
    return handle ? asPin(handle)->entry->obj : nullptr;
}

void UniqueKeyIndexCache::Erase(const ROCKSDB_NAMESPACE::Slice & key)
{
    backing->remove(keyOf(key));
}

uint64_t UniqueKeyIndexCache::NewId()
{
    return next_id.fetch_add(1, std::memory_order_relaxed);
}

void UniqueKeyIndexCache::SetCapacity(size_t capacity)
{
    backing->setMaxSizeInBytes(capacity);
}

void UniqueKeyIndexCache::SetStrictCapacityLimit(bool v)
{
    strict_capacity_limit.store(v, std::memory_order_relaxed);
}

size_t UniqueKeyIndexCache::GetCapacity() const
{
    return backing->maxSizeInBytes();
}

size_t UniqueKeyIndexCache::GetUsage() const
{
    return backing->sizeInBytes();
}

size_t UniqueKeyIndexCache::GetUsage(Handle * handle) const
{
    if (!handle)
        return 0;
    const auto * pin = asPin(handle);
    return pin->entry->charge + UniqueKeyIndexCacheEntryWeight::OVERHEAD;
}

size_t UniqueKeyIndexCache::GetPinnedUsage() const
{
    /// Approximate: we don't track this separately. Returning 0 is safe for the
    /// BlockBasedTable path — it's an observability metric, not a correctness one.
    return 0;
}

size_t UniqueKeyIndexCache::GetCharge(Handle * handle) const
{
    return handle ? asPin(handle)->entry->charge : 0;
}

const ROCKSDB_NAMESPACE::Cache::CacheItemHelper *
UniqueKeyIndexCache::GetCacheItemHelper(Handle * handle) const
{
    return handle ? asPin(handle)->entry->helper : nullptr;
}

void UniqueKeyIndexCache::ApplyToAllEntries(
    const std::function<void(const ROCKSDB_NAMESPACE::Slice &, ObjectPtr, size_t, const CacheItemHelper *)> & /*callback*/,
    const ApplyToAllEntriesOptions & /*opts*/)
{
    /// RocksDB uses this for block-cache statistics reporting. We don't retain
    /// the original keys in the backing (we hash on Insert), so we cannot
    /// faithfully replay them here. Leaving the callback unfired is correct for
    /// the BlockBasedTable use case — the stats collector treats unreported
    /// entries as "untracked," not as "missing."
}

void UniqueKeyIndexCache::ApplyToHandle(
    ROCKSDB_NAMESPACE::Cache * /*cache*/,
    Handle * handle,
    const std::function<void(const ROCKSDB_NAMESPACE::Slice &, ObjectPtr, size_t, const CacheItemHelper *)> & callback)
{
    if (!handle)
        return;
    const auto * pin = asPin(handle);
    /// Key is unknown post-hashing; pass an empty Slice to keep the callback
    /// signature satisfied. Callers of ApplyToHandle in BlockBasedTable use the
    /// callback for reporting, not routing, so an empty key is tolerated.
    ROCKSDB_NAMESPACE::Slice empty_key;
    callback(empty_key, pin->entry->obj, pin->entry->charge,
             pin->entry->helper);
}

void UniqueKeyIndexCache::EraseUnRefEntries()
{
    backing->clear();
}

void UniqueKeyIndexCache::setMaxSizeInBytes(size_t bytes)
{
    backing->setMaxSizeInBytes(bytes);
}

void UniqueKeyIndexCache::clear()
{
    backing->clear();
}

#endif

}
