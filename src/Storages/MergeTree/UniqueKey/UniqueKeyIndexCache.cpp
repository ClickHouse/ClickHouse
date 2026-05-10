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
    /// Distinguishes standalone (no backing slot) from regular pinned entries.
    /// Cannot infer from `key == UInt128{}` because a real key whose hash is
    /// all-zero would otherwise be misclassified.
    bool is_standalone = false;
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
    auto entry = std::make_shared<UniqueKeyIndexCacheEntry>();
    entry->obj = obj;
    entry->helper = helper;
    entry->charge = charge;

    Stopwatch insert_watch;
    backing->set(keyOf(key), entry);
    ProfileEvents::increment(
        ProfileEvents::UniqueKeyIndexCacheLookupMicroseconds,
        insert_watch.elapsedMicroseconds());

    if (handle)
    {
        auto * pin = new HandlePin{entry, keyOf(key), /*is_standalone=*/false, std::atomic<int32_t>{1}};
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
    /// Best-effort cap (see header): no strict-cap admission. The standalone
    /// handle is always returned with the requested charge.
    auto entry = std::make_shared<UniqueKeyIndexCacheEntry>();
    entry->obj = obj;
    entry->helper = helper;
    entry->charge = charge;
    auto * pin = new HandlePin{entry, UInt128{}, /*is_standalone=*/true, std::atomic<int32_t>{1}};
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

    auto * pin = new HandlePin{entry, keyOf(key), /*is_standalone=*/false, std::atomic<int32_t>{1}};
    ProfileEvents::increment(ProfileEvents::UniqueKeyIndexCacheHits);
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
    if (pin->refs.fetch_sub(1, std::memory_order_acq_rel) != 1)
        return false;

    /// Identity-aware "last reference" erase, per rocksdb::Cache::Release.
    /// Predicate runs under CacheBase's bucket lock; same lock taken by Lookup
    /// (`backing->get`) and Insert (`backing->set`), so during this call no
    /// other thread can copy the table's shared_ptr or insert a new HandlePin
    /// for this entry. Inside the lock the contributors to `pinned.use_count()`
    /// are stable: the table holds one strong ref, our HandlePin holds one,
    /// and any additional Lookup-issued HandlePin contributes one more. So
    /// use_count == 2 means we are the last external pin and it is safe to
    /// erase; > 2 means another live HandlePin exists and we must keep the
    /// entry resident so a fresh Lookup still hits.
    bool erased = false;
    if (erase_if_last_ref && !pin->is_standalone)
    {
        const auto & pinned = pin->entry;
        std::function<bool(const UInt128 &, const std::shared_ptr<UniqueKeyIndexCacheEntry> &)>
            pred = [&](const UInt128 & k, const std::shared_ptr<UniqueKeyIndexCacheEntry> & v)
            {
                const bool matches = (k == pin->key && v == pinned && pinned.use_count() == 2);
                if (matches)
                    erased = true;
                return matches;
            };
        backing->remove(pred);
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

void UniqueKeyIndexCache::SetStrictCapacityLimit(bool value)
{
    strict_capacity_limit.store(value, std::memory_order_relaxed);
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
    /// Standalone entries don't occupy a backing slot, so OVERHEAD doesn't apply.
    if (pin->is_standalone)
        return pin->entry->charge;
    return pin->entry->charge + UniqueKeyIndexCacheEntryWeight::OVERHEAD;
}

size_t UniqueKeyIndexCache::GetPinnedUsage() const
{
    /// Best-effort cap: not tracked. RocksDB's BlockBasedTable hot paths read
    /// this only as a metric, and our consumers don't read it.
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
    /// Known contract gap: rocksdb::Cache spec asks the callback to fire for
    /// every entry, with the original key. The backing here is a CacheBase
    /// keyed by SipHash128 with no public iteration API, so we can't replay
    /// either the keys or the entries. Cache-dump (`CacheDumperImpl`) and the
    /// statistics walker degrade to reporting nothing for this cache; the
    /// BlockBasedTable hot paths don't depend on this surface.
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
