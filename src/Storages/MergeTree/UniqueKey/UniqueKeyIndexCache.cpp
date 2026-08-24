#include <Storages/MergeTree/UniqueKey/UniqueKeyIndexCache.h>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>

#if USE_ROCKSDB
#include <rocksdb/advanced_cache.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#endif

#include <functional>

namespace ProfileEvents
{
    extern const Event UniqueKeyIndexCacheLookupMicroseconds;
    extern const Event UniqueKeyIndexCacheHits;
    extern const Event UniqueKeyIndexCacheMisses;
}

namespace DB
{

#if USE_ROCKSDB

namespace
{

/// One entry per cached object. Weight contributed to the backing is
/// `charge + OVERHEAD`. The destructor invokes the RocksDB-supplied
/// `helper->del_cb` to destroy `obj` when the entry's last `shared_ptr`
/// drops (either via backing eviction or via the last HandlePin going
/// away).
struct UniqueKeyIndexCacheEntry
{
    void * obj = nullptr;
    const ROCKSDB_NAMESPACE::Cache::CacheItemHelper * helper = nullptr;
    size_t charge = 0;

    UniqueKeyIndexCacheEntry() = default;
    ~UniqueKeyIndexCacheEntry()
    {
        /// Per the `rocksdb::Cache` contract, the deleter must be invoked with
        /// the same `MemoryAllocator *` that allocated `obj`, which is whatever
        /// `cache->memory_allocator()` returned at Insert time. This adapter
        /// passes nothing to the base `rocksdb::Cache` constructor, so
        /// `memory_allocator_` defaults to nullptr for the cache's lifetime —
        /// every caller allocates with default `new`, and the deleter must be
        /// given `nullptr` to match. (`Cache::memory_allocator()` is
        /// non-virtual; you can't override it. To add a custom allocator,
        /// pass `std::shared_ptr<MemoryAllocator>` to the base constructor
        /// and thread the pointer onto the entry at Insert time.)
        ///
        /// `helper->del_cb` may be nullptr for `kNoopCacheItemHelper`-style
        /// entries; guard accordingly.
        if (helper && helper->del_cb && obj)
            helper->del_cb(obj, /*allocator=*/nullptr);
    }

    UniqueKeyIndexCacheEntry(const UniqueKeyIndexCacheEntry &) = delete;
    UniqueKeyIndexCacheEntry & operator=(const UniqueKeyIndexCacheEntry &) = delete;
};

struct UniqueKeyIndexCacheEntryWeight
{
    static constexpr size_t OVERHEAD = 64;
    size_t operator()(const UniqueKeyIndexCacheEntry & e) const { return e.charge + OVERHEAD; }
};

/// `Handle*` returned by Insert(handle) / Lookup / CreateStandalone.
/// Heap-allocated; freed by `Release`. Holds a `shared_ptr` to the cached
/// entry so the entry stays alive as long as the caller pins it, even if
/// the backing evicts it under capacity pressure.
///
/// No per-handle refcount: `Ref` is declined (returns false), so each
/// HandlePin has exactly one outstanding "reference" — the implicit one
/// from its constructor. `Release` is therefore always final.
struct HandlePin
{
    std::shared_ptr<UniqueKeyIndexCacheEntry> entry;
    UInt128 key{};
};

HandlePin * asPin(ROCKSDB_NAMESPACE::Cache::Handle * h)
{
    return reinterpret_cast<HandlePin *>(h);
}

ROCKSDB_NAMESPACE::Cache::Handle * fromPin(HandlePin * p)
{
    return reinterpret_cast<ROCKSDB_NAMESPACE::Cache::Handle *>(p);
}

UInt128 hashKey(const ROCKSDB_NAMESPACE::Slice & key)
{
    SipHash hash;
    hash.update(key.size());
    hash.update(key.data(), key.size());
    return hash.get128();
}

}

/// Backing store: SipHash-keyed `CacheBase<UInt128, UniqueKeyIndexCacheEntry>`.
/// Collisions at 128 bits are irrelevant for an in-process cache; RocksDB's
/// own block cache uses similar-strength hashing for sharding. Defined here
/// (not in an anonymous namespace) so that the header's forward declaration
/// matches.
class UniqueKeyIndexCacheBacking : public CacheBase<UInt128, UniqueKeyIndexCacheEntry, UInt128TrivialHash, UniqueKeyIndexCacheEntryWeight>
{
    using Base = CacheBase<UInt128, UniqueKeyIndexCacheEntry, UInt128TrivialHash, UniqueKeyIndexCacheEntryWeight>;
public:
    UniqueKeyIndexCacheBacking(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        double size_ratio)
        : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, /*max_count=*/0, size_ratio)
    {
    }
};

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
    backing->set(hashKey(key), entry);
    ProfileEvents::increment(
        ProfileEvents::UniqueKeyIndexCacheLookupMicroseconds,
        insert_watch.elapsedMicroseconds());

    if (handle)
    {
        auto * pin = new HandlePin{entry, hashKey(key)};
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
    /// Strict mode is permanently off (see header), so per the
    /// `rocksdb::Cache::CreateStandalone` spec the operation always succeeds
    /// with a charged handle; `allow_uncharged` is unused.
    auto entry = std::make_shared<UniqueKeyIndexCacheEntry>();
    entry->obj = obj;
    entry->helper = helper;
    entry->charge = charge;
    /// Standalone handle: not in the backing. Its Release predicate is a
    /// guaranteed no-match (the backing never holds this entry's shared_ptr
    /// and the entry's use_count is 1, not 2), so no special handling needed.
    auto * pin = new HandlePin{entry, UInt128{}};
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
    auto entry = backing->get(hashKey(key));
    ProfileEvents::increment(
        ProfileEvents::UniqueKeyIndexCacheLookupMicroseconds,
        lookup_watch.elapsedMicroseconds());
    if (!entry)
    {
        ProfileEvents::increment(ProfileEvents::UniqueKeyIndexCacheMisses);
        return nullptr;
    }

    auto * pin = new HandlePin{entry, hashKey(key)};
    ProfileEvents::increment(ProfileEvents::UniqueKeyIndexCacheHits);
    return fromPin(pin);
}

bool UniqueKeyIndexCache::Ref(Handle * /*handle*/)
{
    /// Decline additional refs (see header). The spec permits this via
    /// "returns false if the entry could not be refed." Letting Ref always
    /// fail removes per-handle refcounting.
    return false;
}

bool UniqueKeyIndexCache::Release(Handle * handle, bool erase_if_last_ref)
{
    if (!handle)
        return false;
    auto * pin = asPin(handle);

    /// Identity-aware "last reference" erase per the rocksdb::Cache::Release
    /// contract, via the O(1) `CacheBase::removeIfMatches` primitive:
    ///   - `v != pin->entry` means the table resident was replaced under
    ///     the same key by a concurrent Insert: refuse the erase.
    ///   - `pin->entry.use_count() != 3` means there's another live
    ///     external HandlePin: refuse the erase. (The `3` accounts for
    ///     table + caller's HandlePin + `removeIfMatches`'s local copy.)
    bool erased = false;
    if (erase_if_last_ref)
    {
        erased = backing->removeIfMatches(
            pin->key,
            [&](const std::shared_ptr<UniqueKeyIndexCacheEntry> & v)
            {
                return v == pin->entry && pin->entry.use_count() == 3;
            });
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
    backing->remove(hashKey(key));
}

uint64_t UniqueKeyIndexCache::NewId()
{
    return next_id.fetch_add(1, std::memory_order_relaxed);
}

void UniqueKeyIndexCache::SetCapacity(size_t capacity)
{
    backing->setMaxSizeInBytes(capacity);
}

void UniqueKeyIndexCache::SetStrictCapacityLimit(bool /*value*/)
{
    /// No-op: strict-capacity mode is unsupported (see header).
    /// `HasStrictCapacityLimit()` is hardcoded to return false.
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
    return asPin(handle)->entry->charge + UniqueKeyIndexCacheEntryWeight::OVERHEAD;
}

size_t UniqueKeyIndexCache::GetPinnedUsage() const
{
    /// Strict-cap unsupported (see header). Pinned bytes are not tracked
    /// because admission is not gated on them; the only consumer of this
    /// surface in rocksdb is stats reporting (`db/internal_stats.cc`), which
    /// degrades gracefully to 0.
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
    /// Silent no-op. RocksDB calls this from periodic stats collection
    /// (`CacheEntryStatsCollector::CollectStats` -> `InternalStats::Collect-
    /// CacheEntryStats` -> `DBImpl::DumpStats`), so the surface must be
    /// callable without side effects. We can't honor the spec: the backing
    /// is keyed by SipHash128 with no public iteration API on `CacheBase`,
    /// so we have neither the original keys nor a way to walk entries. The
    /// stats walker degrades to reporting empty per-role buckets for this
    /// cache; the BlockBasedTable hot paths don't depend on this surface.
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
    /// signature satisfied. Callers of ApplyToHandle in BlockBasedTable use
    /// the callback for reporting, not routing, so an empty key is tolerated.
    ROCKSDB_NAMESPACE::Slice empty_key;
    callback(empty_key, pin->entry->obj, pin->entry->charge, pin->entry->helper);
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
