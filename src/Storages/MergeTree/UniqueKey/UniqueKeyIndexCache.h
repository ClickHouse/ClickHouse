#pragma once

#include "config.h"

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>

#include <cstddef>
#include <memory>
#include <string>

#if USE_ROCKSDB
#include <rocksdb/advanced_cache.h>
#endif


namespace DB
{

/// Internal entry type stored in the backing `CacheBase`. One entry per cached
/// object; the weight function returns `charge + overhead` to mirror the
/// `ObjectPtr`'s footprint reported by RocksDB at Insert time.
struct UniqueKeyIndexCacheEntry
{
    /// The object pointer handed to us by RocksDB. Ownership stays with RocksDB
    /// semantically; we only hold a raw pointer + the RocksDB-supplied deleter.
    void * obj = nullptr;
#if USE_ROCKSDB
    const ROCKSDB_NAMESPACE::Cache::CacheItemHelper * helper = nullptr;
#endif
    /// Charge reported at Insert time. Used for GetCharge() and weight function.
    size_t charge = 0;
    /// Memory allocator passed at Insert time (must outlive the cache; RocksDB
    /// contract). Forwarded to helper->del_cb on eviction.
    void * allocator = nullptr;

    UniqueKeyIndexCacheEntry() = default;
    ~UniqueKeyIndexCacheEntry();

    UniqueKeyIndexCacheEntry(const UniqueKeyIndexCacheEntry &) = delete;
    UniqueKeyIndexCacheEntry & operator=(const UniqueKeyIndexCacheEntry &) = delete;
};

struct UniqueKeyIndexCacheEntryWeight
{
    static constexpr size_t OVERHEAD = 64;
    size_t operator()(const UniqueKeyIndexCacheEntry & e) const { return e.charge + OVERHEAD; }
};

/// Backing store: SipHash-keyed CacheBase<UInt128, UniqueKeyIndexCacheEntry>.
/// Key is a 128-bit hash of the RocksDB-supplied Slice key (SipHash128).
/// Collisions at 128 bits are irrelevant for an in-process cache; RocksDB's
/// own block cache uses similar-strength hashing for sharding.
class UniqueKeyIndexCacheBacking : public CacheBase<UInt128, UniqueKeyIndexCacheEntry, UInt128TrivialHash, UniqueKeyIndexCacheEntryWeight>
{
private:
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

using UniqueKeyIndexCacheBackingPtr = std::shared_ptr<UniqueKeyIndexCacheBacking>;

/// Hash a byte range into a 128-bit cache key.
UInt128 uniqueKeyIndexHashKey(const char * data, size_t size);

#if USE_ROCKSDB

/// Adapter that implements the minimal `rocksdb::Cache` surface required for
/// use as `BlockBasedTableOptions::block_cache`, delegating storage to a
/// ClickHouse `CacheBase<UInt128, UniqueKeyIndexCacheEntry>` so that all memory
/// accounting flows through `system.caches` via `UniqueKeyIndexCacheBytes` /
/// `UniqueKeyIndexCacheEntries` CurrentMetrics.
///
/// Design notes:
///   - Single-sharded. Sharding is delegated to `CacheBase`'s internal mutex;
///     benchmarks will decide later whether to stripe.
///   - `Handle*` lifetime: Lookup allocates a `HandlePin` on the heap holding
///     a `std::shared_ptr<UniqueKeyIndexCacheEntry>` keeping the entry alive and
///     an atomic ref count. `Ref` increments, `Release` decrements and deletes
///     the pin at zero.
///   - Secondary cache: not supported. `create_context`, `StartAsyncLookup`,
///     `WaitAll`, `SecondaryCache*` surfaces are no-ops (inherited defaults).
///   - Eviction callback: `CacheBase::onEntryRemoval` fires RocksDB's
///     `helper->del_cb` for each evicted object.
///
/// The adapter is registered on `Context` alongside `MarkCache`; see
/// `Context::setUniqueKeyIndexCache`.
class UniqueKeyIndexCache : public ROCKSDB_NAMESPACE::Cache
{
public:
    UniqueKeyIndexCache(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        double size_ratio);

    ~UniqueKeyIndexCache() override;

    /// --- rocksdb::Cache surface ---
    const char * Name() const override { return "ClickHouseUniqueKeyIndexCache"; }

    ROCKSDB_NAMESPACE::Status Insert(
        const ROCKSDB_NAMESPACE::Slice & key,
        ObjectPtr obj,
        const CacheItemHelper * helper,
        size_t charge,
        Handle ** handle,
        Priority priority,
        const ROCKSDB_NAMESPACE::Slice & compressed,
        ROCKSDB_NAMESPACE::CompressionType type) override;

    Handle * CreateStandalone(
        const ROCKSDB_NAMESPACE::Slice & key,
        ObjectPtr obj,
        const CacheItemHelper * helper,
        size_t charge,
        bool allow_uncharged) override;

    Handle * Lookup(
        const ROCKSDB_NAMESPACE::Slice & key,
        const CacheItemHelper * helper,
        CreateContext * create_context,
        Priority priority,
        ROCKSDB_NAMESPACE::Statistics * stats) override;

    bool Ref(Handle * handle) override;
    using ROCKSDB_NAMESPACE::Cache::Release;
    bool Release(Handle * handle, bool erase_if_last_ref) override;

    ObjectPtr Value(Handle * handle) override;
    void Erase(const ROCKSDB_NAMESPACE::Slice & key) override;
    uint64_t NewId() override;

    void SetCapacity(size_t capacity) override;
    void SetStrictCapacityLimit(bool strict_capacity_limit) override;
    bool HasStrictCapacityLimit() const override { return strict_capacity_limit; }

    size_t GetCapacity() const override;
    size_t GetUsage() const override;
    size_t GetUsage(Handle * handle) const override;
    size_t GetPinnedUsage() const override;
    size_t GetCharge(Handle * handle) const override;
    const CacheItemHelper * GetCacheItemHelper(Handle * handle) const override;

    void ApplyToAllEntries(
        const std::function<void(const ROCKSDB_NAMESPACE::Slice & key, ObjectPtr obj, size_t charge, const CacheItemHelper * helper)> & callback,
        const ApplyToAllEntriesOptions & opts) override;

    void ApplyToHandle(
        ROCKSDB_NAMESPACE::Cache * cache,
        Handle * handle,
        const std::function<void(const ROCKSDB_NAMESPACE::Slice & key, ObjectPtr obj, size_t charge, const CacheItemHelper * helper)> & callback) override;

    void EraseUnRefEntries() override;

    /// --- ClickHouse-side helpers ---
    /// Proportional setter used by `Context::updateUniqueKeyIndexCacheConfiguration`
    /// when the server-side config changes at runtime.
    void setMaxSizeInBytes(size_t bytes);

    /// Clear the cache (eviction-style). Does not drop pinned handles.
    void clear();

    UniqueKeyIndexCacheBacking & getBacking() { return *backing; }

private:
    UniqueKeyIndexCacheBackingPtr backing;
    std::atomic<bool> strict_capacity_limit{false};
    std::atomic<uint64_t> next_id{1};
};

using UniqueKeyIndexCachePtr = std::shared_ptr<UniqueKeyIndexCache>;

#else

/// Stub type so Context.h can forward-declare a consistent shape.
class UniqueKeyIndexCache;
using UniqueKeyIndexCachePtr = std::shared_ptr<UniqueKeyIndexCache>;

#endif

}
