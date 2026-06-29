#pragma once

#include "config.h"

#include <Common/CurrentMetrics.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>

#if USE_ROCKSDB
#include <rocksdb/advanced_cache.h>
#endif


namespace DB
{

#if USE_ROCKSDB

class UniqueKeyIndexCacheBacking;

/// Adapter that implements the minimal `rocksdb::Cache` surface required for
/// use as `BlockBasedTableOptions::block_cache`, delegating storage to a
/// ClickHouse `CacheBase` so that all memory accounting flows through
/// `system.caches` via `UniqueKeyIndexCacheBytes` /
/// `UniqueKeyIndexCacheEntries` CurrentMetrics.
///
/// Layering:
///
///   RocksDB BlockBasedTable
///         │  (rocksdb::Cache::Handle*)
///         ▼
///   ┌──────────────────────────────────────────┐
///   │  UniqueKeyIndexCache : rocksdb::Cache    │   public adapter
///   │   backing ──────┐                        │
///   └─────────────────│────────────────────────┘
///                     │
///                     ▼
///   ┌──────────────────────────────────────────┐
///   │  UniqueKeyIndexCacheBacking              │   private impl
///   │  : CacheBase<UInt128, Entry, ...>        │   (.cpp only)
///   │   hashmap<UInt128, shared_ptr<Entry>>    │
///   │   SLRU eviction, byte accounting         │
///   └──────────────────────────────────────────┘
///         │ shared_ptr (table strong ref)
///         ▼
///   ┌──────────────────────────────────────────┐
///   │  UniqueKeyIndexCacheEntry                │   one per cached object
///   │   void *obj  ─> RocksDB Block (KB-scale) │
///   │   helper, charge                         │
///   │   ~Entry() => helper->del_cb             │
///   └──────────────────────────────────────────┘
///         ▲
///         │ shared_ptr (per outstanding Handle*)
///         │
///   ┌──────────────────────────────────────────┐
///   │  HandlePin                               │   one per Insert/Lookup/
///   │   shared_ptr<Entry> entry                │   CreateStandalone call;
///   │   UInt128 key (SipHash128 of slice key)  │   reinterpret_cast'd to
///   └──────────────────────────────────────────┘   rocksdb::Cache::Handle*
///
/// The adapter narrows several `rocksdb::Cache` surfaces to avoid shadow
/// bookkeeping on top of `CacheBase`:
///   - `HasStrictCapacityLimit()` is hardcoded to false;
///     `SetStrictCapacityLimit(...)` is a no-op.
///   - `CreateStandalone(...)` always returns a charged handle. Per the
///     `rocksdb::Cache::CreateStandalone` spec, "if `allow_uncharged==true`
///     or `strict_capacity_limit=false`, the operation always succeeds";
///     since strict mode is permanently off here, `allow_uncharged` is
///     unused.
///   - `Ref(handle)` returns false; the spec allows it ("returns false if
///     the entry could not be refed"). Per-handle refcounting is therefore
///     not maintained and `Release` is unconditionally final.
///   - `GetPinnedUsage()` returns 0 — pinned bytes are not tracked.
///
/// `Release(handle, erase_if_last_ref=true)` honors the spec contract:
/// erases only when the entry's `shared_ptr` use_count and identity confirm
/// this handle is the last external pin and the table resident has not
/// been replaced by a concurrent Insert. Predicate evaluation runs under
/// CacheBase's bucket lock for stability.
///
/// Honoring `Ref` / strict-cap / `CreateStandalone(allow_uncharged=false)`
/// would require shadow bookkeeping on top of `CacheBase` (per-entry pin
/// counts plus a joint get-and-mutate primitive `CacheBase` does not
/// expose). Those surfaces are not exercised by rocksdb's `BlockBasedTable`
/// against a user-provided `block_cache`.
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
    void SetStrictCapacityLimit(bool value) override;
    bool HasStrictCapacityLimit() const override { return false; }

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

private:
    std::shared_ptr<UniqueKeyIndexCacheBacking> backing;
    std::atomic<uint64_t> next_id{1};
};

using UniqueKeyIndexCachePtr = std::shared_ptr<UniqueKeyIndexCache>;

#else

/// Stub type so Context.h can forward-declare a consistent shape.
class UniqueKeyIndexCache;
using UniqueKeyIndexCachePtr = std::shared_ptr<UniqueKeyIndexCache>;

#endif

}
