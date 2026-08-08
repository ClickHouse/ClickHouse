#pragma once

#include <memory>

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Columns/IColumn.h>
#include <Core/UUID.h>
#include <Storages/MergeTree/MarkRange.h>

namespace ProfileEvents
{
    extern const Event ColumnsCacheHits;
    extern const Event ColumnsCacheMisses;
    extern const Event ColumnsCacheEvictedBytes;
    extern const Event ColumnsCacheEvictedEntries;
}

namespace DB
{

/// Key for looking up cached deserialized columns.
/// Identifies a specific column in a specific row range of a specific data part.
/// Uses Table UUID so that RENAME TABLE properly invalidates the cache.
/// Row ranges (not mark ranges) allow for flexible block sizes and intersection queries.
struct ColumnsCacheKey
{
    UUID table_uuid;
    String part_name;
    String column_name;
    size_t row_begin = 0;
    size_t row_end = 0;

    bool operator==(const ColumnsCacheKey & other) const = default;

    bool intersects(const ColumnsCacheKey & other) const
    {
        return table_uuid == other.table_uuid
            && part_name == other.part_name
            && column_name == other.column_name
            && row_begin < other.row_end
            && row_end > other.row_begin;
    }
};

struct ColumnsCacheKeyHash
{
    size_t operator()(const ColumnsCacheKey & key) const
    {
        SipHash hash;
        hash.update(key.table_uuid);
        hash.update(key.part_name);
        hash.update(key.column_name);
        hash.update(key.row_begin);
        hash.update(key.row_end);
        return hash.get64();
    }
};

/// Cached deserialized column data.
struct ColumnsCacheEntry
{
    ColumnPtr column;
    size_t rows;
};

struct ColumnsCacheWeightFunction
{
    /// Overhead for key storage, hash map entry, shared pointers, etc.
    static constexpr size_t COLUMNS_CACHE_OVERHEAD = 256;

    size_t operator()(const ColumnsCacheEntry & entry) const
    {
        return entry.column->byteSize() + COLUMNS_CACHE_OVERHEAD;
    }
};

extern template class CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

/// Cache of deserialized columns for MergeTree tables.
/// Eliminates the need to read compressed data, decompress, and deserialize
/// for frequently accessed data parts and columns.
/// Supports intersection queries to find cached blocks overlapping with requested row ranges.
class ColumnsCache : public CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>
{
private:
    using Base = CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

    /// Interval index organized by part, then column, then row ranges
    /// This structure makes cleanup efficient when parts are removed
    struct PartIdentifier
    {
        UUID table_uuid;
        String part_name;

        bool operator==(const PartIdentifier & other) const = default;
    };

    struct PartIdentifierHash
    {
        size_t operator()(const PartIdentifier & id) const
        {
            SipHash hash;
            hash.update(id.table_uuid);
            hash.update(id.part_name);
            return hash.get64();
        }
    };

    using IntervalMap = std::map<std::pair<size_t, size_t>, ColumnsCacheKey>;
    using ColumnIntervalsMap = std::unordered_map<String, IntervalMap>;
    using PartIndexMap = std::unordered_map<PartIdentifier, ColumnIntervalsMap, PartIdentifierHash>;

    PartIndexMap interval_index;
    mutable std::mutex interval_index_mutex;

    /// Per-table invalidation generation, advanced by removeTable. See
    /// getInvalidationGeneration. Guarded by interval_index_mutex.
    std::unordered_map<UUID, UInt64> table_generations;

    /// Cache-wide invalidation generation, advanced by clearAll (`SYSTEM DROP
    /// COLUMNS CACHE`). It is folded into the token returned by
    /// getInvalidationGeneration, so a drop also rejects deferred writes from
    /// readers that started before it. Guarded by interval_index_mutex.
    UInt64 global_generation = 0;

    /// The invalidation token a reader captures for a table: the sum of the
    /// cache-wide and the per-table generations. Both components only ever
    /// increase, so the sum increases on every invalidation and a token captured
    /// before an invalidation can never compare equal to the current one.
    /// Must be called with interval_index_mutex held.
    UInt64 currentGeneration(const UUID & table_uuid) const
    {
        auto it = table_generations.find(table_uuid);
        return global_generation + (it == table_generations.end() ? 0 : it->second);
    }

    /// Counts set() calls since the last compaction. Used to amortize the cost of
    /// compactIntervalIndex() across many inserts.
    size_t sets_since_compaction = 0;

    /// Lower bound on the number of set() calls between two compaction sweeps.
    static constexpr size_t MIN_SETS_BETWEEN_COMPACTIONS = 1024;

    /// Run the next compaction sweep after this many set() calls. Recomputed by
    /// compactIntervalIndex as max(MIN_SETS_BETWEEN_COMPACTIONS, number of
    /// surviving indexed ranges), so the sweep interval scales with the index
    /// size: a sweep costs O(entries), the index grows by at most one entry per
    /// set(), so at the next sweep the index holds at most 2 * threshold entries
    /// and the amortized cost per set() stays O(1) no matter how many ranges the
    /// cache holds. Guarded by interval_index_mutex.
    size_t compaction_threshold = MIN_SETS_BETWEEN_COMPACTIONS;

public:
    ColumnsCache(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        size_t max_count,
        double size_ratio);

    /// Look up a cached column. Returns nullptr on miss.
    MappedPtr get(const Key & key)
    {
        auto result = Base::get(key);
        if (result)
            ProfileEvents::increment(ProfileEvents::ColumnsCacheHits);
        else
            ProfileEvents::increment(ProfileEvents::ColumnsCacheMisses);
        return result;
    }

    /// Find all cached entries that intersect with the given row range for a column.
    /// Returns a vector of (cache_key, cached_entry) pairs, sorted by row_begin.
    /// Does NOT update hit/miss profile events; the caller should count at request level.
    std::vector<std::pair<Key, MappedPtr>> getIntersecting(
        const UUID & table_uuid,
        const String & part_name,
        const String & column_name,
        size_t row_begin,
        size_t row_end);

    /// Insert a column into the cache.
    /// Maintains a non-overlapping invariant on the per-column interval map so
    /// that `getIntersecting` runs in O(log N) instead of scanning every entry
    /// before `lower_bound`. See implementation for details.
    /// Returns true if an entry was actually inserted, false if the call was a
    /// no-op (an existing wider interval already covers this range, so nothing
    /// was written; the write was rejected as stale - see below; or the entry
    /// could not stay resident in the cache, e.g. its weight exceeds the cache
    /// size limit). Callers use the return value to avoid charging the per-query
    /// write budget for writes that never landed in the cache.
    ///
    /// `expected_generation` is the invalidation token the caller captured (via
    /// getInvalidationGeneration) when it started reading the data being cached.
    /// If it no longer matches the table's current token, the table (or the whole
    /// cache) was invalidated (e.g. by a `RENAME COLUMN` or a `SYSTEM DROP COLUMNS
    /// CACHE`) after the read started, so this write would repopulate the cache
    /// with stale or just-dropped data and is dropped instead.
    bool set(const Key & key, const MappedPtr & mapped, UInt64 expected_generation);

    /// Current invalidation token for a table. Advances each time removeTable is
    /// called (a metadata change that can remap column names) and each time
    /// clearAll is called (`SYSTEM DROP COLUMNS CACHE`). A reader captures this at
    /// the start of a read and passes it back to `set`, so a deferred write issued
    /// by a reader that started before the invalidation cannot repopulate the
    /// cache with stale data, and cannot resurrect entries an explicit drop
    /// removed.
    UInt64 getInvalidationGeneration(const UUID & table_uuid);

    /// Remove all cached entries for a specific data part.
    /// Should be called when a part is dropped, merged, or mutated.
    void removePart(const UUID & table_uuid, const String & part_name);

    /// Remove all cached entries for a specific table.
    /// Should be called on column metadata changes such as `RENAME COLUMN` that
    /// affect existing cache entries without rewriting parts. Cache keys identify
    /// columns by name, so a `RENAME a TO b; ADD COLUMN a` sequence could otherwise
    /// serve stale data for the freshly added `a`.
    void removeTable(const UUID & table_uuid);

    /// Clear both the base cache and the interval index.
    /// Used by SYSTEM DROP COLUMNS CACHE.
    /// Advances the cache-wide invalidation generation first, so the drop is
    /// sticky: a reader that started before it cannot write its deferred entries
    /// back into the cache afterwards.
    /// Holds interval_index_mutex across both operations so that a concurrent
    /// set() cannot insert into interval_index between the two clears.
    /// This is deadlock-safe because both paths use the same lock order:
    /// interval_index_mutex first, then briefly the CacheBase internal mutex
    /// (taken inside Base::set / Base::clear). There is no lock-order cycle.
    void clearAll()
    {
        std::lock_guard lock(interval_index_mutex);
        ++global_generation;
        Base::clear();
        interval_index.clear();
        sets_since_compaction = 0;
        compaction_threshold = MIN_SETS_BETWEEN_COMPACTIONS;
    }

    /// Lower the maximum size in bytes and immediately compact the interval index
    /// so that entries evicted by the resulting eviction sweep do not leave stale
    /// keys behind. `CacheBase::onEntryRemoval` does not receive the key, so
    /// without an explicit compaction here a runtime config reload that shrinks
    /// the cache would leak metadata indefinitely if no further `set` calls
    /// trigger periodic compaction.
    /// Takes interval_index_mutex first, then the CacheBase internal mutex (inside
    /// Base::setMaxSizeInBytes), matching the lock order of clearAll / set /
    /// removePart so there is no lock-order cycle. Holding interval_index_mutex
    /// across the eviction also makes the eviction and the following compaction
    /// atomic with respect to a concurrent clearAll / set.
    void setMaxSizeInBytesAndCompact(size_t max_size_in_bytes)
    {
        std::lock_guard lock(interval_index_mutex);
        Base::setMaxSizeInBytes(max_size_in_bytes);
        compactIntervalIndex();
        sets_since_compaction = 0;
    }

    /// Metadata for a cache entry, used by system.columns_cache.
    /// Does not hold a shared_ptr to column data, so it does not pin cached columns in memory.
    struct EntryMetadata
    {
        Key key;
        size_t rows = 0;
        size_t bytes = 0;
    };

    /// Get metadata for all cache entries for introspection (system.columns_cache table).
    /// Returns lightweight metadata without holding shared_ptrs to column data.
    std::vector<EntryMetadata> getAllEntriesMetadata();

private:
    /// Remove stale entries from interval_index.
    /// Must be called without holding the CacheBase lock to avoid deadlock.
    void removeStaleKeys(const std::vector<Key> & stale_keys);

    /// Walk the entire interval_index and erase any key that is no longer in Base
    /// (i.e., evicted by LRU). Must be called with interval_index_mutex held.
    /// Cost is O(interval_index entries). Also recomputes compaction_threshold
    /// from the number of surviving entries, which keeps the periodic sweeps in
    /// set() amortized O(1) per call (see compaction_threshold).
    void compactIntervalIndex();

    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override
    {
        ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedEntries);
        ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedBytes, weight_loss);

        /// We can't remove from interval_index here because the eviction callback
        /// doesn't provide the key. Stale entries are cleaned up lazily in
        /// getIntersecting, eagerly in set/removePart, and via periodic compaction
        /// driven by sets_since_compaction in set() (see compactIntervalIndex).
    }
};

using ColumnsCachePtr = std::shared_ptr<ColumnsCache>;

/// Per-query shared accounting for columns cache writes.
/// One instance is created per query (in Context::makeQueryContext) and shared
/// by all of that query's read pools, so the documented per-query budgets
/// (`columns_cache_max_bytes_to_write_to_cache` and
/// `columns_cache_max_estimated_compressed_bytes_to_write_to_cache`) apply to the
/// query as a whole rather than to each `MergeTreeReadPoolBase` independently.
/// Without this, a query with several MergeTree read pipelines (for example a
/// `JOIN`, `UNION`, or subqueries) would let each pool write up to the full cap,
/// so total writes could exceed the configured budget by a multiple.
struct ColumnsCacheWriteBudget
{
    /// Running total of bytes actually written to the cache by this query.
    std::atomic<size_t> bytes_written{0};

    /// Running total of compressed bytes this query's read pools estimate they
    /// will read, accumulated as the pools are constructed.
    std::atomic<size_t> estimated_bytes{0};

    /// Latches to true once `estimated_bytes` exceeds the estimate budget, after
    /// which every later pool of the query disables cache writes.
    std::atomic<bool> writes_disabled{false};
};

using ColumnsCacheWriteBudgetPtr = std::shared_ptr<ColumnsCacheWriteBudget>;

}
