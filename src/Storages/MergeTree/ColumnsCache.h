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
    size_t row_begin;
    size_t row_end;

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

    /// Counts set() calls since the last compaction. Used to amortize the cost of
    /// compactIntervalIndex() across many inserts.
    size_t sets_since_compaction = 0;

    /// Run a full compaction sweep every this many set() calls. Tuned so that
    /// the amortized cost per set() is O(1): a compaction is O(count), and we
    /// sweep at most once per `count` inserts.
    static constexpr size_t COMPACT_INTERVAL_INDEX_EVERY_N_SETS = 1024;

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
    void set(const Key & key, const MappedPtr & mapped);

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
    /// Holds interval_index_mutex across both operations so that a concurrent
    /// set() cannot insert into interval_index between the two clears.
    /// This is deadlock-safe because both paths use the same lock order:
    /// interval_index_mutex first, then briefly the CacheBase internal mutex
    /// (taken inside Base::set / Base::clear). There is no lock-order cycle.
    void clearAll()
    {
        std::lock_guard lock(interval_index_mutex);
        Base::clear();
        interval_index.clear();
    }

    /// Lower the maximum size in bytes and immediately compact the interval index
    /// so that entries evicted by the resulting eviction sweep do not leave stale
    /// keys behind. `CacheBase::onEntryRemoval` does not receive the key, so
    /// without an explicit compaction here a runtime config reload that shrinks
    /// the cache would leak metadata indefinitely if no further `set` calls
    /// trigger periodic compaction.
    void setMaxSizeInBytesAndCompact(size_t max_size_in_bytes)
    {
        Base::setMaxSizeInBytes(max_size_in_bytes);
        std::lock_guard lock(interval_index_mutex);
        compactIntervalIndex();
        sets_since_compaction = 0;
    }

    /// Metadata for a cache entry, used by system.columns_cache.
    /// Does not hold a shared_ptr to column data, so it does not pin cached columns in memory.
    struct EntryMetadata
    {
        Key key;
        size_t rows;
        size_t bytes;
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
    /// Cost is O(interval_index entries); amortized O(1) per set() because it runs
    /// at most once per COMPACT_INTERVAL_INDEX_EVERY_N_SETS inserts.
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

}
