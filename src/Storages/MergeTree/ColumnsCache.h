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
    void set(const Key & key, const MappedPtr & mapped)
    {
        /// Hold interval_index_mutex across both operations so that clearAll()
        /// cannot run between them and leave an orphaned Base entry.
        /// Insert into base cache first, then publish to interval_index.
        /// This order ensures there is no window where a key is visible in the index
        /// but not yet in the cache (which would cause getIntersecting to classify it
        /// as stale and erase it).
        std::lock_guard lock(interval_index_mutex);
        Base::set(key, mapped);

        PartIdentifier part_id{key.table_uuid, key.part_name};
        auto & intervals = interval_index[part_id][key.column_name];
        intervals[{key.row_begin, key.row_end}] = key;
    }

    /// Remove all cached entries for a specific data part.
    /// Should be called when a part is dropped, merged, or mutated.
    void removePart(const UUID & table_uuid, const String & part_name);

    /// Clear both the base cache and the interval index.
    /// Used by SYSTEM DROP COLUMNS CACHE.
    /// Holds interval_index_mutex across both operations so that a concurrent
    /// set() cannot insert into interval_index between the two clears.
    /// This is deadlock-safe: set() releases the CacheBase internal lock
    /// before acquiring interval_index_mutex, so there is no lock-order cycle.
    void clearAll()
    {
        std::lock_guard lock(interval_index_mutex);
        Base::clear();
        interval_index.clear();
    }

    /// Get all cache entries for introspection (system.columns_cache table).
    /// Returns a vector of (key, entry) pairs for all cached columns.
    std::vector<std::pair<Key, MappedPtr>> getAllEntries();

private:
    /// Remove stale entries from interval_index.
    /// Must be called without holding the CacheBase lock to avoid deadlock.
    void removeStaleKeys(const std::vector<Key> & stale_keys);

    void onEntryRemoval(size_t weight_loss, const MappedPtr &) override
    {
        ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedEntries);
        ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedBytes, weight_loss);

        /// Note: We don't remove from interval_index here because the eviction callback
        /// doesn't provide the key. Stale entries are cleaned up lazily in getIntersecting
        /// and eagerly in set (when overwriting) and removePart.
    }
};

using ColumnsCachePtr = std::shared_ptr<ColumnsCache>;

}
