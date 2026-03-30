#include <Storages/MergeTree/ColumnsCache.h>

namespace DB
{

template class CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

ColumnsCache::ColumnsCache(
    const String & cache_policy,
    CurrentMetrics::Metric size_in_bytes_metric,
    CurrentMetrics::Metric count_metric,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, max_count, size_ratio)
{
}

void ColumnsCache::removeStaleKeys(const std::vector<Key> & stale_keys)
{
    std::lock_guard lock(interval_index_mutex);
    for (const auto & key : stale_keys)
    {
        PartIdentifier part_id{key.table_uuid, key.part_name};
        auto part_it = interval_index.find(part_id);
        if (part_it == interval_index.end())
            continue;

        auto & columns_map = part_it->second;
        auto col_it = columns_map.find(key.column_name);
        if (col_it == columns_map.end())
            continue;

        auto & intervals = col_it->second;
        auto it = intervals.find({key.row_begin, key.row_end});
        if (it != intervals.end() && it->second == key)
            intervals.erase(it);

        /// Clean up empty maps
        if (intervals.empty())
            columns_map.erase(col_it);
        if (columns_map.empty())
            interval_index.erase(part_it);
    }
}

std::vector<std::pair<ColumnsCache::Key, ColumnsCache::MappedPtr>>
ColumnsCache::getIntersecting(
    const UUID & table_uuid,
    const String & part_name,
    const String & column_name,
    size_t row_begin,
    size_t row_end)
{
    std::vector<std::pair<Key, MappedPtr>> result;

    /// First collect intersecting keys while holding the interval_index lock
    std::vector<Key> intersecting_keys;
    {
        std::lock_guard lock(interval_index_mutex);

        PartIdentifier part_id{table_uuid, part_name};
        auto part_it = interval_index.find(part_id);
        if (part_it == interval_index.end())
            return result;

        const auto & columns_map = part_it->second;
        auto column_it = columns_map.find(column_name);
        if (column_it == columns_map.end())
            return result;

        const auto & intervals = column_it->second;

        /// Collect all intervals that intersect with [row_begin, row_end).
        /// An interval [a, b) intersects if a < row_end AND b > row_begin.
        /// Since the map is sorted by (row_begin, row_end), we iterate from the start
        /// and stop once row_begin >= row_end (all subsequent entries also start at or after row_end).
        for (const auto & [range_key, key] : intervals)
        {
            /// Stop if we've gone past the query range
            if (range_key.first >= row_end)
                break;

            /// Check if this interval actually intersects
            if (key.row_end > row_begin)
            {
                intersecting_keys.push_back(key);
            }
        }
    }

    /// Then query cache entries without holding interval_index lock to avoid deadlock.
    /// Hit/miss counting is left to the caller at request level to avoid inflating
    /// counters with per-entry or stale-cleanup events.
    std::vector<Key> stale_keys;
    for (const auto & key : intersecting_keys)
    {
        /// Verify the entry still exists in cache (might have been evicted)
        auto entry = Base::get(key);
        if (entry)
            result.emplace_back(key, entry);
        else
            stale_keys.push_back(key);
    }

    /// Clean up stale entries from interval_index (entries evicted by LRU/SLRU)
    if (!stale_keys.empty())
        removeStaleKeys(stale_keys);

    return result;
}

void ColumnsCache::removePart(const UUID & table_uuid, const String & part_name)
{
    /// First collect all keys while holding the interval_index lock
    std::vector<Key> keys;
    {
        std::lock_guard lock(interval_index_mutex);

        PartIdentifier part_id{table_uuid, part_name};
        auto part_it = interval_index.find(part_id);
        if (part_it == interval_index.end())
            return;

        /// Collect all cache entries for this part
        const auto & columns_map = part_it->second;
        for (const auto & column_entry : columns_map)
        {
            for (const auto & interval_entry : column_entry.second)
            {
                keys.push_back(interval_entry.second);
            }
        }

        /// Remove the part from the interval index
        interval_index.erase(part_it);
    }

    /// Then remove cache entries without holding interval_index lock to avoid deadlock
    for (const auto & key : keys)
    {
        Base::remove(key);
    }
}

std::vector<ColumnsCache::EntryMetadata>
ColumnsCache::getAllEntriesMetadata()
{
    std::vector<EntryMetadata> result;

    /// First collect all keys while holding the interval_index lock
    std::vector<Key> keys;
    {
        std::lock_guard lock(interval_index_mutex);

        /// Iterate through all parts, columns, and intervals
        for (const auto & part_entry : interval_index)
        {
            for (const auto & column_entry : part_entry.second)
            {
                for (const auto & interval_entry : column_entry.second)
                {
                    keys.push_back(interval_entry.second);
                }
            }
        }
    }

    /// Then query cache entries without holding interval_index lock to avoid deadlock.
    /// We extract metadata (rows, bytes) immediately and release the shared_ptr,
    /// so we don't pin all cached column data in memory during the query.
    std::vector<Key> stale_keys;
    for (const auto & key : keys)
    {
        /// Verify the entry still exists in cache (might have been evicted)
        auto entry = Base::get(key);
        if (entry)
            result.push_back(EntryMetadata{key, entry->rows, entry->column->byteSize()});
        else
            stale_keys.push_back(key);
    }

    /// Clean up stale entries from interval_index
    if (!stale_keys.empty())
        removeStaleKeys(stale_keys);

    return result;
}

}
