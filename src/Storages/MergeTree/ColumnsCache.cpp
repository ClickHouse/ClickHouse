#include <Storages/MergeTree/ColumnsCache.h>

namespace ProfileEvents
{
    extern const Event ColumnsCacheHits;
    extern const Event ColumnsCacheMisses;
}

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
        /// Since the map is sorted by row_begin (a), we iterate from the start
        /// and stop once a >= row_end (all subsequent entries also have a >= row_end).
        for (const auto & [interval_row_begin, key] : intervals)
        {
            /// Stop if we've gone past the query range
            if (key.row_begin >= row_end)
                break;

            /// Check if this interval actually intersects
            if (key.row_end > row_begin)
            {
                intersecting_keys.push_back(key);
            }
        }
    }

    /// Then query cache entries without holding interval_index lock to avoid deadlock
    for (const auto & key : intersecting_keys)
    {
        /// Verify the entry still exists in cache (might have been evicted)
        auto entry = Base::get(key);
        if (entry)
        {
            result.emplace_back(key, entry);
            ProfileEvents::increment(ProfileEvents::ColumnsCacheHits);
        }
        else
        {
            /// Entry was evicted, we could remove it from interval_index here,
            /// but it's not critical - will be cleaned up on next set()
            ProfileEvents::increment(ProfileEvents::ColumnsCacheMisses);
        }
    }

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
        for (const auto & [column_name, intervals] : columns_map)
        {
            for (const auto & [row_begin, key] : intervals)
            {
                keys.push_back(key);
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

std::vector<std::pair<ColumnsCache::Key, ColumnsCache::MappedPtr>>
ColumnsCache::getAllEntries()
{
    std::vector<std::pair<Key, MappedPtr>> result;

    /// First collect all keys while holding the interval_index lock
    std::vector<Key> keys;
    {
        std::lock_guard lock(interval_index_mutex);

        /// Iterate through all parts, columns, and intervals
        for (const auto & [part_id, columns_map] : interval_index)
        {
            for (const auto & [column_name, intervals] : columns_map)
            {
                for (const auto & [row_begin, key] : intervals)
                {
                    keys.push_back(key);
                }
            }
        }
    }

    /// Then query cache entries without holding interval_index lock to avoid deadlock
    for (const auto & key : keys)
    {
        /// Verify the entry still exists in cache (might have been evicted)
        auto entry = Base::get(key);
        if (entry)
        {
            result.emplace_back(key, entry);
        }
    }

    return result;
}

}
