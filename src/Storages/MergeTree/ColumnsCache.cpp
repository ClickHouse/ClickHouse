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

void ColumnsCache::compactIntervalIndex()
{
    /// Caller must hold interval_index_mutex. Lock order: interval_index_mutex first,
    /// then briefly the CacheBase mutex inside Base::contains, matching set/removePart.
    for (auto part_it = interval_index.begin(); part_it != interval_index.end();)
    {
        auto & columns_map = part_it->second;
        for (auto col_it = columns_map.begin(); col_it != columns_map.end();)
        {
            auto & intervals = col_it->second;
            for (auto it = intervals.begin(); it != intervals.end();)
            {
                if (Base::contains(it->second))
                    ++it;
                else
                    it = intervals.erase(it);
            }
            if (intervals.empty())
                col_it = columns_map.erase(col_it);
            else
                ++col_it;
        }
        if (columns_map.empty())
            part_it = interval_index.erase(part_it);
        else
            ++part_it;
    }
}

void ColumnsCache::removeStaleKeys(const std::vector<Key> & stale_keys)
{
    std::lock_guard lock(interval_index_mutex);
    for (const auto & key : stale_keys)
    {
        /// Re-check that the key is actually missing from Base before erasing
        /// from interval_index. Between the Base::get(key) probe in getIntersecting
        /// (which observed nullptr) and the lock acquisition above, another thread
        /// may have re-inserted the same key via set(). In that case the index
        /// entry is now fresh and must not be erased. Base::contains() is a
        /// non-touching lookup and it is safe to call while holding
        /// interval_index_mutex (lock order: interval_index_mutex -> CacheBase mutex,
        /// matching set/removePart/clearAll).
        if (Base::contains(key))
            continue;

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
        /// The map is sorted by (range_begin, range_end). `set` maintains a
        /// non-overlapping invariant on this map, so at most one predecessor
        /// can extend into the query range; we only need to inspect that one.
        /// Then walk forward from `lower_bound` until range_begin >= row_end.
        auto it = intervals.lower_bound({row_begin, 0});
        if (it != intervals.begin())
        {
            auto prev_it = std::prev(it);
            if (prev_it->second.row_end > row_begin)
                intersecting_keys.push_back(prev_it->second);
        }

        for (; it != intervals.end(); ++it)
        {
            const auto & range_key = it->first;
            const auto & key = it->second;

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

void ColumnsCache::set(const Key & key, const MappedPtr & mapped)
{
    /// Hold interval_index_mutex across all updates so getIntersecting / clearAll
    /// observe a consistent view, and so that overlap detection cannot race with
    /// concurrent writers. Lock order matches removePart / clearAll / removeStaleKeys:
    /// interval_index_mutex first, then briefly the CacheBase internal mutex
    /// (taken inside Base::set / Base::remove). No lock-order cycle.
    std::lock_guard lock(interval_index_mutex);

    PartIdentifier part_id{key.table_uuid, key.part_name};
    auto & intervals = interval_index[part_id][key.column_name];

    /// Maintain a non-overlapping invariant on the per-column interval map.
    /// This keeps getIntersecting O(log N): with non-overlap, at most one
    /// predecessor can extend into the query range, so the lookup only needs
    /// to check the immediate predecessor instead of scanning from begin().
    ///
    /// When an existing interval fully contains the new range, the existing
    /// entry is preserved (a wider cached range is more useful for future
    /// lookups). Otherwise all overlapping entries are evicted from both
    /// `interval_index` and `Base` before inserting the new one.
    auto it = intervals.lower_bound({key.row_begin, 0});

    if (it != intervals.begin())
    {
        auto prev = std::prev(it);
        const auto & prev_key = prev->second;
        if (prev_key.row_end > key.row_begin)
        {
            if (prev_key.row_begin <= key.row_begin && prev_key.row_end >= key.row_end)
            {
                /// The fast-path skip is only valid while the existing wider
                /// interval is still in `Base`. LRU/SLRU eviction in `Base`
                /// does not clean up `interval_index`, so a stale entry here
                /// would otherwise cause us to skip the write indefinitely
                /// (especially in writes-only mode, where `getIntersecting`
                /// is not called and stale entries never get cleaned up
                /// lazily).
                if (Base::contains(prev_key))
                    return;
                it = intervals.erase(prev);
            }
            else
            {
                Base::remove(prev_key);
                it = intervals.erase(prev);
            }
        }
    }

    /// `lower_bound({row_begin, 0})` can land directly on an existing entry that
    /// shares `row_begin` with the new key. The predecessor check above skips it
    /// (because that entry is at `it`, not `prev`), so without this fast path the
    /// erase loop below would drop a wider containing interval like `[100, 200)`
    /// in favor of a narrower `[100, 150)`, reducing hit rate for later reads.
    /// The skip is only valid while the existing entry is still in `Base`; see
    /// the comment in the predecessor branch above.
    if (it != intervals.end() && it->first.first == key.row_begin && it->first.second >= key.row_end)
    {
        if (Base::contains(it->second))
            return;
        it = intervals.erase(it);
    }

    while (it != intervals.end() && it->first.first < key.row_end)
    {
        Base::remove(it->second);
        it = intervals.erase(it);
    }

    /// Insert into base cache first so there is no window where the index
    /// references a key that the cache does not yet contain (which would
    /// cause getIntersecting to classify it as stale and erase it).
    Base::set(key, mapped);
    intervals[{key.row_begin, key.row_end}] = key;

    /// Eviction in `Base` happens via a callback that does not provide the key,
    /// so `interval_index` retains entries for evicted keys. Sweep periodically
    /// so that metadata memory cannot grow unboundedly when evicted entries are
    /// never queried again (which would skip the lazy cleanup in `getIntersecting`).
    if (++sets_since_compaction >= COMPACT_INTERVAL_INDEX_EVERY_N_SETS)
    {
        compactIntervalIndex();
        sets_since_compaction = 0;
    }
}

void ColumnsCache::removeTable(const UUID & table_uuid)
{
    /// Lock ordering matches removePart: interval_index_mutex first, then the
    /// CacheBase mutex (taken inside Base::remove). No lock-order cycle.
    std::lock_guard lock(interval_index_mutex);

    std::vector<PartIdentifier> parts_to_remove;
    std::vector<Key> keys_to_remove;
    for (const auto & [part_id, columns_map] : interval_index)
    {
        if (part_id.table_uuid != table_uuid)
            continue;
        parts_to_remove.push_back(part_id);
        for (const auto & [_, intervals] : columns_map)
        {
            for (const auto & [_, key] : intervals)
                keys_to_remove.push_back(key);
        }
    }

    for (const auto & part_id : parts_to_remove)
        interval_index.erase(part_id);
    for (const auto & key : keys_to_remove)
        Base::remove(key);
}

void ColumnsCache::removePart(const UUID & table_uuid, const String & part_name)
{
    /// Hold interval_index_mutex across both the index erase and the Base::remove
    /// calls so that a concurrent set() for the same key cannot re-insert between
    /// the two steps and end up with the Base entry deleted but the interval_index
    /// still pointing at it.
    ///
    /// Lock ordering: set() acquires interval_index_mutex first, then calls
    /// Base::set (which briefly takes the CacheBase lock). removePart follows the
    /// same order, so there is no lock-order cycle.
    std::lock_guard lock(interval_index_mutex);

    PartIdentifier part_id{table_uuid, part_name};
    auto part_it = interval_index.find(part_id);
    if (part_it == interval_index.end())
        return;

    /// Collect all cache entries for this part
    std::vector<Key> keys;
    const auto & columns_map = part_it->second;
    for (const auto & column_entry : columns_map)
    {
        for (const auto & interval_entry : column_entry.second)
        {
            keys.push_back(interval_entry.second);
        }
    }

    /// Remove from the interval index and from the base cache atomically w.r.t. set()
    interval_index.erase(part_it);
    for (const auto & key : keys)
        Base::remove(key);
}

std::vector<ColumnsCache::EntryMetadata>
ColumnsCache::getAllEntriesMetadata()
{
    /// Use Base::dump() rather than iterating interval_index and calling Base::get,
    /// because Base::get updates LRU recency in the cache policy and would cause
    /// the diagnostic query to perturb eviction order (i.e., "touch" every entry).
    /// dump() returns a snapshot without changing priorities.
    /// Note: entries returned by dump() briefly hold a MappedPtr; we extract the
    /// metadata (rows, bytes) and drop the shared_ptr immediately so column data
    /// is not pinned beyond the lifetime of this vector.
    auto snapshot = Base::dump();

    std::vector<EntryMetadata> result;
    result.reserve(snapshot.size());
    for (const auto & entry : snapshot)
    {
        if (entry.mapped)
            result.push_back(EntryMetadata{entry.key, entry.mapped->rows, entry.mapped->column->byteSize()});
    }
    return result;
}

}
