#include <algorithm>
#include <chrono>

#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/thread_local_rng.h>
#include <Core/Defines.h>
#include <Storages/MergeTree/ColumnsCache.h>

namespace DB
{

template class CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

size_t getDefaultColumnsCacheSize(size_t physical_server_memory, double size_to_ram_ratio)
{
    if (physical_server_memory == 0)
        return DEFAULT_COLUMNS_CACHE_MAX_SIZE;

    return static_cast<size_t>(static_cast<double>(physical_server_memory) * size_to_ram_ratio);
}

ColumnsCache::ColumnsCache(
    const String & cache_policy,
    CurrentMetrics::Metric size_in_bytes_metric,
    CurrentMetrics::Metric count_metric,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, max_count, size_ratio)
    , configured_max_size_in_bytes(max_size_in_bytes)
    , effective_max_size_in_bytes(max_size_in_bytes)
{
}

std::vector<ColumnsCache::MappedPtr> ColumnsCache::getMany(
    const UUID & table_uuid,
    const String & part_name,
    const String & column_name,
    UInt64 schema_identity,
    size_t first_mark,
    size_t end_mark)
{
    std::vector<Key> keys;
    keys.reserve(end_mark - first_mark);
    for (size_t mark = first_mark; mark < end_mark; ++mark)
        keys.push_back(Key{table_uuid, part_name, column_name, mark, schema_identity});

    auto entries = Base::getMany(keys);
    for (const auto & entry : entries)
        if (entry)
            entry->used.store(true, std::memory_order_relaxed);
    return entries;
}

bool ColumnsCache::shouldAdmit()
{
    if (effective_max_size_in_bytes.load(std::memory_order_relaxed) == 0)
        return false;

    const UInt64 log2 = admission_log2.load(std::memory_order_relaxed);
    if (log2 == 0)
        return true;
    return (thread_local_rng() & ((UInt64(1) << log2) - 1)) == 0;
}

void ColumnsCache::accountAdmitted(size_t admitted)
{
    const size_t total_admitted = recent_admitted.fetch_add(admitted, std::memory_order_relaxed) + admitted;
    const size_t total_evicted_unused = recent_evicted_unused.load(std::memory_order_relaxed);
    if (total_admitted + total_evicted_unused < ADMISSION_WINDOW)
        return;

    /// Several threads can get here at once; each of them adjusts by what it takes out of the
    /// counters, which is a fair enough approximation.
    const size_t window_admitted = recent_admitted.exchange(0, std::memory_order_relaxed);
    const size_t window_evicted_unused = recent_evicted_unused.exchange(0, std::memory_order_relaxed);
    if (window_admitted == 0)
        return;

    const double unused_ratio = static_cast<double>(window_evicted_unused) / static_cast<double>(window_admitted);
    UInt64 log2 = admission_log2.load(std::memory_order_relaxed);
    if (unused_ratio > 0.75 && log2 < MAX_ADMISSION_LOG2)
        admission_log2.store(log2 + 1, std::memory_order_relaxed);
    else if (unused_ratio < 0.25 && log2 > 0)
        admission_log2.store(log2 - 1, std::memory_order_relaxed);
}

UInt64 ColumnsCache::getInvalidationGeneration(const UUID & table_uuid)
{
    std::lock_guard lock(index_mutex);
    return currentGeneration(table_uuid);
}

bool ColumnsCache::set(const MappedPtr & mapped, UInt64 expected_table_generation)
{
    const Key & key = mapped->key;

    /// An entry whose weight exceeds the cache size limit would be evicted by
    /// Base::set immediately after insertion. Reject it up front.
    if (ColumnsCacheWeightFunction{}(*mapped) > Base::maxSizeInBytes())
        return false;

    const PartIdentifier part_id{key.table_uuid, key.part_name};
    const ColumnIdentifier column_id{key.column_name, key.schema_identity};

    {
        std::lock_guard lock(index_mutex);

        /// Reject the write if the table was invalidated (removeTable) or the whole
        /// cache was dropped (clearAll) after the reader captured the generation.
        /// Otherwise a deferred write from a reader that started before a `RENAME
        /// COLUMN` could repopulate the cache with stale data the invalidation was
        /// meant to drop, and a reader that started before a `SYSTEM DROP COLUMNS
        /// CACHE` could resurrect entries the drop removed. Checked under the same
        /// lock removeTable and clearAll use, so the comparison cannot race with a
        /// concurrent bump.
        if (currentGeneration(key.table_uuid) != expected_table_generation)
            return false;

        /// Record the granule before the entry is inserted, so that the eviction callback,
        /// which may run inside `Base::set` itself, always finds the bit to reset.
        auto & marks = part_index[part_id][column_id];
        if (marks.size() <= key.mark)
            marks.resize(key.mark + 1);
        marks[key.mark] = true;
    }

    Base::set(key, mapped);

    /// The entry can fail admission: SLRU does not keep a probationary entry that does not fit
    /// the space left by the protected segment, even when it is within the overall size limit.
    /// In that case the eviction callback has already reset the bit.
    if (!Base::contains(key))
        return false;

    /// An invalidation can have happened between the check above and the insertion. The entry
    /// is then stale and nobody will look it up under this schema identity, but it would hold
    /// its memory until the eviction reaches it, so take it out right away.
    bool stale = false;
    {
        std::lock_guard lock(index_mutex);
        if (currentGeneration(key.table_uuid) != expected_table_generation)
        {
            stale = true;
            auto part_it = part_index.find(part_id);
            if (part_it != part_index.end())
            {
                auto column_it = part_it->second.find(column_id);
                if (column_it != part_it->second.end() && key.mark < column_it->second.size())
                    column_it->second[key.mark] = false;
            }
        }
    }

    if (stale)
    {
        Base::remove(key);
        return false;
    }

    accountAdmitted(1);
    return true;
}

size_t ColumnsCache::setMany(const std::vector<MappedPtr> & entries, UInt64 expected_table_generation)
{
    if (entries.empty())
        return 0;

    /// Entries whose weight exceeds the cache size limit would be evicted right after insertion.
    const size_t max_size = Base::maxSizeInBytes();
    std::vector<Key> keys;
    std::vector<MappedPtr> admitted;
    std::vector<size_t> weights;
    keys.reserve(entries.size());
    admitted.reserve(entries.size());
    weights.reserve(entries.size());
    for (const auto & entry : entries)
    {
        const size_t weight = ColumnsCacheWeightFunction{}(*entry);
        if (weight > max_size)
            continue;
        keys.push_back(entry->key);
        admitted.push_back(entry);
        weights.push_back(weight);
    }

    if (admitted.empty())
        return 0;

    const UUID table_uuid = keys.front().table_uuid;

    /// See `set` for the reasoning behind every step; this is the same sequence for many entries.
    {
        std::lock_guard lock(index_mutex);
        if (currentGeneration(table_uuid) != expected_table_generation)
            return 0;

        for (const auto & key : keys)
        {
            auto & marks = part_index[PartIdentifier{key.table_uuid, key.part_name}][ColumnIdentifier{key.column_name, key.schema_identity}];
            if (marks.size() <= key.mark)
                marks.resize(key.mark + 1);
            marks[key.mark] = true;
        }
    }

    Base::setMany(keys, admitted);
    const auto resident = Base::containsMany(keys);

    bool stale = false;
    {
        std::lock_guard lock(index_mutex);
        stale = currentGeneration(table_uuid) != expected_table_generation;
        for (size_t i = 0; i < keys.size(); ++i)
        {
            if (resident[i] && !stale)
                continue;

            const auto & key = keys[i];
            auto part_it = part_index.find(PartIdentifier{key.table_uuid, key.part_name});
            if (part_it == part_index.end())
                continue;
            auto column_it = part_it->second.find(ColumnIdentifier{key.column_name, key.schema_identity});
            if (column_it != part_it->second.end() && key.mark < column_it->second.size())
                column_it->second[key.mark] = false;
        }
    }

    if (stale)
    {
        for (const auto & key : keys)
            Base::remove(key);
        return 0;
    }

    size_t bytes_admitted = 0;
    size_t entries_admitted = 0;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (resident[i])
        {
            bytes_admitted += weights[i];
            ++entries_admitted;
        }
    }
    accountAdmitted(entries_admitted);
    return bytes_admitted;
}

void ColumnsCache::onEntryRemoval(size_t weight_loss, const MappedPtr & mapped)
{
    ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedEntries);
    ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedBytes, weight_loss);

    if (!mapped)
        return;

    if (!mapped->used.load(std::memory_order_relaxed))
        recent_evicted_unused.fetch_add(1, std::memory_order_relaxed);

    /// Runs under the mutex of the base cache; `index_mutex` is taken second, see `part_index`.
    const Key & key = mapped->key;
    std::lock_guard lock(index_mutex);

    auto part_it = part_index.find(PartIdentifier{key.table_uuid, key.part_name});
    if (part_it == part_index.end())
        return;

    auto column_it = part_it->second.find(ColumnIdentifier{key.column_name, key.schema_identity});
    if (column_it == part_it->second.end())
        return;

    auto & marks = column_it->second;
    if (key.mark < marks.size())
        marks[key.mark] = false;
}

void ColumnsCache::removeFromBase(const PartIdentifier & part_id, const PartIndex & index)
{
    for (const auto & [column_id, marks] : index)
    {
        for (size_t mark = 0; mark < marks.size(); ++mark)
        {
            if (marks[mark])
                Base::remove(Key{part_id.table_uuid, part_id.part_name, column_id.column_name, mark, column_id.schema_identity});
        }
    }
}

void ColumnsCache::removeTable(const UUID & table_uuid)
{
    /// Not under `index_mutex`: it takes the mutex of the base cache, see `part_index`.
    const bool disabled = Base::maxSizeInBytes() == 0;

    std::vector<std::pair<PartIdentifier, PartIndex>> parts_to_remove;
    {
        std::lock_guard lock(index_mutex);

        /// A zero-sized cache holds no entries, but readers that started while it was still
        /// enabled do hold a stamp and can insert once a config reload re-enables it, so the
        /// invalidation still has to be remembered. Advance the cache-wide stamp instead of a
        /// per-table one: it invalidates every stamp handed out so far just as well, and it
        /// keeps the per-table map from growing forever on a server where the cache is off.
        if (disabled)
        {
            global_generation = nextGeneration();
            table_generations.clear();
            return;
        }

        /// Advance the table's invalidation generation before clearing entries, so a
        /// deferred set() from a reader that captured an older generation is rejected
        /// and cannot repopulate the cache with stale data after this invalidation.
        table_generations[table_uuid] = nextGeneration();

        for (auto it = part_index.begin(); it != part_index.end();)
        {
            if (it->first.table_uuid == table_uuid)
            {
                parts_to_remove.emplace_back(it->first, std::move(it->second));
                it = part_index.erase(it);
            }
            else
                ++it;
        }
    }

    for (const auto & [part_id, index] : parts_to_remove)
        removeFromBase(part_id, index);
}

void ColumnsCache::removePart(const UUID & table_uuid, const String & part_name)
{
    /// No invalidation stamp is advanced here, see the declaration: the part is removed from
    /// the cache only once no reader can hold it, so there is no deferred write to reject.
    const PartIdentifier part_id{table_uuid, part_name};

    PartIndex index;
    {
        std::lock_guard lock(index_mutex);
        auto part_it = part_index.find(part_id);
        if (part_it == part_index.end())
            return;

        index = std::move(part_it->second);
        part_index.erase(part_it);
    }

    removeFromBase(part_id, index);
}

void ColumnsCache::clearAll()
{
    {
        std::lock_guard lock(index_mutex);
        global_generation = nextGeneration();
        /// The new cache-wide stamp already invalidates every token captured so far, so the
        /// per-table stamps can be reclaimed instead of being kept forever.
        table_generations.clear();
        part_index.clear();
    }

    recent_admitted.store(0, std::memory_order_relaxed);
    recent_evicted_unused.store(0, std::memory_order_relaxed);
    admission_log2.store(0, std::memory_order_relaxed);

    /// A write that started before the stamp was advanced cannot land after this point: `set`
    /// checks the stamp again once its entry is in and removes the entry if it changed.
    Base::clear();
}

void ColumnsCache::setConfiguredMaxSizeInBytes(size_t max_size_in_bytes)
{
    configured_max_size_in_bytes.store(max_size_in_bytes, std::memory_order_relaxed);
    effective_max_size_in_bytes.store(max_size_in_bytes, std::memory_order_relaxed);
    Base::setMaxSizeInBytes(max_size_in_bytes);
}

void ColumnsCache::setAutoResizeSettings(double free_memory_ratio_, Int64 history_window_ms_)
{
    free_memory_ratio.store(free_memory_ratio_, std::memory_order_relaxed);
    std::lock_guard lock(resize_mutex);
    history_window_ms = history_window_ms_;
}

bool ColumnsCache::autoResize(Int64 memory_usage_signed, size_t memory_limit)
{
    /// This is called from `MemoryTracker` when an allocation is about to exceed the limit;
    /// anything allocated here must not be tracked, or it would end up right back there.
    /// The entries evicted below were allocated by the queries that read them, and tracked,
    /// so their release is tracked as well: `MemoryTracker::free` is not affected by the blocker.
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);

    /// One resize at a time: the periodic one and the ones from allocations that hit the limit
    /// can coincide, and a resize that finds another in progress has nothing to add to it.
    std::unique_lock lock(resize_mutex, std::try_to_lock);
    if (!lock.owns_lock())
        return false;

    const size_t configured_max_size = configured_max_size_in_bytes.load(std::memory_order_relaxed);
    const size_t cache_size = Base::sizeInBytes();
    const size_t memory_usage = static_cast<size_t>(std::max<Int64>(memory_usage_signed, 0));
    const size_t usage_excluding_cache = memory_usage - std::min(cache_size, memory_usage);

    size_t peak = 0;
    if (history_window_ms <= 0)
    {
        peak = usage_excluding_cache;
    }
    else
    {
        /// The peak over the current and the previous window: a "leapfrogging" window that
        /// reacts to a rise at once and forgets a spike within two windows.
        const Int64 now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        const Int64 bucket = now_ms / history_window_ms;
        if (bucket > current_history_bucket + 1)
            peak_memory_buckets[0] = peak_memory_buckets[1] = 0;
        else if (bucket == current_history_bucket + 1)
        {
            peak_memory_buckets[1] = peak_memory_buckets[0];
            peak_memory_buckets[0] = 0;
        }
        current_history_bucket = bucket;
        peak_memory_buckets[0] = std::max(peak_memory_buckets[0], usage_excluding_cache);
        peak = std::max(peak_memory_buckets[0], peak_memory_buckets[1]);
    }

    const double ratio = std::clamp(free_memory_ratio.load(std::memory_order_relaxed), 0.0, 1.0);
    const size_t reduced_limit = static_cast<size_t>(static_cast<double>(memory_limit) * (1.0 - ratio));
    const size_t target_size = std::min(configured_max_size, reduced_limit - std::min(peak, reduced_limit));

    if (target_size != Base::maxSizeInBytes())
    {
        effective_max_size_in_bytes.store(target_size, std::memory_order_relaxed);
        Base::setMaxSizeInBytes(target_size);
    }

    const size_t new_cache_size = Base::sizeInBytes();
    return memory_usage - std::min(cache_size, memory_usage) + new_cache_size <= memory_limit;
}

std::vector<ColumnsCache::EntryMetadata>
ColumnsCache::getAllEntriesMetadata()
{
    /// `Base::dump()` returns a snapshot without changing the priorities of the entries,
    /// so the diagnostic query does not perturb the eviction order.
    /// Note: entries returned by dump() briefly hold a MappedPtr; we extract the
    /// metadata and drop the shared_ptr immediately so column data
    /// is not pinned beyond the lifetime of this vector.
    auto snapshot = Base::dump();

    std::vector<EntryMetadata> result;
    result.reserve(snapshot.size());
    for (const auto & entry : snapshot)
    {
        /// `bytes` reports the memory the entry retains, the same quantity the cache is bounded
        /// by, so that the sum over `system.columns_cache` can be compared with
        /// `columns_cache_size`. See `ColumnsCacheWeightFunction`.
        if (entry.mapped)
            result.push_back(EntryMetadata{entry.key, entry.mapped->row_begin, entry.mapped->rows, ColumnsCacheWeightFunction{}(*entry.mapped)});
    }
    return result;
}

}
