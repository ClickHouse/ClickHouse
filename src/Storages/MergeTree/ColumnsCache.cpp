#include <algorithm>
#include <chrono>

#include <Common/CurrentMetrics.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>
#include <Core/Defines.h>
#include <Storages/MergeTree/ColumnsCache.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

namespace CurrentMetrics
{
    extern const Metric ColumnsCacheSizeLimit;
}

namespace ProfileEvents
{
    extern const Event ColumnsCacheEvictedBytes;
    extern const Event ColumnsCacheEvictedEntries;
}

namespace DB
{

template class CacheBase<ColumnsCacheKey, ColumnsCacheEntry, ColumnsCacheKeyHash, ColumnsCacheWeightFunction>;

size_t getDefaultColumnsCacheSize(size_t physical_server_memory, double size_to_ram_ratio)
{
    if (physical_server_memory == 0)
        return DEFAULT_COLUMNS_CACHE_MAX_SIZE;

    return static_cast<size_t>(static_cast<double>(physical_server_memory) * size_to_ram_ratio);
}

UInt128 getColumnsCacheColumnIdentity(const UUID & table_uuid, const String & part_name, const String & column_name, UInt64 schema_identity)
{
    SipHash hash;
    hash.update(table_uuid);
    hash.update(part_name);
    hash.update(column_name);
    hash.update(schema_identity);
    return hash.get128();
}

ColumnsCacheStripes::ColumnsCacheStripes(size_t avg_rows_per_mark)
{
    /// The average granule of the part decides how many granules make a stripe of about
    /// `TARGET_ROWS` rows. With a fixed `index_granularity` this is exact; with an adaptive one
    /// the stripes come out around the target.
    avg_rows_per_mark = std::max<size_t>(1, avg_rows_per_mark);
    stripe_marks = std::clamp<size_t>((TARGET_ROWS + avg_rows_per_mark / 2) / avg_rows_per_mark, 1, MAX_STRIPE_MARKS);
}

ColumnsCacheStripes ColumnsCacheStripes::forPart(const MergeTreeIndexGranularity & index_granularity)
{
    /// The last granule is usually short, so it is left out of the average.
    const size_t marks = index_granularity.getMarksCountWithoutFinal();
    const size_t rows = index_granularity.getTotalRows();
    if (marks <= 1)
        return ColumnsCacheStripes(rows);
    return ColumnsCacheStripes((rows - index_granularity.getMarkRows(marks - 1)) / (marks - 1));
}

size_t ColumnsCache::PartIdentifierHash::operator()(const PartIdentifier & id) const
{
    SipHash hash;
    hash.update(id.table_uuid);
    hash.update(id.part_name);
    return hash.get64();
}

ColumnsCache::ColumnsCache(
    const String & cache_policy,
    CurrentMetrics::Metric size_in_bytes_metric,
    CurrentMetrics::Metric count_metric,
    size_t max_size_in_bytes,
    size_t max_count,
    double size_ratio)
    : configured_max_size_in_bytes(max_size_in_bytes)
    , effective_max_size_in_bytes(max_size_in_bytes)
{
    const size_t num_shards = numberOfShards(max_size_in_bytes);
    const size_t shard_max_size = (max_size_in_bytes + num_shards - 1) / num_shards;
    const size_t shard_max_count = max_count ? (max_count + num_shards - 1) / num_shards : 0;
    shards.reserve(num_shards);
    for (size_t i = 0; i < num_shards; ++i)
        shards.push_back(std::make_unique<Shard>(*this, cache_policy, size_in_bytes_metric, count_metric, shard_max_size, shard_max_count, size_ratio));

    CurrentMetrics::set(CurrentMetrics::ColumnsCacheSizeLimit, max_size_in_bytes);
}

ColumnsCache::~ColumnsCache() = default;

std::vector<ColumnsCache::MappedPtr> ColumnsCache::getMany(const UInt128 & column_identity, size_t first_stripe, size_t end_stripe)
{
    const size_t num_keys = end_stripe - first_stripe;
    std::vector<MappedPtr> result(num_keys);

    /// The keys of a column spread over the shards; each shard is locked once for its keys.
    std::vector<std::vector<size_t>> positions_by_shard(shards.size());
    for (size_t i = 0; i < num_keys; ++i)
        positions_by_shard[shardIndex(Key{column_identity, first_stripe + i})].push_back(i);

    std::vector<Key> keys;
    for (size_t shard = 0; shard < shards.size(); ++shard)
    {
        const auto & positions = positions_by_shard[shard];
        if (positions.empty())
            continue;

        keys.clear();
        for (size_t i : positions)
            keys.push_back(Key{column_identity, first_stripe + i});

        auto entries = shards[shard]->getMany(keys);
        for (size_t j = 0; j < positions.size(); ++j)
        {
            if (entries[j])
                entries[j]->used.store(true, std::memory_order_relaxed);
            result[positions[j]] = std::move(entries[j]);
        }
    }

    return result;
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

ColumnsCache::MappedPtr ColumnsCache::mergeEntries(const MappedPtr & existing, const MappedPtr & incoming)
{
    if (!existing)
        return incoming;

    /// Nothing new: the cache holds these granules already.
    if (existing->coversMarks(incoming->first_mark, incoming->end_mark))
        return nullptr;

    /// The new entry holds everything the existing one does, and more.
    if (incoming->coversMarks(existing->first_mark, existing->end_mark))
        return incoming;

    /// Disjoint with a gap between them: only one of them can be kept. The larger wins, and the
    /// existing one is kept on a tie, so that two reads writing the halves of a stripe in turns
    /// do not evict each other's entry.
    if (incoming->end_mark < existing->first_mark || existing->end_mark < incoming->first_mark)
        return incoming->rows > existing->rows ? incoming : nullptr;

    /// Overlapping or adjacent: the union, made of the rows of the existing entry and the rows
    /// of the new one beyond it on either side. The same part, the same column and the same
    /// schema, so the rows agree where the two overlap.
    auto merged = std::make_shared<ColumnsCacheEntry>();
    merged->table_uuid = existing->table_uuid;
    merged->part_name = existing->part_name;
    merged->column_name = existing->column_name;
    merged->schema_identity = existing->schema_identity;
    merged->key = existing->key;
    merged->first_mark = std::min(existing->first_mark, incoming->first_mark);
    merged->end_mark = std::max(existing->end_mark, incoming->end_mark);
    merged->row_begin = std::min(existing->row_begin, incoming->row_begin);
    const size_t row_end = std::max(existing->row_begin + existing->rows, incoming->row_begin + incoming->rows);
    merged->rows = row_end - merged->row_begin;

    /// The two runs can hold the same rows in different representations: the accumulated copy
    /// of a read is a clone of the column the read produced, and the same column of the same
    /// part is a `ColumnSparse` under one set of settings and a full column under another, which
    /// is why `MergeTreeReaderWide::serveRowsFromColumnsCache` normalizes the serve path as well.
    /// A sparse destination takes rows from a full source, but a full one cannot take them from
    /// a `ColumnSparse`, so make the source full when the destination is not sparse.
    ColumnPtr incoming_column = incoming->column;
    if (!existing->column->isSparse() && incoming_column->isSparse())
        incoming_column = incoming_column->convertToFullColumnIfSparse();

    auto column = existing->column->cloneEmpty();
    column->reserve(merged->rows);
    if (incoming->row_begin < existing->row_begin)
        column->insertRangeFrom(*incoming_column, 0, existing->row_begin - incoming->row_begin);
    column->insertRangeFrom(*existing->column, 0, existing->rows);
    const size_t existing_row_end = existing->row_begin + existing->rows;
    if (row_end > existing_row_end)
        column->insertRangeFrom(*incoming_column, existing_row_end - incoming->row_begin, row_end - existing_row_end);
    column->shrinkToFit();
    merged->column = std::move(column);

    return merged;
}

size_t ColumnsCache::setMany(const std::vector<MappedPtr> & entries, UInt64 expected_table_generation)
{
    if (entries.empty())
        return 0;

    const UUID & table_uuid = entries.front()->table_uuid;

    /// Reject the write if the table was invalidated (removeTable) or the whole
    /// cache was dropped (clearAll) after the reader captured the generation.
    /// Otherwise a deferred write from a reader that started before a `RENAME
    /// COLUMN` could repopulate the cache with stale data the invalidation was
    /// meant to drop, and a reader that started before a `SYSTEM DROP COLUMNS
    /// CACHE` could resurrect entries the drop removed. Checked under the same
    /// lock removeTable and clearAll use, so the comparison cannot race with a
    /// concurrent bump.
    {
        std::lock_guard lock(index_mutex);
        if (currentGeneration(table_uuid) != expected_table_generation)
            return 0;
    }

    /// Merge with what the cache holds for the stripes, see `mergeEntries`. The merge happens
    /// outside the locks; two readers writing the same stripe at once can lose one of the two
    /// writes, and the next read of the lost granules writes them again.
    std::vector<Key> keys;
    std::vector<MappedPtr> to_store;
    std::vector<size_t> weights;
    keys.reserve(entries.size());
    to_store.reserve(entries.size());
    weights.reserve(entries.size());

    /// Two runs of the same stripe in one batch - a read that found the middle of the stripe in
    /// the cache and read the granules on both sides of it - merge with each other as well.
    std::unordered_map<Key, size_t, ColumnsCacheKeyHash> position_by_key;

    const size_t shard_max_size = shards.front()->maxSizeInBytes();
    for (const auto & entry : entries)
    {
        auto position = position_by_key.find(entry->key);
        const MappedPtr existing = position == position_by_key.end() ? shardOf(entry->key).get(entry->key) : to_store[position->second];
        auto merged = mergeEntries(existing, entry);
        if (!merged)
            continue;

        /// An entry whose weight exceeds the size limit of its shard would be evicted right after
        /// insertion. Reject it up front.
        const size_t weight = ColumnsCacheWeightFunction{}(*merged);
        if (weight > shard_max_size)
            continue;

        if (position != position_by_key.end())
        {
            to_store[position->second] = std::move(merged);
            weights[position->second] = weight;
            continue;
        }

        position_by_key.emplace(merged->key, keys.size());
        keys.push_back(merged->key);
        to_store.push_back(std::move(merged));
        weights.push_back(weight);
    }

    if (to_store.empty())
        return 0;

    /// Record the stripes before the entries are inserted, so that the eviction callback, which
    /// may run inside the insertion itself, always finds the bit to reset.
    {
        std::lock_guard lock(index_mutex);
        if (currentGeneration(table_uuid) != expected_table_generation)
            return 0;

        for (size_t i = 0; i < keys.size(); ++i)
        {
            const auto & entry = *to_store[i];
            part_index[PartIdentifier{entry.table_uuid, entry.part_name}][keys[i].column_identity].insert(keys[i].stripe);
        }
    }

    if (on_entries_staged_for_test)
        on_entries_staged_for_test();

    for (size_t i = 0; i < keys.size(); ++i)
        shardOf(keys[i]).set(keys[i], to_store[i]);

    if (on_entries_inserted_for_test)
        on_entries_inserted_for_test();

    /// An entry can fail admission: SLRU does not keep a probationary entry that does not fit
    /// the space left by the protected segment, even when it is within the overall size limit.
    /// In that case the eviction callback has already reset the bit.
    std::vector<bool> resident(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        resident[i] = shardOf(keys[i]).contains(keys[i]);

    /// An invalidation can have happened between the check above and the insertion. The entries
    /// are then stale and nobody will look them up under this schema identity, but they would
    /// hold their memory until the eviction reaches them, so take them out right away.
    bool stale = false;
    {
        std::lock_guard lock(index_mutex);
        stale = currentGeneration(table_uuid) != expected_table_generation;

        /// A stale writer of the same stripe, racing this call, releases the bucket of the
        /// stripe when it takes its own entry back out - see below - and the bucket has to be
        /// there for every entry the cache holds, or `removePart` would walk past this one and
        /// leave it resident for a part that is gone. Recording it again costs a lookup in a
        /// hash set that holds it already in every other case.
        if (!stale)
        {
            for (size_t i = 0; i < keys.size(); ++i)
            {
                if (!resident[i])
                    continue;
                const auto & entry = *to_store[i];
                part_index[PartIdentifier{entry.table_uuid, entry.part_name}][keys[i].column_identity].insert(keys[i].stripe);
            }
        }
    }

    if (stale)
    {
        removeStaleEntries(keys, to_store);
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

void ColumnsCache::Shard::onEntryRemoval(size_t weight_loss, const MappedPtr & mapped)
{
    ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedEntries);
    ProfileEvents::increment(ProfileEvents::ColumnsCacheEvictedBytes, weight_loss);

    if (!mapped)
        return;

    if (!mapped->used.load(std::memory_order_relaxed))
        parent.recent_evicted_unused.fetch_add(1, std::memory_order_relaxed);

    /// Runs under the mutex of the shard; `index_mutex` is taken second, see `part_index`.
    std::lock_guard lock(parent.index_mutex);

    parent.forgetStripe(PartIdentifier{mapped->table_uuid, mapped->part_name}, mapped->key.column_identity, mapped->key.stripe);
}

void ColumnsCache::forgetStripe(const PartIdentifier & part, const UInt128 & column_identity, size_t stripe)
{
    auto part_it = part_index.find(part);
    if (part_it == part_index.end())
        return;

    auto column_it = part_it->second.find(column_identity);
    if (column_it == part_it->second.end())
        return;

    column_it->second.erase(stripe);

    /// An empty bucket holds no information, only memory: erase it, so that the index of a part
    /// whose entries were all evicted costs nothing until the part is read again.
    if (column_it->second.empty())
        part_it->second.erase(column_it);
    if (part_it->second.empty())
        part_index.erase(part_it);
}

void ColumnsCache::removeFromShards(const std::vector<Key> & keys)
{
    for (const auto & key : keys)
        shardOf(key).remove(key);
}

void ColumnsCache::removeStaleEntries(const std::vector<Key> & keys, const std::vector<MappedPtr> & entries)
{
    for (size_t i = 0; i < keys.size(); ++i)
    {
        /// Only the entries this batch put in the shards, identified by the object and not by
        /// the key: an invalidation makes this write stale, and a reader that started after the
        /// invalidation is allowed to write the very same key - after a `SYSTEM DROP COLUMNS
        /// CACHE` it is exactly the reader that should repopulate the cache. Removing by key
        /// alone would throw its fresh entry away together with this stale one.
        ///
        /// The bucket of the stripe is released with the entry, under the mutex of the shard and
        /// then `index_mutex` - the order `onEntryRemoval` uses - so that the fresh writer cannot
        /// insert its entry in between and be left without its stripe recorded. It can still
        /// insert after this, which is what the recording at the end of `setMany` is for.
        shardOf(keys[i]).removeIfMatches(keys[i], [&](const MappedPtr & resident)
        {
            if (resident != entries[i])
                return false;

            std::lock_guard lock(index_mutex);
            forgetStripe(PartIdentifier{entries[i]->table_uuid, entries[i]->part_name}, keys[i].column_identity, keys[i].stripe);
            return true;
        });
    }
}

void ColumnsCache::removeTable(const UUID & table_uuid)
{
    /// Not under `index_mutex`: `maxSizeInBytes` of a shard takes the mutex of the shard.
    const bool disabled = shards.front()->maxSizeInBytes() == 0;

    std::vector<Key> keys_to_remove;
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
        }
        else
        {
            /// Advance the table's invalidation generation before clearing entries, so a
            /// deferred write from a reader that captured an older generation is rejected
            /// and cannot repopulate the cache with stale data after this invalidation.
            table_generations[table_uuid] = nextGeneration();
        }

        /// The index of the table is cleaned up in both cases: a cache that has just been
        /// disabled by a config reload has already evicted its entries, but the reload and
        /// this call can interleave, and an index bucket left behind would retain memory
        /// nothing accounts for.

        for (auto it = part_index.begin(); it != part_index.end();)
        {
            if (it->first.table_uuid != table_uuid)
            {
                ++it;
                continue;
            }

            for (const auto & [column_identity, stripes] : it->second)
                for (size_t stripe : stripes)
                    keys_to_remove.push_back(Key{column_identity, stripe});

            it = part_index.erase(it);
        }
    }

    removeFromShards(keys_to_remove);
}

void ColumnsCache::removePart(const UUID & table_uuid, const String & part_name)
{
    /// No invalidation stamp is advanced here, see the declaration: the part is removed from
    /// the cache only once no reader can hold it, so there is no deferred write to reject.
    std::vector<Key> keys_to_remove;
    {
        std::lock_guard lock(index_mutex);
        auto part_it = part_index.find(PartIdentifier{table_uuid, part_name});
        if (part_it == part_index.end())
            return;

        for (const auto & [column_identity, stripes] : part_it->second)
            for (size_t stripe : stripes)
                keys_to_remove.push_back(Key{column_identity, stripe});

        part_index.erase(part_it);
    }

    removeFromShards(keys_to_remove);
}

bool ColumnsCache::containsPart(const UUID & table_uuid, const String & part_name) const
{
    std::lock_guard lock(index_mutex);
    return part_index.contains(PartIdentifier{table_uuid, part_name});
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

    /// A write that started before the stamp was advanced cannot land after this point:
    /// `setMany` checks the stamp again once its entries are in and removes them if it changed.
    for (auto & shard : shards)
        shard->clear();
}

void ColumnsCache::setShardsMaxSize(size_t total_max_size_in_bytes)
{
    const size_t shard_max_size = (total_max_size_in_bytes + shards.size() - 1) / shards.size();
    for (auto & shard : shards)
        shard->setMaxSizeInBytes(shard_max_size);
}

void ColumnsCache::setEffectiveMaxSize(size_t max_size_in_bytes)
{
    effective_max_size_in_bytes.store(max_size_in_bytes, std::memory_order_relaxed);
    CurrentMetrics::set(CurrentMetrics::ColumnsCacheSizeLimit, max_size_in_bytes);
    setShardsMaxSize(max_size_in_bytes);
}

void ColumnsCache::setConfiguredMaxSizeInBytes(size_t max_size_in_bytes)
{
    /// Under `resize_mutex`, so a resize in progress, which computed its target from the old
    /// configured size, cannot put that target in effect after the new size. The allocations
    /// here must not reach `autoResize` from `MemoryTracker`: this thread already holds the mutex.
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    std::lock_guard lock(resize_mutex);
    configured_max_size_in_bytes.store(max_size_in_bytes, std::memory_order_relaxed);
    setEffectiveMaxSize(max_size_in_bytes);
}

void ColumnsCache::setAutoResizeSettings(double free_memory_ratio_, Int64 history_window_ms_)
{
    free_memory_ratio.store(free_memory_ratio_, std::memory_order_relaxed);
    std::lock_guard lock(resize_mutex);
    history_window_ms = history_window_ms_;
}

size_t ColumnsCache::sizeInBytes() const
{
    size_t sum = 0;
    for (const auto & shard : shards)
        sum += shard->sizeInBytes();
    return sum;
}

size_t ColumnsCache::count() const
{
    size_t sum = 0;
    for (const auto & shard : shards)
        sum += shard->count();
    return sum;
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
    const size_t cache_size = sizeInBytes();
    const size_t memory_usage = static_cast<size_t>(std::max<Int64>(memory_usage_signed, 0));
    const size_t usage_excluding_cache = memory_usage - std::min(cache_size, memory_usage);

    /// No limit: nothing to give memory back to.
    if (memory_limit == 0)
    {
        if (effective_max_size_in_bytes.load(std::memory_order_relaxed) != configured_max_size)
            setEffectiveMaxSize(configured_max_size);
        return true;
    }

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

    const size_t previous_size = effective_max_size_in_bytes.load(std::memory_order_relaxed);
    if (target_size != previous_size)
    {
        LOG_TRACE(getLogger("ColumnsCache"),
            "Resizing the columns cache from {} to {} (configured {}): the server uses {} ({} without the cache, peak {}) of the limit {}",
            formatReadableSizeWithBinarySuffix(previous_size), formatReadableSizeWithBinarySuffix(target_size),
            formatReadableSizeWithBinarySuffix(configured_max_size), formatReadableSizeWithBinarySuffix(memory_usage),
            formatReadableSizeWithBinarySuffix(usage_excluding_cache), formatReadableSizeWithBinarySuffix(peak),
            formatReadableSizeWithBinarySuffix(memory_limit));
        setEffectiveMaxSize(target_size);
    }

    const size_t new_cache_size = sizeInBytes();
    return memory_usage - std::min(cache_size, memory_usage) + new_cache_size <= memory_limit;
}

std::vector<ColumnsCache::EntryMetadata> ColumnsCache::getAllEntriesMetadata()
{
    std::vector<EntryMetadata> result;
    for (auto & shard : shards)
    {
        /// `dump()` returns a snapshot without changing the priorities of the entries, so the
        /// diagnostic query does not perturb the eviction order. The snapshot briefly holds the
        /// entries; only their metadata is kept.
        auto snapshot = shard->dump();
        result.reserve(result.size() + snapshot.size());
        for (const auto & item : snapshot)
        {
            if (!item.mapped)
                continue;
            const auto & entry = *item.mapped;
            /// `bytes` reports the memory the entry retains, the same quantity the cache is bounded
            /// by, so that the sum over `system.columns_cache` can be compared with
            /// `columns_cache_size`. See `ColumnsCacheWeightFunction`.
            result.push_back(EntryMetadata{entry.table_uuid, entry.part_name, entry.column_name, entry.row_begin, entry.rows, ColumnsCacheWeightFunction{}(entry)});
        }
    }
    return result;
}

}
