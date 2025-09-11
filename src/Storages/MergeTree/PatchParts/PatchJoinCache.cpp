#include <shared_mutex>
#include <utility>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
    extern const Event BuildPatchesJoinMicroseconds;
    extern const Event PatchesJoinRowsAddedToHashTable;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
}

static const PaddedPODArray<UInt64> & getColumnUInt64Data(const Block & block, const String & column_name)
{
    return assert_cast<const ColumnUInt64 &>(*block.getByName(column_name).column).getData();
}

void PatchJoinCache::init(const RangesInPatchParts & ranges_in_patches)
{
    std::lock_guard lock(mutex);
    const auto & all_ranges = ranges_in_patches.getRanges();

    /// Spread ranges among buckets.
    /// We assume that ranges are already sorted
    /// and put contiguous ranges into the same bucket.
    for (const auto & [patch_name, ranges] : all_ranges)
    {
        if (ranges.empty())
            continue;

        auto & entries = cache[patch_name];
        auto & buckets = ranges_to_buckets[patch_name];

        size_t current_buckets = std::min(num_buckets, ranges.size());
        size_t num_ranges_in_bucket = (ranges.size() + current_buckets - 1) / current_buckets;

        entries.reserve(current_buckets);
        for (size_t i = 0; i < current_buckets; ++i)
            entries.push_back(std::make_shared<PatchJoinCache::Entry>());

        for (size_t i = 0; i < ranges.size(); ++i)
        {
            size_t idx = i / num_ranges_in_bucket;
            chassert(idx < entries.size());
            buckets[ranges[i]] = idx;
        }
    }
}

PatchJoinCache::PatchStatsEntryPtr PatchJoinCache::getStatsEntry(const DataPartPtr & patch_part, const MergeTreeReaderSettings & settings)
{
    auto stats_entry = getOrCreatePatchStats(patch_part->name);
    std::lock_guard lock(stats_entry->mutex);

    if (stats_entry->initialized)
        return stats_entry;

    MarkRanges all_patch_ranges;
    auto it = ranges_to_buckets.find(patch_part->name);

    if (it == ranges_to_buckets.end())
    {
        stats_entry->initialized = true;
        return stats_entry;
    }

    for (const auto & [range, _] : it->second)
        all_patch_ranges.push_back(range);

    auto block_number_stats = getPatchMinMaxStats(patch_part, all_patch_ranges, BlockNumberColumn::name, settings);
    auto block_offset_stats = getPatchMinMaxStats(patch_part, all_patch_ranges, BlockOffsetColumn::name, settings);

    if (block_number_stats && block_offset_stats)
    {
        chassert(block_number_stats->size() == all_patch_ranges.size());
        chassert(block_offset_stats->size() == all_patch_ranges.size());

        for (size_t i = 0; i < all_patch_ranges.size(); ++i)
        {
            auto & range_stats = stats_entry->stats[all_patch_ranges[i]];
            range_stats.block_number_stat = (*block_number_stats)[i];
            range_stats.block_offset_stat = (*block_offset_stats)[i];
        }
    }

    stats_entry->initialized = true;
    return stats_entry;
}

PatchJoinCache::PatchStatsEntryPtr PatchJoinCache::getOrCreatePatchStats(const String & patch_name)
{
    std::lock_guard lock(mutex);
    auto & entry = stats_cache[patch_name];
    if (!entry)
        entry = std::make_shared<PatchStatsEntry>();
    return entry;
}

PatchJoinCache::Entries PatchJoinCache::getEntries(const String & patch_name, const MarkRanges & ranges, Reader reader)
{
    auto [entries, ranges_for_entries] = getEntriesAndRanges(patch_name, ranges);
    std::vector<std::shared_future<void>> futures;

    for (size_t i = 0; i < entries.size(); ++i)
    {
        auto entry_futures = entries[i]->addRangesAsync(ranges_for_entries[i], reader);
        futures.insert(futures.end(), entry_futures.begin(), entry_futures.end());
    }

    for (const auto & future : futures)
        future.get();

    return entries;
}

std::pair<PatchJoinCache::Entries, std::vector<MarkRanges>> PatchJoinCache::getEntriesAndRanges(const String & patch_name, const MarkRanges & ranges)
{
    std::lock_guard lock(mutex);
    const auto & entries = cache.at(patch_name);

    if (entries.empty())
        return {};

    std::map<size_t, MarkRanges> ranges_for_entries;
    const auto & buckets = ranges_to_buckets.at(patch_name);

    for (const auto & range : ranges)
    {
        size_t idx = buckets.at(range);
        ranges_for_entries[idx].push_back(range);
    }

    Entries result_entries;
    std::vector<MarkRanges> result_ranges;

    for (auto & [idx, ranges_for_entry] : ranges_for_entries)
    {
        result_entries.push_back(entries.at(idx));
        result_ranges.push_back(std::move(ranges_for_entry));
    }

    return {result_entries, result_ranges};
}

std::vector<std::shared_future<void>> PatchJoinCache::Entry::addRangesAsync(const MarkRanges & ranges, Reader reader)
{
    std::vector<std::shared_future<void>> futures;
    std::vector<std::shared_ptr<std::promise<void>>> promises;

    MarkRanges ranges_to_read;
    bool has_ranges_to_read = false;

    {
        /// Firstly lookup with read lock, because all needed ranges are likely read already.
        std::shared_lock lock(mutex);

        for (const auto & range : ranges)
        {
            auto it = ranges_futures.find(range);

            if (it != ranges_futures.end())
            {
                futures.push_back(it->second);
            }
            else
            {
                has_ranges_to_read = true;
                break;
            }
        }
    }

    if (has_ranges_to_read)
    {
        futures.clear();
        std::lock_guard lock(mutex);

        for (const auto & range : ranges)
        {
            auto it = ranges_futures.find(range);

            if (it != ranges_futures.end())
            {
                futures.push_back(it->second);
            }
            else
            {
                auto promise = std::make_shared<std::promise<void>>();
                auto future = promise->get_future().share();

                ranges_to_read.push_back(range);
                ranges_futures.emplace(range, future);

                futures.push_back(std::move(future));
                promises.push_back(std::move(promise));
            }
        }
    }

    if (!ranges_to_read.empty())
    {
        try
        {
            /// Read and add block in the same thread, because there
            /// is already enough concurrency on the level of read tasks.
            auto read_block = reader(ranges_to_read);
            addBlock(std::move(read_block));

            for (const auto & promise : promises)
                promise->set_value();
        }
        catch (...)
        {
            for (const auto & promise : promises)
                promise->set_exception(std::current_exception());
            throw;
        }
    }

    return futures;
}


void PatchJoinCache::Entry::addBlock(Block read_block)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::BuildPatchesJoinMicroseconds);

    size_t new_block_idx = 0;
    size_t num_read_rows = read_block.rows();

    if (num_read_rows == 0)
        return;

    /// System columns are not needed in block, they are stored in the hash map.
    auto block_with_data = std::make_shared<Block>(read_block);
    block_with_data->erase(BlockNumberColumn::name);
    block_with_data->erase(BlockOffsetColumn::name);

    {
        std::lock_guard lock(mutex);
        new_block_idx = blocks.size();
        blocks.push_back(std::move(block_with_data));
    }

    if (new_block_idx > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too large index of block ({}) in patch join cache", new_block_idx);

    if (num_read_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Too many rows ({}) in patch ranges", num_read_rows);

    const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);
    const auto & block_offset_column = getColumnUInt64Data(read_block, BlockOffsetColumn::name);
    const auto & data_version_column = getColumnUInt64Data(read_block, PartDataVersionColumn::name);

    /// Build hash map locally and then merge it with the global one under the lock.
    PatchHashMap local_hash_map;
    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    UInt64 local_min_block = std::numeric_limits<UInt64>::max();
    UInt64 local_max_block = 0;
    OffsetsHashMap * offsets_hash_map = nullptr;

    for (size_t i = 0; i < num_read_rows; ++i)
    {
        UInt64 block_number = block_number_column[i];
        UInt64 block_offset = block_offset_column[i];

        if (block_number != prev_block_number)
        {
            prev_block_number = block_number;
            offsets_hash_map = &local_hash_map[block_number];

            local_min_block = std::min(local_min_block, block_number);
            local_max_block = std::max(local_max_block, block_number);
        }

        auto [it, inserted] = offsets_hash_map->try_emplace(block_offset);

        /// Keep only the row with the highest version.
        if (inserted || data_version_column[i] > data_version_column[it->second.second])
            it->second = std::make_pair(static_cast<UInt32>(new_block_idx), static_cast<UInt32>(i));
    }

    ProfileEvents::increment(ProfileEvents::PatchesJoinRowsAddedToHashTable, num_read_rows);

    {
        std::lock_guard lock(mutex);

        min_block = std::min(min_block, local_min_block);
        max_block = std::max(max_block, local_max_block);

        for (auto & [block_number, offsets_map] : local_hash_map)
        {
            auto [it, inserted] = hash_map.try_emplace(block_number);

            if (inserted)
            {
                it->second = std::move(offsets_map);
            }
            else
            {
                it->second.reserve(it->second.size() + offsets_map.size());
                it->second.insert(offsets_map.begin(), offsets_map.end());
            }
        }
    }
}

}
