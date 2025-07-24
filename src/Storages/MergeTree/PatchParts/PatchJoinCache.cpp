#include <utility>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <Columns/ColumnsNumber.h>

namespace ProfileEvents
{
    extern const Event BuildPatchesJoinMicroseconds;
}

namespace DB
{

static const PaddedPODArray<UInt64> & getColumnUInt64Data(const Block & block, const String & column_name)
{
    return assert_cast<const ColumnUInt64 &>(*block.getByName(column_name).column).getData();
}

PatchJoinCache::PatchJoinCache(size_t num_buckets_, ThreadPool & thread_pool_)
    : num_buckets(num_buckets_), thread_pool(thread_pool_)
{
}

void PatchJoinCache::init(const RangesInPatchParts & ranges_in_pathces)
{
    const auto & all_ranges = ranges_in_pathces.getRanges();

    for (const auto & [patch_name, ranges] : all_ranges)
    {
        if (ranges.empty())
            continue;

        size_t current_buckets = std::min(num_buckets, ranges.size());
        size_t num_ranges_in_bucket = ranges.size() / current_buckets;

        auto & entries = cache[patch_name];
        auto & buckets = ranges_to_buckets[patch_name];

        entries.reserve(current_buckets);
        for (size_t i = 0; i < ranges.size(); ++i)
        {
            size_t idx = i / num_ranges_in_bucket;
            buckets[ranges[i]] = idx;
            entries.push_back(std::make_shared<PatchJoinCache::Entry>());
        }
    }
}

PatchJoinCache::Entries PatchJoinCache::getEntries(const String & patch_name, const MarkRanges & ranges, Reader reader)
{
    auto [entries, ranges_for_entries] = getEntriesAndRanges(patch_name, ranges);
    std::vector<std::shared_future<void>> futures;

    for (size_t i = 0; i < entries.size(); ++i)
    {
        auto entry_futures = entries[i]->addRangesAsync(ranges_for_entries[i], thread_pool, reader);
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

std::vector<std::shared_future<void>> PatchJoinCache::Entry::addRangesAsync(const MarkRanges & ranges, ThreadPool & pool, Reader reader)
{
    std::vector<std::shared_future<void>> futures;
    std::vector<std::shared_ptr<std::promise<void>>> promises;
    MarkRanges ranges_to_read;

    {
        std::lock_guard lock(mutex);

        for (const auto & range : ranges)
        {
            if (ranges_futures.contains(range))
            {
                futures.push_back(ranges_futures.at(range));
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
        pool.scheduleOrThrowOnError([this, ranges_to_read, reader, promises]
        {
            try
            {
                auto read_block = reader(ranges_to_read);
                addBlock(std::move(read_block));

                for (const auto & promise : promises)
                    promise->set_value();
            }
            catch (...)
            {
                for (const auto & promise : promises)
                    promise->set_exception(std::current_exception());
            }
        });
    }

    return futures;
}

void PatchJoinCache::Entry::addBlock(Block read_block)
{
    Stopwatch watch;

    size_t num_read_rows = read_block.rows();
    if (num_read_rows == 0)
        return;

    std::lock_guard lock(mutex);
    size_t old_num_rows = block.rows();

    if (block.rows() == 0)
    {
        block = read_block;
    }
    else
    {
        for (size_t i = 0; i < read_block.columns(); ++i)
        {
            auto & result_column = block.safeGetByPosition(i).column;
            const auto & read_column = read_block.safeGetByPosition(i).column;
            result_column->assumeMutableRef().insertRangeFrom(*read_column, 0, read_column->size());
        }
    }

    const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);
    const auto & block_offset_column = getColumnUInt64Data(read_block, BlockOffsetColumn::name);
    const auto & data_version_column = getColumnUInt64Data(read_block, PartDataVersionColumn::name);

    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    OffsetsHashMap * offsets_hash_map = nullptr;

    for (size_t i = 0; i < num_read_rows; ++i)
    {
        UInt64 block_number = block_number_column[i];
        UInt64 block_offset = block_offset_column[i];

        if (block_number != prev_block_number)
        {
            prev_block_number = block_number;
            offsets_hash_map = &hash_map[block_number];

            min_block = std::min(min_block, block_number);
            max_block = std::max(max_block, block_number);
        }

        auto [it, inserted] = offsets_hash_map->try_emplace(block_offset);

        /// Keep only the row with the highest version.
        if (inserted || data_version_column[i] > data_version_column[it->second])
            it->second = i + old_num_rows;
    }

    auto elapsed = watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::BuildPatchesJoinMicroseconds, elapsed);
}

}
