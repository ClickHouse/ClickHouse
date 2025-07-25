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

PatchJoinCache::EntryPtr PatchJoinCache::getEntry(const String & patch_name, const MarkRanges & ranges, Reader reader)
{
    auto entry = getOrEmplaceEntry(patch_name);
    auto futures = entry->addRangesAsync(ranges, thread_pool, reader);

    for (const auto & future : futures)
        future.get();

    return entry;
}

PatchJoinCache::EntryPtr PatchJoinCache::getOrEmplaceEntry(const String & patch_name)
{
    std::lock_guard lock(mutex);

    auto & entry = cache[patch_name];
    if (!entry)
        entry = std::make_shared<Entry>();

    return entry;
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

    size_t new_block_idx = 0;
    size_t num_read_rows = read_block.rows();

    if (num_read_rows == 0)
        return;

    {
        std::lock_guard lock(mutex);
        new_block_idx = blocks.size();
        blocks.push_back(std::make_shared<Block>(read_block));
    }

    PatchHashMap local_hash_map;

    const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);
    const auto & block_offset_column = getColumnUInt64Data(read_block, BlockOffsetColumn::name);
    const auto & data_version_column = getColumnUInt64Data(read_block, PartDataVersionColumn::name);

    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    UInt64 lock_min_block = std::numeric_limits<UInt64>::max();
    UInt64 lock_max_block = 0;
    OffsetsHashMap * offsets_hash_map = nullptr;

    for (size_t i = 0; i < num_read_rows; ++i)
    {
        UInt64 block_number = block_number_column[i];
        UInt64 block_offset = block_offset_column[i];

        if (block_number != prev_block_number)
        {
            prev_block_number = block_number;
            offsets_hash_map = &local_hash_map[block_number];

            lock_min_block = std::min(lock_min_block, block_number);
            lock_max_block = std::max(lock_max_block, block_number);
        }

        auto [it, inserted] = offsets_hash_map->try_emplace(block_offset);

        /// Keep only the row with the highest version.
        if (inserted || data_version_column[i] > data_version_column[it->second.second])
            it->second = std::make_pair(new_block_idx, i);
    }

    {
        std::lock_guard lock(mutex);

        min_block = std::min(min_block, lock_min_block);
        max_block = std::max(max_block, lock_max_block);

        for (auto & [block_number, offsets_map] : local_hash_map)
        {
            auto [it, inserted] = hash_map.try_emplace(block_number);

            if (inserted)
                it->second = std::move(offsets_map);
            else
                it->second.insert(offsets_map.begin(), offsets_map.end());
        }
    }

    auto elapsed = watch.elapsedMicroseconds();
    ProfileEvents::increment(ProfileEvents::BuildPatchesJoinMicroseconds, elapsed);
}

}
