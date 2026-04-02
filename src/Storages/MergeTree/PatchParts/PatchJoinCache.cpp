#include <utility>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace CurrentMetrics
{
    extern const Metric BuildPatchJoinCacheThreads;
    extern const Metric BuildPatchJoinCacheThreadsActive;
    extern const Metric BuildPatchJoinCacheThreadsScheduled;
}

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

void PatchJoinCache::init(const RangesInPatchParts & ranges_in_patches, size_t num_buckets_)
{
    num_buckets = num_buckets_;

    entries.resize(num_buckets);
    for (size_t i = 0; i < num_buckets; ++i)
        entries[i] = std::make_shared<Entry>();

    const auto & all_ranges = ranges_in_patches.getRanges();
    for (const auto & [patch_name, ranges] : all_ranges)
    {
        if (!ranges.empty())
            all_ranges_by_name[patch_name] = ranges;
    }
}

void PatchJoinCache::build(const ReaderFactory & reader_factory, size_t num_threads)
{
    if (all_ranges_by_name.empty())
    {
        built = true;
        return;
    }

    /// Flatten all ranges into work items: (patch_name, single_range).
    struct ReadWorkItem
    {
        String patch_name;
        MarkRange range;
    };

    std::vector<ReadWorkItem> work_items;
    for (const auto & [patch_name, ranges] : all_ranges_by_name)
        for (const auto & range : ranges)
            work_items.push_back({patch_name, range});

    if (work_items.empty())
    {
        built = true;
        return;
    }

    size_t actual_threads = std::min(num_threads, work_items.size());
    if (actual_threads == 0)
        actual_threads = 1;

    /// Phase 1: Read ranges in parallel. Each thread produces per-bucket sub-blocks.
    /// per_thread_buckets[thread_id][bucket_id] = list of sub-blocks for that bucket.
    std::vector<std::vector<std::vector<Block>>> per_thread_buckets(actual_threads);
    for (auto & thread_buckets : per_thread_buckets)
        thread_buckets.resize(num_buckets);

    /// Distribute work items evenly across threads.
    size_t items_per_thread = (work_items.size() + actual_threads - 1) / actual_threads;

    auto read_task = [&](size_t thread_id, size_t begin, size_t end)
    {
        auto & my_buckets = per_thread_buckets[thread_id];

        /// Group consecutive work items by patch_name to reuse reader.
        String current_patch_name;
        Reader current_reader;

        for (size_t wi = begin; wi < end; ++wi)
        {
            const auto & item = work_items[wi];

            if (item.patch_name != current_patch_name)
            {
                current_patch_name = item.patch_name;
                current_reader = reader_factory(current_patch_name);
            }

            MarkRanges single_range = {item.range};
            Block read_block = current_reader(single_range);

            size_t num_read_rows = read_block.rows();
            if (num_read_rows == 0)
                continue;

            if (num_buckets == 1)
            {
                my_buckets[0].push_back(std::move(read_block));
                continue;
            }

            const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);

            /// Classify rows by bucket.
            std::vector<std::vector<size_t>> rows_by_bucket(num_buckets);
            for (size_t i = 0; i < num_read_rows; ++i)
                rows_by_bucket[block_number_column[i] % num_buckets].push_back(i);

            for (size_t bucket = 0; bucket < num_buckets; ++bucket)
            {
                const auto & row_indices = rows_by_bucket[bucket];
                if (row_indices.empty())
                    continue;

                auto columns = read_block.cloneEmpty().mutateColumns();
                for (auto & col : columns)
                    col->reserve(row_indices.size());

                for (size_t row : row_indices)
                    for (size_t col = 0; col < columns.size(); ++col)
                        columns[col]->insertFrom(*read_block.getByPosition(col).column, row);

                my_buckets[bucket].push_back(read_block.cloneWithColumns(std::move(columns)));
            }
        }
    };

    if (actual_threads <= 1)
    {
        read_task(0, 0, work_items.size());
    }
    else
    {
        /// Sort work items by patch_name so consecutive items for the same patch are together.
        /// This allows each thread to reuse readers for consecutive ranges of the same patch.
        std::stable_sort(work_items.begin(), work_items.end(),
            [](const auto & a, const auto & b) { return a.patch_name < b.patch_name; });

        ThreadPool pool(
            CurrentMetrics::BuildPatchJoinCacheThreads,
            CurrentMetrics::BuildPatchJoinCacheThreadsActive,
            CurrentMetrics::BuildPatchJoinCacheThreadsScheduled,
            actual_threads);

        for (size_t t = 0; t < actual_threads; ++t)
        {
            size_t begin = t * items_per_thread;
            size_t end = std::min(begin + items_per_thread, work_items.size());
            if (begin >= end)
                break;

            pool.scheduleOrThrow(
                [&, t, begin, end, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_INDEX);
                    read_task(t, begin, end);
                });
        }

        pool.wait();
    }

    /// Phase 2: Fill each bucket from all threads' sub-blocks. No locks needed —
    /// each bucket is written by exactly one thread.
    auto fill_bucket = [&](size_t bucket)
    {
        auto & entry = *entries[bucket];
        for (size_t t = 0; t < actual_threads; ++t)
            for (auto & block : per_thread_buckets[t][bucket])
                entry.addBlock(std::move(block));
    };

    if (actual_threads <= 1)
    {
        for (size_t b = 0; b < num_buckets; ++b)
            fill_bucket(b);
    }
    else
    {
        ThreadPool pool(
            CurrentMetrics::BuildPatchJoinCacheThreads,
            CurrentMetrics::BuildPatchJoinCacheThreadsActive,
            CurrentMetrics::BuildPatchJoinCacheThreadsScheduled,
            actual_threads);

        for (size_t b = 0; b < num_buckets; ++b)
        {
            pool.scheduleOrThrow(
                [&, b, thread_group = CurrentThread::getGroup()]
                {
                    ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGETREE_INDEX);
                    fill_bucket(b);
                });
        }

        pool.wait();
    }

    built = true;
}

void PatchJoinCache::Entry::addBlock(Block read_block)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::BuildPatchesJoinMicroseconds);

    size_t num_read_rows = read_block.rows();
    if (num_read_rows == 0)
        return;

    ProfileEvents::increment(ProfileEvents::PatchesJoinRowsAddedToHashTable, num_read_rows);

    const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);
    const auto & block_offset_column = getColumnUInt64Data(read_block, BlockOffsetColumn::name);
    const auto & data_version_column = getColumnUInt64Data(read_block, PartDataVersionColumn::name);

    /// Build a data block without system columns used only for the hash map.
    Block data_block(read_block);
    data_block.erase(BlockNumberColumn::name);
    data_block.erase(BlockOffsetColumn::name);
    size_t version_column_position = data_block.getPositionByName(PartDataVersionColumn::name);

    size_t base_row_offset = block.rows();

    if (base_row_offset == 0)
    {
        block = std::move(data_block);
    }
    else
    {
#ifdef DEBUG_OR_SANITIZER_BUILD
        assertCompatibleHeader(data_block, block, "patch join cache");
#endif
        auto mutable_columns = block.mutateColumns();
        for (size_t col = 0; col < mutable_columns.size(); ++col)
            mutable_columns[col]->insertRangeFrom(*data_block.getByPosition(col).column, 0, num_read_rows);
        block.setColumns(std::move(mutable_columns));
    }

    if (num_read_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Too many rows ({}) in patch ranges", num_read_rows);

    if (base_row_offset + num_read_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too large row offset ({}) in patch join cache", base_row_offset + num_read_rows);

    PatchOffsetsMap * current_offsets = nullptr;
    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    /// Offsets are mostly sorted, so use the iterator to latest inserted offset as a hint
    /// to optimize the insertion. It gives average complexity of O(1) instead of O(log n).
    PatchOffsetsMap::const_iterator last_inserted_it;

    for (size_t i = 0; i < num_read_rows; ++i)
    {
        UInt64 block_number = block_number_column[i];
        UInt64 block_offset = block_offset_column[i];

        if (block_number != prev_block_number)
        {
            prev_block_number = block_number;
            current_offsets = &hash_map[block_number];

            min_block = std::min(min_block, block_number);
            max_block = std::max(max_block, block_number);
            last_inserted_it = current_offsets->end();
        }

        /// try_emplace overload with hint doesn't return 'inserted' flag,
        /// so we need to check size before and after emplace.
        size_t old_size = current_offsets->size();
        auto it = current_offsets->try_emplace(last_inserted_it, block_offset);
        last_inserted_it = it;
        bool inserted = current_offsets->size() > old_size;

        if (inserted)
        {
            it->second = static_cast<UInt32>(base_row_offset + i);
        }
        else
        {
            UInt32 existing_row = it->second;
            const auto & existing_version_column = block.getByPosition(version_column_position).column;

            UInt64 current_version = data_version_column[i];
            UInt64 existing_version = assert_cast<const ColumnUInt64 &>(*existing_version_column).getData()[existing_row];
            chassert(current_version != existing_version);

            /// Keep only the row with the highest version.
            if (current_version > existing_version)
                it->second = static_cast<UInt32>(base_row_offset + i);
        }
    }
}

}
