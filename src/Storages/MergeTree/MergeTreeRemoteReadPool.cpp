#include <Storages/MergeTree/MergeTreeRemoteReadPool.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Common/formatReadable.h>
#include <base/range.h>


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

MergeTreeRemoteReadPool::MergeTreeRemoteReadPool(
    size_t threads_,
    size_t sum_marks_,
    size_t min_marks_for_concurrent_read_,
    RangesInDataParts && parts_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const Names & column_names_,
    const Names & virtual_column_names_,
    size_t preferred_block_size_bytes_)
    : storage_snapshot{storage_snapshot_}
{
    fillPerThreadInfo(
        threads_, sum_marks_, parts_ranges, min_marks_for_concurrent_read_,
        prewhere_info_, column_names_, virtual_column_names_, preferred_block_size_bytes_);
}

MergeTreeReadTaskPtr MergeTreeRemoteReadPool::getTask(size_t, size_t thread, const Names &)
{
    const std::lock_guard lock{mutex};

    auto & read_tasks = threads_tasks[thread];

    if (read_tasks.empty())
        return nullptr;

    auto read_task = std::move(read_tasks.back());
    read_tasks.pop_back();

    return read_task;
}

Block MergeTreeRemoteReadPool::getHeader() const
{
    return storage_snapshot->getSampleBlockForColumns(column_names);
}

MergeTreeRemoteReadPool::PartsInfos MergeTreeRemoteReadPool::getPartsInfosGroupedByDiskName(
    const RangesInDataParts & parts,
    const PrewhereInfoPtr & prewhere_info,
    const Names & column_names,
    const Names & virtual_column_names,
    size_t preferred_block_size_bytes)
{
    PartInfos parts_infos;
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    /// Group parts by disk name.
    /// We try minimize the number of threads concurrently read from the same disk.
    std::map<String, std::vector<PartInfo>> parts_per_disk;

    for (const auto & part : parts)
    {
        PartInfo part_info;

        part_info.data_part = part.data_part;
        part_info.part_index_in_query = part.part_index_in_query;

        /// Sum up total size of all mark ranges in a data part.
        for (const auto & range : part.ranges)
            part_info.sum_marks += range.end - range.begin;

        const auto task_columns = getReadTaskColumns(
            LoadedMergeTreeDataPartInfoForReader(part.data_part),
            storage_snapshot,
            column_names,
            virtual_column_names,
            prewhere_info,
            /*with_subcolumns=*/true);

        part_info.size_predictor = !predict_block_size_bytes
            ? nullptr
            : MergeTreeBaseSelectProcessor::getSizePredictor(part.data_part, task_columns, sample_block);

        /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
        const auto & required_column_names = task_columns.columns.getNames();
        part_info.column_name_set = {required_column_names.begin(), required_column_names.end()};
        part_info.task_columns = std::move(task_columns);

        if (part.data_part->isStoredOnDisk())
            parts_per_disk[part.data_part->getDataPartStorage().getDiskName()].push_back(std::move(part_info));
        else
            parts_per_disk[""].push_back(std::move(part_info));
    }

    for (auto & info : parts_per_disk)
        parts_infos.push(std::move(info.second));

    return parts_infos;
}


void MergeTreeRemoteReadPool::fillPerThreadInfo(
    size_t threads,
    size_t sum_marks,
    const RangesInDataParts & parts,
    size_t min_marks_for_concurrent_read,
    const PrewhereInfoPtr & prewhere_info,
    const Names & column_names,
    const Names & virtual_column_names,
    size_t preferred_block_size_bytes)
{
    threads_tasks.resize(threads);

    if (parts.empty())
        return;

    const auto parts_queue = getPartsInfosGroupedByDiskName(parts, prewhere_info);
    const size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;

    for (size_t i = 0; i < threads && !parts_queue.empty(); ++i)
    {
        auto need_marks = min_marks_per_thread;

        while (need_marks > 0 && !parts_queue.empty())
        {
            auto & current_parts = parts_queue.front();
            RangesInDataPart & part = current_parts.back().part;
            size_t & marks_in_part = current_parts.back().sum_marks;
            const auto part_idx = current_parts.back().part_idx;

            /// Do not get too few rows from part.
            if (marks_in_part >= min_marks_for_concurrent_read &&
                need_marks < min_marks_for_concurrent_read)
                need_marks = min_marks_for_concurrent_read;

            /// Do not leave too few rows in part for next time.
            if (marks_in_part > need_marks &&
                marks_in_part - need_marks < min_marks_for_concurrent_read)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;
            size_t marks_in_ranges = need_marks;

            /// Get whole part to read if it is small enough.
            if (marks_in_part <= need_marks)
            {
                ranges_to_get_from_part = part.ranges;
                marks_in_ranges = marks_in_part;

                need_marks -= marks_in_part;
                current_parts.pop_back();
                if (current_parts.empty())
                    parts_queue.pop();
            }
            else
            {
                /// Loop through part ranges.
                while (need_marks > 0)
                {
                    if (part.ranges.empty())
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Unexpected end of ranges while spreading marks among threads");
                    }

                    MarkRange & range = part.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part.ranges.pop_front();
                }
            }

            const auto & per_part = per_part_params[part_idx];
            auto curr_task_size_predictor = !per_part.size_predictor ? nullptr
                : std::make_unique<MergeTreeBlockSizePredictor>(*per_part.size_predictor); /// make a copy

            auto read_task = std::make_unique<MergeTreeReadTask>(
                part.data_part, ranges_to_get_from_part, part.part_index_in_query, /* ordered_names */Names{}, /// FIXME
                per_part.column_name_set, per_part.task_columns,
                prewhere_info && prewhere_info->remove_prewhere_column, std::move(curr_task_size_predictor));

            threads_tasks[i].push_back(std::move(read_task));

            if (marks_in_ranges != 0)
                remaining_thread_tasks.insert(i);
        }

        /// Before processing next thread, change disk if possible.
        /// Different threads will likely start reading from different disk,
        /// which may improve read parallelism for JBOD.
        /// It also may be helpful in case we have backoff threads.
        /// Backoff threads will likely to reduce load for different disks, not the same one.
        if (parts_queue.size() > 1)
        {
            parts_queue.push(std::move(parts_queue.front()));
            parts_queue.pop();
        }
    }
}


}
