#include "Storages/MergeTree/MergeTreeBlockReadUtils.h"
#include "Storages/MergeTree/MergeTreeReadTask.h"
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <base/range.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/RequestResponse.h>


namespace ProfileEvents
{
    extern const Event SlowRead;
    extern const Event ReadBackoff;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

size_t getApproxSizeOfPart(const IMergeTreeDataPart & part, const Names & columns_to_read)
{
    ColumnSize columns_size{};
    for (const auto & col_name : columns_to_read)
        columns_size.add(part.getColumnSize(col_name));
    return columns_size.data_compressed;
}

MergeTreeReadPool::MergeTreeReadPool(
    RangesInDataParts && parts_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const Names & virtual_column_names_,
    const PoolSettings & settings_,
    const ContextPtr & context_)
    : MergeTreeReadPoolBase(
        std::move(parts_),
        storage_snapshot_,
        prewhere_info_,
        actions_settings_,
        reader_settings_,
        column_names_,
        virtual_column_names_,
        settings_,
        context_)
    , min_marks_for_concurrent_read(pool_settings.min_marks_for_concurrent_read)
    , backoff_settings{context_->getSettingsRef()}
    , backoff_state{pool_settings.threads}
{
    if (std::ranges::count(is_part_on_remote_disk, true))
    {
        const auto & settings = context_->getSettingsRef();

        size_t total_compressed_bytes = 0;
        size_t total_marks = 0;
        for (const auto & part : parts_ranges)
        {
            const auto & columns = settings.merge_tree_determine_task_size_by_prewhere_columns && prewhere_info
                ? prewhere_info->prewhere_actions->getRequiredColumnsNames()
                : column_names_;

            total_compressed_bytes += getApproxSizeOfPart(*part.data_part, columns);
            total_marks += part.getMarksCount();
        }

        if (total_marks)
        {
            const auto min_bytes_per_task = settings.merge_tree_min_bytes_per_task_for_remote_reading;
            const auto avg_mark_bytes = std::max<size_t>(total_compressed_bytes / total_marks, 1);
            /// We're taking min here because number of tasks shouldn't be too low - it will make task stealing impossible.
            const auto heuristic_min_marks = std::min<size_t>(total_marks / pool_settings.threads, min_bytes_per_task / avg_mark_bytes);

            if (heuristic_min_marks > min_marks_for_concurrent_read)
                min_marks_for_concurrent_read = heuristic_min_marks;
        }
    }

    fillPerThreadInfo(pool_settings.threads, pool_settings.sum_marks);
}

MergeTreeReadTaskPtr MergeTreeReadPool::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    const std::lock_guard lock{mutex};

    /// If number of threads was lowered due to backoff, then will assign work only for maximum 'backoff_state.current_threads' threads.
    if (task_idx >= backoff_state.current_threads)
        return nullptr;

    if (remaining_thread_tasks.empty())
        return nullptr;

    const auto tasks_remaining_for_this_thread = !threads_tasks[task_idx].sum_marks_in_parts.empty();
    if (!tasks_remaining_for_this_thread && pool_settings.do_not_steal_tasks)
        return nullptr;

    /// Steal task if nothing to do and it's not prohibited
    auto thread_idx = task_idx;
    if (!tasks_remaining_for_this_thread)
    {
        auto it = remaining_thread_tasks.lower_bound(backoff_state.current_threads);
        // Grab the entire tasks of a thread which is killed by backoff
        if (it != remaining_thread_tasks.end())
        {
            threads_tasks[task_idx] = std::move(threads_tasks[*it]);
            remaining_thread_tasks.erase(it);
            remaining_thread_tasks.insert(task_idx);
        }
        else // Try steal tasks from the next thread
        {
            it = remaining_thread_tasks.upper_bound(task_idx);
            if (it == remaining_thread_tasks.end())
                it = remaining_thread_tasks.begin();
            thread_idx = *it;
        }
    }

    auto & thread_tasks = threads_tasks[thread_idx];
    auto & thread_task = thread_tasks.parts_and_ranges.back();

    const auto part_idx = thread_task.part_idx;
    auto & marks_in_part = thread_tasks.sum_marks_in_parts.back();

    size_t need_marks;
    if (is_part_on_remote_disk[part_idx] && !pool_settings.use_const_size_tasks_for_remote_reading)
        need_marks = marks_in_part;
    else /// Get whole part to read if it is small enough.
        need_marks = std::min(marks_in_part, min_marks_for_concurrent_read);

    /// Do not leave too little rows in part for next time.
    if (marks_in_part > need_marks && marks_in_part - need_marks < min_marks_for_concurrent_read / 2)
        need_marks = marks_in_part;

    MarkRanges ranges_to_get_from_part;

    /// Get whole part to read if it is small enough.
    if (marks_in_part <= need_marks)
    {
        ranges_to_get_from_part = thread_task.ranges;
        marks_in_part = 0;

        thread_tasks.parts_and_ranges.pop_back();
        thread_tasks.sum_marks_in_parts.pop_back();

        if (thread_tasks.sum_marks_in_parts.empty())
            remaining_thread_tasks.erase(thread_idx);
    }
    else
    {
        /// Loop through part ranges.
        while (need_marks > 0 && !thread_task.ranges.empty())
        {
            auto & range = thread_task.ranges.front();

            const size_t marks_in_range = range.end - range.begin;
            const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

            ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
            range.begin += marks_to_get_from_range;
            if (range.begin == range.end)
                thread_task.ranges.pop_front();

            marks_in_part -= marks_to_get_from_range;
            need_marks -= marks_to_get_from_range;
        }
    }

    return createTask(per_part_infos[part_idx], std::move(ranges_to_get_from_part), previous_task);
}

void MergeTreeReadPool::profileFeedback(ReadBufferFromFileBase::ProfileInfo info)
{
    if (backoff_settings.min_read_latency_ms == 0 || pool_settings.do_not_steal_tasks)
        return;

    if (info.nanoseconds < backoff_settings.min_read_latency_ms * 1000000)
        return;

    std::lock_guard lock(mutex);

    if (backoff_state.current_threads <= backoff_settings.min_concurrency)
        return;

    size_t throughput = info.bytes_read * 1000000000 / info.nanoseconds;

    if (throughput >= backoff_settings.max_throughput)
        return;

    if (backoff_state.time_since_prev_event.elapsed() < backoff_settings.min_interval_between_events_ms * 1000000)
        return;

    backoff_state.time_since_prev_event.restart();
    ++backoff_state.num_events;

    ProfileEvents::increment(ProfileEvents::SlowRead);
    LOG_DEBUG(log, "Slow read, event №{}: read {} bytes in {} sec., {}/s.",
        backoff_state.num_events, info.bytes_read, info.nanoseconds / 1e9,
        ReadableSize(throughput));

    if (backoff_state.num_events < backoff_settings.min_events)
        return;

    backoff_state.num_events = 0;
    --backoff_state.current_threads;

    ProfileEvents::increment(ProfileEvents::ReadBackoff);
    LOG_DEBUG(log, "Will lower number of threads to {}", backoff_state.current_threads);
}

void MergeTreeReadPool::fillPerThreadInfo(size_t threads, size_t sum_marks)
{
    threads_tasks.resize(threads);
    if (parts_ranges.empty())
        return;

    struct PartInfo
    {
        RangesInDataPart part;
        size_t sum_marks;
        size_t part_idx;
    };

    using PartsInfo = std::vector<PartInfo>;
    std::queue<PartsInfo> parts_queue;

    auto per_part_sum_marks = getPerPartSumMarks();

    {
        /// Group parts by disk name.
        /// We try minimize the number of threads concurrently read from the same disk.
        /// It improves the performance for JBOD architecture.
        std::map<String, std::vector<PartInfo>> parts_per_disk;

        for (size_t i = 0; i < parts_ranges.size(); ++i)
        {
            PartInfo part_info{parts_ranges[i], per_part_sum_marks[i], i};
            if (parts_ranges[i].data_part->isStoredOnDisk())
                parts_per_disk[parts_ranges[i].data_part->getDataPartStorage().getDiskName()].push_back(std::move(part_info));
            else
                parts_per_disk[""].push_back(std::move(part_info));
        }

        for (auto & info : parts_per_disk)
            parts_queue.push(std::move(info.second));
    }

    LOG_DEBUG(log, "min_marks_for_concurrent_read={}", min_marks_for_concurrent_read);

    const size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;

    for (size_t i = 0; i < threads && !parts_queue.empty(); ++i)
    {
        auto need_marks = min_marks_per_thread;

        while (need_marks > 0 && !parts_queue.empty())
        {
            auto & current_parts = parts_queue.front();
            auto & part_with_ranges = current_parts.back().part;
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
                ranges_to_get_from_part = part_with_ranges.ranges;
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
                    if (part_with_ranges.ranges.empty())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected end of ranges while spreading marks among threads");

                    MarkRange & range = part_with_ranges.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part_with_ranges.ranges.pop_front();
                }
            }

            threads_tasks[i].parts_and_ranges.push_back({part_idx, ranges_to_get_from_part});
            threads_tasks[i].sum_marks_in_parts.push_back(marks_in_ranges);
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
