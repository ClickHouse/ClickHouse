#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
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
    size_t threads_,
    size_t sum_marks_,
    size_t min_marks_for_concurrent_read_,
    RangesInDataParts && parts_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const Names & virtual_column_names_,
    ContextPtr context_,
    bool do_not_steal_tasks_)
    : storage_snapshot(storage_snapshot_)
    , column_names(column_names_)
    , virtual_column_names(virtual_column_names_)
    , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , reader_settings(reader_settings_)
    , parts_ranges(std::move(parts_))
    , predict_block_size_bytes(context_->getSettingsRef().preferred_block_size_bytes > 0)
    , do_not_steal_tasks(do_not_steal_tasks_)
    , merge_tree_use_const_size_tasks_for_remote_reading(context_->getSettingsRef().merge_tree_use_const_size_tasks_for_remote_reading)
    , backoff_settings{context_->getSettingsRef()}
    , backoff_state{threads_}
{
    /// parts don't contain duplicate MergeTreeDataPart's.
    const auto per_part_sum_marks = fillPerPartInfo(
        parts_ranges, storage_snapshot, is_part_on_remote_disk,
        predict_block_size_bytes,
        column_names, virtual_column_names, prewhere_info,
        actions_settings, reader_settings, per_part_params);

    if (std::ranges::count(is_part_on_remote_disk, true))
    {
        const auto & settings = context_->getSettingsRef();

        size_t total_compressed_bytes = 0;
        size_t total_marks = 0;
        for (const auto & part : parts_ranges)
        {
            total_compressed_bytes += getApproxSizeOfPart(
                *part.data_part, prewhere_info ? prewhere_info->prewhere_actions->getRequiredColumnsNames() : column_names_);
            total_marks += part.getMarksCount();
        }

        if (total_marks)
        {
            const auto min_bytes_per_task = settings.merge_tree_min_bytes_per_task_for_remote_reading;
            const auto avg_mark_bytes = std::max<size_t>(total_compressed_bytes / total_marks, 1);
            /// We're taking min here because number of tasks shouldn't be too low - it will make task stealing impossible.
            const auto heuristic_min_marks = std::min<size_t>(total_marks / threads_, min_bytes_per_task / avg_mark_bytes);
            if (heuristic_min_marks > min_marks_for_concurrent_read)
            {
                min_marks_for_concurrent_read = heuristic_min_marks;
            }
        }
    }

    fillPerThreadInfo(threads_, sum_marks_, per_part_sum_marks, parts_ranges);
}

std::vector<size_t> MergeTreeReadPool::fillPerPartInfo(
    const RangesInDataParts & parts,
    const StorageSnapshotPtr & storage_snapshot,
    std::vector<bool> & is_part_on_remote_disk,
    bool & predict_block_size_bytes,
    const Names & column_names,
    const Names & virtual_column_names,
    const PrewhereInfoPtr & prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    const MergeTreeReaderSettings & reader_settings,
    std::vector<MergeTreeReadPool::PerPartParams> & per_part_params)
{
    std::vector<size_t> per_part_sum_marks;
    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    is_part_on_remote_disk.resize(parts.size());

    for (const auto i : collections::range(0, parts.size()))
    {
        const auto & part = parts[i];
#ifndef NDEBUG
        assertSortedAndNonIntersecting(part.ranges);
#endif

        bool part_on_remote_disk = part.data_part->isStoredOnRemoteDisk();
        is_part_on_remote_disk[i] = part_on_remote_disk;

        /// Read marks for every data part.
        size_t sum_marks = 0;
        for (const auto & range : part.ranges)
            sum_marks += range.end - range.begin;

        per_part_sum_marks.push_back(sum_marks);

        auto & per_part = per_part_params.emplace_back();
        per_part.data_part = part;

        LoadedMergeTreeDataPartInfoForReader part_info(part.data_part, part.alter_conversions);
        auto task_columns = getReadTaskColumns(
            part_info, storage_snapshot, column_names, virtual_column_names,
            prewhere_info, actions_settings,
            reader_settings, /*with_subcolumns=*/ true);

        auto size_predictor = !predict_block_size_bytes ? nullptr
            : IMergeTreeSelectAlgorithm::getSizePredictor(part.data_part, task_columns, sample_block);

        per_part.size_predictor = std::move(size_predictor);

        /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
        const auto & required_column_names = task_columns.columns.getNames();
        per_part.column_name_set = {required_column_names.begin(), required_column_names.end()};
        per_part.task_columns = std::move(task_columns);
    }

    return per_part_sum_marks;
}

MergeTreeReadTaskPtr MergeTreeReadPool::getTask(size_t thread)
{
    const std::lock_guard lock{mutex};

    /// If number of threads was lowered due to backoff, then will assign work only for maximum 'backoff_state.current_threads' threads.
    if (thread >= backoff_state.current_threads)
        return nullptr;

    if (remaining_thread_tasks.empty())
        return nullptr;

    const auto tasks_remaining_for_this_thread = !threads_tasks[thread].sum_marks_in_parts.empty();
    if (!tasks_remaining_for_this_thread && do_not_steal_tasks)
        return nullptr;

    /// Steal task if nothing to do and it's not prohibited
    auto thread_idx = thread;
    if (!tasks_remaining_for_this_thread)
    {
        auto it = remaining_thread_tasks.lower_bound(backoff_state.current_threads);
        // Grab the entire tasks of a thread which is killed by backoff
        if (it != remaining_thread_tasks.end())
        {
            threads_tasks[thread] = std::move(threads_tasks[*it]);
            remaining_thread_tasks.erase(it);
            remaining_thread_tasks.insert(thread);
        }
        else // Try steal tasks from the next thread
        {
            it = remaining_thread_tasks.upper_bound(thread);
            if (it == remaining_thread_tasks.end())
                it = remaining_thread_tasks.begin();
            thread_idx = *it;
        }
    }
    auto & thread_tasks = threads_tasks[thread_idx];

    auto & thread_task = thread_tasks.parts_and_ranges.back();
    const auto part_idx = thread_task.part_idx;

    auto & part = per_part_params[part_idx].data_part;
    auto & marks_in_part = thread_tasks.sum_marks_in_parts.back();

    size_t need_marks;
    if (is_part_on_remote_disk[part_idx] && !merge_tree_use_const_size_tasks_for_remote_reading)
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

    const auto & per_part = per_part_params[part_idx];
    auto curr_task_size_predictor = !per_part.size_predictor ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(*per_part.size_predictor); /// make a copy

    return std::make_unique<MergeTreeReadTask>(
        part.data_part,
        part.alter_conversions,
        ranges_to_get_from_part,
        part.part_index_in_query,
        per_part.column_name_set,
        per_part.task_columns,
        std::move(curr_task_size_predictor));
}

Block MergeTreeReadPool::getHeader() const
{
    return storage_snapshot->getSampleBlockForColumns(column_names);
}

void MergeTreeReadPool::profileFeedback(ReadBufferFromFileBase::ProfileInfo info)
{
    if (backoff_settings.min_read_latency_ms == 0 || do_not_steal_tasks)
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


void MergeTreeReadPool::fillPerThreadInfo(
    size_t threads, size_t sum_marks, std::vector<size_t> per_part_sum_marks,
    const RangesInDataParts & parts)
{
    threads_tasks.resize(threads);
    if (parts.empty())
        return;

    struct PartInfo
    {
        RangesInDataPart part;
        size_t sum_marks;
        size_t part_idx;
    };

    using PartsInfo = std::vector<PartInfo>;
    std::queue<PartsInfo> parts_queue;

    {
        /// Group parts by disk name.
        /// We try minimize the number of threads concurrently read from the same disk.
        /// It improves the performance for JBOD architecture.
        std::map<String, std::vector<PartInfo>> parts_per_disk;

        for (size_t i = 0; i < parts.size(); ++i)
        {
            PartInfo part_info{parts[i], per_part_sum_marks[i], i};
            if (parts[i].data_part->isStoredOnDisk())
                parts_per_disk[parts[i].data_part->getDataPartStorage().getDiskName()].push_back(std::move(part_info));
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
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected end of ranges while spreading marks among threads");

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

            threads_tasks[i].parts_and_ranges.push_back({ part_idx, ranges_to_get_from_part });
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


MergeTreeReadPoolParallelReplicas::~MergeTreeReadPoolParallelReplicas() = default;


Block MergeTreeReadPoolParallelReplicas::getHeader() const
{
    return storage_snapshot->getSampleBlockForColumns(extension.columns_to_read);
}

MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicas::getTask(size_t thread)
{
    /// This parameter is needed only to satisfy the interface
    UNUSED(thread);

    std::lock_guard lock(mutex);

    if (no_more_tasks_available)
        return nullptr;

    if (buffered_ranges.empty())
    {
        auto result = extension.callback(ParallelReadRequest(
            CoordinationMode::Default,
            extension.number_of_current_replica,
            min_marks_for_concurrent_read * threads,
            /// For Default coordination mode we don't need to pass part names.
            RangesInDataPartsDescription{}));

        if (!result || result->finish)
        {
            no_more_tasks_available = true;
            return nullptr;
        }

        buffered_ranges = std::move(result->description);
    }

    if (buffered_ranges.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No tasks to read. This is a bug");

    auto & current_task = buffered_ranges.front();

    RangesInDataPart part;
    size_t part_idx = 0;
    for (size_t index = 0; index < per_part_params.size(); ++index)
    {
        auto & other_part = per_part_params[index];
        if (other_part.data_part.data_part->info == current_task.info)
        {
            part = other_part.data_part;
            part_idx = index;
            break;
        }
    }

    MarkRanges ranges_to_read;
    size_t current_sum_marks = 0;
    while (current_sum_marks < min_marks_for_concurrent_read && !current_task.ranges.empty())
    {
        auto diff = min_marks_for_concurrent_read - current_sum_marks;
        auto range = current_task.ranges.front();
        if (range.getNumberOfMarks() > diff)
        {
            auto new_range = range;
            new_range.end = range.begin + diff;
            range.begin += diff;

            current_task.ranges.front() = range;
            ranges_to_read.push_back(new_range);
            current_sum_marks += new_range.getNumberOfMarks();
            continue;
        }

        ranges_to_read.push_back(range);
        current_sum_marks += range.getNumberOfMarks();
        current_task.ranges.pop_front();
    }

    if (current_task.ranges.empty())
        buffered_ranges.pop_front();

    const auto & per_part = per_part_params[part_idx];

    auto curr_task_size_predictor
        = !per_part.size_predictor ? nullptr : std::make_unique<MergeTreeBlockSizePredictor>(*per_part.size_predictor); /// make a copy

    return std::make_unique<MergeTreeReadTask>(
        part.data_part,
        part.alter_conversions,
        ranges_to_read,
        part.part_index_in_query,
        per_part.column_name_set,
        per_part.task_columns,
        std::move(curr_task_size_predictor));
}


MarkRanges MergeTreeInOrderReadPoolParallelReplicas::getNewTask(RangesInDataPartDescription description)
{
    std::lock_guard lock(mutex);

    auto get_from_buffer = [&]() -> std::optional<MarkRanges>
    {
        for (auto & desc : buffered_tasks)
        {
            if (desc.info == description.info && !desc.ranges.empty())
            {
                auto result = std::move(desc.ranges);
                desc.ranges = MarkRanges{};
                return result;
            }
        }
        return std::nullopt;
    };

    if (auto result = get_from_buffer(); result)
        return result.value();

    if (no_more_tasks)
        return {};

    auto response = extension.callback(ParallelReadRequest(
        mode,
        extension.number_of_current_replica,
        min_marks_for_concurrent_read * request.size(),
        request
    ));

    if (!response || response->description.empty() || response->finish)
    {
        no_more_tasks = true;
        return {};
    }

    /// Fill the buffer
    for (size_t i = 0; i < request.size(); ++i)
    {
        auto & new_ranges = response->description[i].ranges;
        auto & old_ranges = buffered_tasks[i].ranges;
        std::move(new_ranges.begin(), new_ranges.end(), std::back_inserter(old_ranges));
    }

    if (auto result = get_from_buffer(); result)
        return result.value();

    return {};
}

}
