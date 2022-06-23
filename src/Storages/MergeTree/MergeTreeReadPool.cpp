#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Common/formatReadable.h>
#include <common/range.h>


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
MergeTreeReadPool::MergeTreeReadPool(
    const size_t threads_,
    const size_t sum_marks_,
    const size_t min_marks_for_concurrent_read_,
    RangesInDataParts && parts_,
    const MergeTreeData & data_,
    const StorageMetadataPtr & metadata_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const bool check_columns_,
    const Names & column_names_,
    const BackoffSettings & backoff_settings_,
    size_t preferred_block_size_bytes_,
    const bool do_not_steal_tasks_)
    : backoff_settings{backoff_settings_}
    , backoff_state{threads_}
    , data{data_}
    , metadata_snapshot{metadata_snapshot_}
    , column_names{column_names_}
    , do_not_steal_tasks{do_not_steal_tasks_}
    , predict_block_size_bytes{preferred_block_size_bytes_ > 0}
    , prewhere_info{prewhere_info_}
    , parts_ranges{std::move(parts_)}
{
    /// parts don't contain duplicate MergeTreeDataPart's.
    const auto per_part_sum_marks = fillPerPartInfo(parts_ranges, check_columns_);
    fillPerThreadInfo(threads_, sum_marks_, per_part_sum_marks, parts_ranges, min_marks_for_concurrent_read_);
}


MergeTreeReadTaskPtr MergeTreeReadPool::getTask(const size_t min_marks_to_read, const size_t thread, const Names & ordered_names)
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

    auto & part = parts_with_idx[part_idx];
    auto & marks_in_part = thread_tasks.sum_marks_in_parts.back();

    /// Get whole part to read if it is small enough.
    auto need_marks = std::min(marks_in_part, min_marks_to_read);

    /// Do not leave too little rows in part for next time.
    if (marks_in_part > need_marks &&
        marks_in_part - need_marks < min_marks_to_read)
        need_marks = marks_in_part;

    MarkRanges ranges_to_get_from_part;

    /// Get whole part to read if it is small enough.
    if (marks_in_part <= need_marks)
    {
        const auto marks_to_get_from_range = marks_in_part;
        ranges_to_get_from_part = thread_task.ranges;

        marks_in_part -= marks_to_get_from_range;

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

    auto curr_task_size_predictor = !per_part_size_predictor[part_idx] ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(*per_part_size_predictor[part_idx]); /// make a copy

    return std::make_unique<MergeTreeReadTask>(
        part.data_part, ranges_to_get_from_part, part.part_index_in_query, ordered_names,
        per_part_column_name_set[part_idx], per_part_columns[part_idx], per_part_pre_columns[part_idx],
        prewhere_info && prewhere_info->remove_prewhere_column, per_part_should_reorder[part_idx], std::move(curr_task_size_predictor));
}

MarkRanges MergeTreeReadPool::getRestMarks(const IMergeTreeDataPart & part, const MarkRange & from) const
{
    MarkRanges all_part_ranges;

    /// Inefficient in presence of large number of data parts.
    for (const auto & part_ranges : parts_ranges)
    {
        if (part_ranges.data_part.get() == &part)
        {
            all_part_ranges = part_ranges.ranges;
            break;
        }
    }
    if (all_part_ranges.empty())
        throw Exception("Trying to read marks range [" + std::to_string(from.begin) + ", " + std::to_string(from.end) + "] from part '"
            + part.getFullPath() + "' which has no ranges in this query", ErrorCodes::LOGICAL_ERROR);

    auto begin = std::lower_bound(all_part_ranges.begin(), all_part_ranges.end(), from, [] (const auto & f, const auto & s) { return f.begin < s.begin; });
    if (begin == all_part_ranges.end())
        begin = std::prev(all_part_ranges.end());
    begin->begin = from.begin;
    return MarkRanges(begin, all_part_ranges.end());
}

Block MergeTreeReadPool::getHeader() const
{
    return metadata_snapshot->getSampleBlockForColumns(column_names, data.getVirtuals(), data.getStorageID());
}

void MergeTreeReadPool::profileFeedback(const ReadBufferFromFileBase::ProfileInfo info)
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


std::vector<size_t> MergeTreeReadPool::fillPerPartInfo(
    const RangesInDataParts & parts, const bool check_columns)
{
    std::vector<size_t> per_part_sum_marks;
    Block sample_block = metadata_snapshot->getSampleBlock();

    for (const auto i : collections::range(0, parts.size()))
    {
        const auto & part = parts[i];

        /// Read marks for every data part.
        size_t sum_marks = 0;
        for (const auto & range : part.ranges)
            sum_marks += range.end - range.begin;

        per_part_sum_marks.push_back(sum_marks);

        auto [required_columns, required_pre_columns, should_reorder] =
            getReadTaskColumns(data, metadata_snapshot, part.data_part, column_names, prewhere_info, check_columns);

        /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
        const auto & required_column_names = required_columns.getNames();
        per_part_column_name_set.emplace_back(required_column_names.begin(), required_column_names.end());

        per_part_pre_columns.push_back(std::move(required_pre_columns));
        per_part_columns.push_back(std::move(required_columns));
        per_part_should_reorder.push_back(should_reorder);

        parts_with_idx.push_back({ part.data_part, part.part_index_in_query });

        if (predict_block_size_bytes)
        {
            per_part_size_predictor.emplace_back(std::make_unique<MergeTreeBlockSizePredictor>(
                part.data_part, column_names, sample_block));
        }
        else
            per_part_size_predictor.emplace_back(nullptr);
    }

    return per_part_sum_marks;
}


void MergeTreeReadPool::fillPerThreadInfo(
    const size_t threads, const size_t sum_marks, std::vector<size_t> per_part_sum_marks,
    const RangesInDataParts & parts, const size_t min_marks_for_concurrent_read)
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
                parts_per_disk[parts[i].data_part->volume->getDisk()->getName()].push_back(std::move(part_info));
            else
                parts_per_disk[""].push_back(std::move(part_info));
        }

        for (auto & info : parts_per_disk)
            parts_queue.push(std::move(info.second));
    }

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
                        throw Exception("Unexpected end of ranges while spreading marks among threads", ErrorCodes::LOGICAL_ERROR);

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


}
