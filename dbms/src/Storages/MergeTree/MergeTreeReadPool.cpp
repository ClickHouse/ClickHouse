#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <ext/range.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>


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
    const size_t threads_, const size_t sum_marks_, const size_t min_marks_for_concurrent_read_,
    RangesInDataParts parts_, const MergeTreeData & data_, const PrewhereInfoPtr & prewhere_info_,
    const bool check_columns_, const Names & column_names_,
    const BackoffSettings & backoff_settings_, size_t preferred_block_size_bytes_,
    const bool do_not_steal_tasks_)
    : backoff_settings{backoff_settings_}, backoff_state{threads_}, data{data_},
      column_names{column_names_}, do_not_steal_tasks{do_not_steal_tasks_},
      predict_block_size_bytes{preferred_block_size_bytes_ > 0}, prewhere_info{prewhere_info_}, parts_ranges{parts_}
{
    /// reverse from right-to-left to left-to-right
    /// because 'reverse' was done in MergeTreeDataSelectExecutor
    for (auto & part_ranges : parts_ranges)
        std::reverse(std::begin(part_ranges.ranges), std::end(part_ranges.ranges));

    /// parts don't contain duplicate MergeTreeDataPart's.
    const auto per_part_sum_marks = fillPerPartInfo(parts_, check_columns_);
    fillPerThreadInfo(threads_, sum_marks_, per_part_sum_marks, parts_, min_marks_for_concurrent_read_);
}


MergeTreeReadTaskPtr MergeTreeReadPool::getTask(
    const size_t min_marks_to_read, const size_t thread, const Names & ordered_names, const IndicesApplyFunc & apply_function)
{
    const auto & fetch_usable_thread_tasks = [&](size_t & thread_idx, ThreadTask *& thread_tasks)
    {
        const std::lock_guard lock{mutex};

        /// If number of threads was lowered due to backoff, then will assign work only for maximum 'backoff_state.current_threads' threads.
        if (thread >= backoff_state.current_threads)
            return false;

        if (remaining_thread_tasks.empty())
            return false;

        const auto tasks_remaining_for_this_thread = !threads_tasks[thread].sum_marks_in_parts.empty();
        if (!tasks_remaining_for_this_thread && do_not_steal_tasks)
            return false;

        /// Steal task if nothing to do and it's not prohibited
        thread_idx = tasks_remaining_for_this_thread ? thread : *std::begin(remaining_thread_tasks);
        thread_tasks = &threads_tasks[thread_idx];
        return true;
    };

    const auto & fetch_usable_thread_task = [&](ThreadTask * usable_thread_tasks) -> std::tuple<size_t, ThreadTask::PartIndexAndRange *>
    {
        auto & thread_task = usable_thread_tasks->parts_and_ranges.back();
        auto & marks_in_part = usable_thread_tasks->sum_marks_in_parts.back();

        if (!thread_task.is_applied_indices)
        {
            thread_task.is_applied_indices = true;
            thread_task.ranges = apply_function(parts_with_idx[thread_task.part_idx].data_part, thread_task.ranges);

            if (thread_task.ranges.empty())
            {
                usable_thread_tasks->parts_and_ranges.pop_back();
                usable_thread_tasks->sum_marks_in_parts.pop_back();
                return std::tuple(0, nullptr);
            }
            else
            {
                marks_in_part = 0;
                for (size_t index = 0; index < thread_task.ranges.size(); ++index)
                    marks_in_part+= thread_task.ranges[index].end - thread_task.ranges[index].begin - 1;
            }
        }

        return std::tuple(thread_task.part_idx, &thread_task);
    };

    size_t thread_idx = 0, part_idx = 0;
    ThreadTask * usable_thread_tasks = nullptr;
    ThreadTask::PartIndexAndRange * usable_thread_task = nullptr;

    while (!usable_thread_task && fetch_usable_thread_tasks(thread_idx, usable_thread_tasks))
    {
        const std::lock_guard thread_lock{*usable_thread_tasks->thread_task_mutex};

        while (!usable_thread_tasks->parts_and_ranges.empty() && !usable_thread_task)
            std::tie(part_idx, usable_thread_task) = fetch_usable_thread_task(usable_thread_tasks);

        const std::lock_guard lock{mutex};
        if (!usable_thread_task && usable_thread_tasks->parts_and_ranges.empty())
            remaining_thread_tasks.erase(thread_idx);
        else
        {
            auto & part = parts_with_idx[part_idx];
            auto & marks_in_part = usable_thread_tasks->sum_marks_in_parts.back();

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

                /** Ranges are in right-to-left order, because 'reverse' was done in MergeTreeDataSelectExecutor
                    *     and that order is supported in 'fillPerThreadInfo'.
                    */
                ranges_to_get_from_part = usable_thread_task->ranges;

                marks_in_part -= marks_to_get_from_range;

                usable_thread_tasks->parts_and_ranges.pop_back();
                usable_thread_tasks->sum_marks_in_parts.pop_back();

                if (usable_thread_tasks->sum_marks_in_parts.empty())
                    remaining_thread_tasks.erase(thread_idx);
            }
            else
            {
                /// Loop through part ranges.
                while (need_marks > 0 && !usable_thread_task->ranges.empty())
                {
                    auto & range = usable_thread_task->ranges.back();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    if (range.begin == range.end)
                    {
                        std::swap(range, usable_thread_task->ranges.back());
                        usable_thread_task->ranges.pop_back();
                    }

                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                }

                /** Change order to right-to-left, for MergeTreeThreadSelectBlockInputStream to get ranges with .pop_back()
                    *  (order was changed to left-to-right due to .pop_back() above).
                    */
                std::reverse(std::begin(ranges_to_get_from_part), std::end(ranges_to_get_from_part));
            }

            auto curr_task_size_predictor = !per_part_size_predictor[part_idx] ? nullptr
                : std::make_unique<MergeTreeBlockSizePredictor>(*per_part_size_predictor[part_idx]); /// make a copy

            return std::make_unique<MergeTreeReadTask>(
                part.data_part, ranges_to_get_from_part, part.part_index_in_query, ordered_names,
                per_part_column_name_set[part_idx], per_part_columns[part_idx], per_part_pre_columns[part_idx],
                prewhere_info && prewhere_info->remove_prewhere_column, per_part_should_reorder[part_idx], std::move(curr_task_size_predictor));
        }
    }

    return nullptr;
}

MarkRanges MergeTreeReadPool::getRestMarks(const MergeTreeDataPart & part, const MarkRange & from) const
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
    return data.getSampleBlockForColumns(column_names);
}

void MergeTreeReadPool::profileFeedback(const ReadBufferFromFileBase::ProfileInfo info)
{
    if (backoff_settings.min_read_latency_ms == 0 || do_not_steal_tasks)
        return;

    if (info.nanoseconds < backoff_settings.min_read_latency_ms * 1000000)
        return;

    std::lock_guard lock(mutex);

    if (backoff_state.current_threads <= 1)
        return;

    size_t throughput = info.bytes_read * 1000000000 / info.nanoseconds;

    if (throughput >= backoff_settings.max_throughput)
        return;

    if (backoff_state.time_since_prev_event.elapsed() < backoff_settings.min_interval_between_events_ms * 1000000)
        return;

    backoff_state.time_since_prev_event.restart();
    ++backoff_state.num_events;

    ProfileEvents::increment(ProfileEvents::SlowRead);
    LOG_DEBUG(log, std::fixed << std::setprecision(3)
        << "Slow read, event №" << backoff_state.num_events
        << ": read " << info.bytes_read << " bytes in " << info.nanoseconds / 1000000000.0 << " sec., "
        << info.bytes_read * 1000.0 / info.nanoseconds << " MB/s.");

    if (backoff_state.num_events < backoff_settings.min_events)
        return;

    backoff_state.num_events = 0;
    --backoff_state.current_threads;

    ProfileEvents::increment(ProfileEvents::ReadBackoff);
    LOG_DEBUG(log, "Will lower number of threads to " << backoff_state.current_threads);
}


std::vector<size_t> MergeTreeReadPool::fillPerPartInfo(
    RangesInDataParts & parts, const bool check_columns)
{
    std::vector<size_t> per_part_sum_marks;
    Block sample_block = data.getSampleBlock();

    for (const auto i : ext::range(0, parts.size()))
    {
        auto & part = parts[i];

        /// Read marks for every data part.
        size_t sum_marks = 0;
        /// Ranges are in right-to-left order, due to 'reverse' in MergeTreeDataSelectExecutor.
        for (const auto & range : part.ranges)
            sum_marks += range.end - range.begin;

        per_part_sum_marks.push_back(sum_marks);

        per_part_columns_lock.emplace_back(part.data_part->columns_lock);

        auto [required_columns, required_pre_columns, should_reorder] =
            getReadTaskColumns(data, part.data_part, column_names, prewhere_info, check_columns);

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
    RangesInDataParts & parts, const size_t min_marks_for_concurrent_read)
{
    threads_tasks.resize(threads);

    const size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;

    for (size_t i = 0; i < threads && !parts.empty(); ++i)
    {
        auto need_marks = min_marks_per_thread;

        while (need_marks > 0 && !parts.empty())
        {
            const auto part_idx = parts.size() - 1;
            RangesInDataPart & part = parts.back();
            size_t & marks_in_part = per_part_sum_marks.back();

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
                /// Leave ranges in right-to-left order for convenience to use .pop_back() in .getTask()
                ranges_to_get_from_part = part.ranges;
                marks_in_ranges = marks_in_part;

                need_marks -= marks_in_part;
                parts.pop_back();
                per_part_sum_marks.pop_back();
            }
            else
            {
                /// Loop through part ranges.
                while (need_marks > 0)
                {
                    if (part.ranges.empty())
                        throw Exception("Unexpected end of ranges while spreading marks among threads", ErrorCodes::LOGICAL_ERROR);

                    MarkRange & range = part.ranges.back();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        part.ranges.pop_back();
                }

                /** Change order to right-to-left, for getTask() to get ranges with .pop_back()
                    *  (order was changed to left-to-right due to .pop_back() above).
                    */
                std::reverse(std::begin(ranges_to_get_from_part), std::end(ranges_to_get_from_part));
            }

            threads_tasks[i].parts_and_ranges.push_back({ part_idx, ranges_to_get_from_part });
            threads_tasks[i].sum_marks_in_parts.push_back(marks_in_ranges);
            if (marks_in_ranges != 0)
                remaining_thread_tasks.insert(i);
        }
    }
}


}
