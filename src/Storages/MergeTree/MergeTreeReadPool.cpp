#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <base/range.h>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/RequestResponse.h>


namespace ProfileEvents
{
    extern const Event SlowRead;
    extern const Event ReadBackoff;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 read_backoff_max_throughput;
    extern const SettingsUInt64 read_backoff_min_concurrency;
    extern const SettingsMilliseconds read_backoff_min_interval_between_events_ms;
    extern const SettingsUInt64 read_backoff_min_events;
    extern const SettingsMilliseconds read_backoff_min_latency_ms;
}

namespace ErrorCodes
{
extern const int CANNOT_SCHEDULE_TASK;
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
}

MergeTreeReadPool::MergeTreeReadPool(
    RangesInDataParts && parts_,
    MutationsSnapshotPtr mutations_snapshot_,
    VirtualFields shared_virtual_fields_,
    const IndexReadTasks & index_read_tasks_,
    const StorageSnapshotPtr & storage_snapshot_,
    const FilterDAGInfoPtr & row_level_filter_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & settings_,
    const MergeTreeReadTask::BlockSizeParams & params_,
    const ContextPtr & context_,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : MergeTreeReadPoolBase(
          std::move(parts_),
          std::move(mutations_snapshot_),
          std::move(shared_virtual_fields_),
          index_read_tasks_,
          storage_snapshot_,
          row_level_filter_,
          prewhere_info_,
          actions_settings_,
          reader_settings_,
          column_names_,
          settings_,
          params_,
          context_)
    , updater(std::move(updater_))
    , backoff_settings{context_->getSettingsRef()}
    , backoff_state{pool_settings.threads}
{
    fillPerThreadInfo(pool_settings.threads, pool_settings.sum_marks);
}

MergeTreeReadTaskPtr MergeTreeReadPool::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    while (true)
    {
        size_t part_idx = 0;
        size_t thread_idx = 0;
        size_t need_marks = 0;
        MarkRanges cut_ranges;

        if (!cutRangesToRead(task_idx, part_idx, thread_idx, need_marks, cut_ranges))
            return nullptr;

        MarkRanges task_ranges;
        if (!ranges_refiner)
        {
            task_ranges = std::move(cut_ranges);
        }
        else
        {
            /// Marks dropped by the refiner do not count towards the task size: keep cutting
            /// more marks from the same part until the task reaches its intended size in
            /// surviving marks or the part has nothing more to cut. Refinement may block
            /// (e.g. building a projection index bitmap on the first use for the part),
            /// so it always happens outside of the mutex.
            while (true)
            {
                auto refined = refineReadRanges(*per_part_infos[part_idx], std::move(cut_ranges));
                for (const auto & range : refined)
                {
                    if (!task_ranges.empty() && task_ranges.back().end == range.begin)
                        task_ranges.back().end = range.end;
                    else
                        task_ranges.push_back(range);
                }

                size_t marks_collected = task_ranges.getNumberOfMarks();
                if (marks_collected >= need_marks)
                    break;

                MarkRanges more_ranges;
                if (!cutMoreRangesToRead(thread_idx, part_idx, need_marks - marks_collected, more_ranges))
                    break;

                cut_ranges = std::move(more_ranges);
            }
        }

        /// Everything was refined away, take the next portion (possibly of another part).
        if (task_ranges.empty())
            continue;

        /// createTask() is costly and not needed guarded by mutex.
        return createTask(per_part_infos[part_idx], std::move(task_ranges), previous_task, updater);
    }
}

bool MergeTreeReadPool::cutRangesToRead(size_t task_idx, size_t & part_idx, size_t & thread_idx, size_t & need_marks, MarkRanges & ranges_to_get_from_part)
{
    const std::lock_guard lock{mutex};

    /// If number of threads was lowered due to backoff, then will assign work only for maximum 'backoff_state.current_threads' threads.
    if (task_idx >= backoff_state.current_threads)
        return false;

    if (remaining_thread_tasks.empty())
        return false;

    const auto tasks_remaining_for_this_thread = !threads_tasks[task_idx].sum_marks_in_parts.empty();
    if (!tasks_remaining_for_this_thread && pool_settings.do_not_steal_tasks)
        return false;

    /// Steal task if nothing to do and it's not prohibited
    thread_idx = task_idx;
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

    part_idx = thread_tasks.parts_and_ranges.back().part_idx;
    const auto marks_in_part = thread_tasks.sum_marks_in_parts.back();
    const auto min_marks_per_task = per_part_infos[part_idx]->min_marks_per_task;

    if (is_part_on_remote_disk[part_idx] && !pool_settings.use_const_size_tasks_for_remote_reading)
        need_marks = marks_in_part;
    else /// Get whole part to read if it is small enough.
        need_marks = std::min(marks_in_part, min_marks_per_task);

    cutFromThreadTask(thread_tasks, thread_idx, need_marks, ranges_to_get_from_part);
    return true;
}

bool MergeTreeReadPool::cutMoreRangesToRead(size_t thread_idx, size_t part_idx, size_t need_marks, MarkRanges & ranges_to_get_from_part)
{
    const std::lock_guard lock{mutex};

    /// Continue only while the same part is on top of the same thread's queue: a read task
    /// must not span multiple parts. The queue may have been consumed or stolen concurrently.
    auto & thread_tasks = threads_tasks[thread_idx];
    if (thread_tasks.parts_and_ranges.empty() || thread_tasks.parts_and_ranges.back().part_idx != part_idx)
        return false;

    cutFromThreadTask(thread_tasks, thread_idx, need_marks, ranges_to_get_from_part);
    return true;
}

void MergeTreeReadPool::cutFromThreadTask(ThreadTask & thread_tasks, size_t thread_idx, size_t need_marks, MarkRanges & ranges_to_get_from_part)
{
    auto & thread_task = thread_tasks.parts_and_ranges.back();
    auto & marks_in_part = thread_tasks.sum_marks_in_parts.back();
    const auto min_marks_per_task = per_part_infos[thread_task.part_idx]->min_marks_per_task;

    /// Do not leave too little rows in part for next time.
    if (marks_in_part > need_marks && marks_in_part - need_marks < min_marks_per_task / 2)
        need_marks = marks_in_part;

    /// Get whole part to read if it is small enough.
    if (marks_in_part <= need_marks)
    {
        ranges_to_get_from_part = std::move(thread_task.ranges);
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
    LOG_DEBUG(log, "Slow read, event №{}: read {} bytes in {:.3f} sec., {}/s.",
        backoff_state.num_events, info.bytes_read, static_cast<double>(info.nanoseconds) / 1e9,
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
    if (threads > 1000000ull)
        throw Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Too many threads ({}) requested", threads);

    std::lock_guard lock(mutex);

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
            parts_per_disk[parts_ranges[i].data_part->getDataPartStorage().getDiskName()].push_back(std::move(part_info));
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
            auto & part_with_ranges = current_parts.back().part;
            size_t & marks_in_part = current_parts.back().sum_marks;
            const auto part_idx = current_parts.back().part_idx;
            const auto min_marks_per_task = per_part_infos[part_idx]->min_marks_per_task;

            if (min_marks_per_task == 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Chosen number of marks to read is zero (likely because of weird interference of settings)");

            /// Do not get too few rows from part.
            if (marks_in_part >= min_marks_per_task && need_marks < min_marks_per_task)
                need_marks = min_marks_per_task;

            /// Do not leave too few rows in part for next time.
            if (marks_in_part > need_marks && marks_in_part - need_marks < min_marks_per_task)
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

MergeTreeReadPool::BackoffSettings::BackoffSettings(const DB::Settings & settings)
    : min_read_latency_ms(settings[Setting::read_backoff_min_latency_ms].totalMilliseconds())
    , max_throughput(settings[Setting::read_backoff_max_throughput])
    , min_interval_between_events_ms(settings[Setting::read_backoff_min_interval_between_events_ms].totalMilliseconds())
    , min_events(settings[Setting::read_backoff_min_events])
    , min_concurrency(settings[Setting::read_backoff_min_concurrency])
{}

}
