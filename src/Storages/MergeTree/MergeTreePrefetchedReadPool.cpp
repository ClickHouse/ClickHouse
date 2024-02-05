#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreePrefetchedReadPool.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <base/getThreadId.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>


namespace ProfileEvents
{
    extern const Event MergeTreePrefetchedReadPoolInit;
    extern const Event WaitPrefetchTaskMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace FailPoints
{
    extern const char prefetched_reader_pool_failpoint[];
}

bool MergeTreePrefetchedReadPool::TaskHolder::operator<(const TaskHolder & other) const
{
    chassert(task->priority >= 0);
    chassert(other.task->priority >= 0);
    /// With default std::priority_queue, top() returns largest element.
    /// So closest to 0 will be on top with this comparator.
    return task->priority > other.task->priority; /// Less is better.
}


MergeTreePrefetchedReadPool::PrefetchedReaders::~PrefetchedReaders()
{
    for (auto & prefetch_future : prefetch_futures)
        if (prefetch_future.valid())
            prefetch_future.wait();
}

MergeTreePrefetchedReadPool::PrefetchedReaders::PrefetchedReaders(
    MergeTreeReadTask::Readers readers_,
    Priority priority_,
    MergeTreePrefetchedReadPool & pool_)
    : is_valid(true)
    , readers(std::move(readers_))
{
    try
    {
        prefetch_futures.reserve(1 + readers.prewhere.size());

        prefetch_futures.push_back(pool_.createPrefetchedFuture(readers.main.get(), priority_));

        for (const auto & reader : readers.prewhere)
            prefetch_futures.push_back(pool_.createPrefetchedFuture(reader.get(), priority_));

        fiu_do_on(FailPoints::prefetched_reader_pool_failpoint,
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failpoint for prefetched reader enabled");
        });
    }
    catch (...) /// in case of memory exceptions we have to wait
    {
        for (auto & prefetch_future : prefetch_futures)
            if (prefetch_future.valid())
                prefetch_future.wait();

        throw;
    }
}

void MergeTreePrefetchedReadPool::PrefetchedReaders::wait()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WaitPrefetchTaskMicroseconds);
    for (auto & prefetch_future : prefetch_futures)
        prefetch_future.wait();
}

MergeTreeReadTask::Readers MergeTreePrefetchedReadPool::PrefetchedReaders::get()
{
    SCOPE_EXIT({ is_valid = false; });
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::WaitPrefetchTaskMicroseconds);

    /// First wait for completion of all futures.
    for (auto & prefetch_future : prefetch_futures)
        prefetch_future.wait();

    /// Then rethrow first exception if any.
    for (auto & prefetch_future : prefetch_futures)
        prefetch_future.get();

    return std::move(readers);
}

MergeTreePrefetchedReadPool::MergeTreePrefetchedReadPool(
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
    , WithContext(context_)
    , prefetch_threadpool(getContext()->getPrefetchThreadpool())
    , log(getLogger("MergeTreePrefetchedReadPool(" + (parts_ranges.empty() ? "" : parts_ranges.front().data_part->storage.getStorageID().getNameForLogs()) + ")"))
{
    /// Tasks creation might also create a lost of readers - check they do not
    /// do any time consuming operations in ctor.
    ProfileEventTimeIncrement<Milliseconds> watch(ProfileEvents::MergeTreePrefetchedReadPoolInit);

    fillPerPartStatistics();
    fillPerThreadTasks(pool_settings.threads, pool_settings.sum_marks);
}

std::future<void> MergeTreePrefetchedReadPool::createPrefetchedFuture(IMergeTreeReader * reader, Priority priority)
{
    /// In order to make a prefetch we need to wait for marks to be loaded. But we just created
    /// a reader (which starts loading marks in its constructor), then if we do prefetch right
    /// after creating a reader, it will be very inefficient. We can do prefetch for all parts
    /// only inside this MergeTreePrefetchedReadPool, where read tasks are created and distributed,
    /// and we cannot block either, therefore make prefetch inside the pool and put the future
    /// into the thread task. When a thread calls getTask(), it will wait for it is not ready yet.
    auto task = [=, context = getContext()]() mutable
    {
        /// For async read metrics in system.query_log.
        PrefetchIncrement watch(context->getAsyncReadCounters());
        reader->prefetchBeginOfRange(priority);
    };

    return scheduleFromThreadPool<void>(std::move(task), prefetch_threadpool, "ReadPrepare", priority);
}

void MergeTreePrefetchedReadPool::createPrefetchedReadersForTask(ThreadTask & task)
{
    if (task.isValidReadersFuture())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task already has a reader");

    auto extras = getExtras();
    auto readers = MergeTreeReadTask::createReaders(task.read_info, extras, task.ranges);
    task.readers_future = std::make_unique<PrefetchedReaders>(std::move(readers), task.priority, *this);
}

void MergeTreePrefetchedReadPool::startPrefetches()
{
    if (prefetch_queue.empty())
        return;

    [[maybe_unused]] TaskHolder prev;
    [[maybe_unused]] const Priority highest_priority{reader_settings.read_settings.priority.value + 1};
    assert(prefetch_queue.top().task->priority == highest_priority);

    while (!prefetch_queue.empty())
    {
        const auto & top = prefetch_queue.top();
        createPrefetchedReadersForTask(*top.task);
#ifndef NDEBUG
        if (prev.task)
        {
            assert(top.task->priority >= highest_priority);
            if (prev.thread_id == top.thread_id)
                assert(prev.task->priority < top.task->priority);
        }
        prev = top;
#endif
        prefetch_queue.pop();
    }
}

MergeTreeReadTaskPtr MergeTreePrefetchedReadPool::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    std::lock_guard lock(mutex);

    if (per_thread_tasks.empty())
        return nullptr;

    if (!started_prefetches)
    {
        started_prefetches = true;
        startPrefetches();
    }

    auto it = per_thread_tasks.find(task_idx);
    if (it == per_thread_tasks.end())
        return stealTask(task_idx, previous_task);

    auto & thread_tasks = it->second;
    assert(!thread_tasks.empty());

    auto thread_task = std::move(thread_tasks.front());
    thread_tasks.pop_front();

    if (thread_tasks.empty())
        per_thread_tasks.erase(it);

    return createTask(*thread_task, previous_task);
}

MergeTreeReadTaskPtr MergeTreePrefetchedReadPool::stealTask(size_t thread, MergeTreeReadTask * previous_task)
{
    auto non_prefetched_tasks_to_steal = per_thread_tasks.end();
    auto prefetched_tasks_to_steal = per_thread_tasks.end();
    int64_t best_prefetched_task_priority = -1;

    /// There is no point stealing in order (like in MergeTreeReadPool, where tasks can be stolen
    /// only from the next thread). Even if we steal task from the next thread, which reads from
    /// the same part as we just read, it might seem that we can reuse our own reader, do some
    /// seek avoiding and it will have a good result as we avoided seek (new request). But it is
    /// not so, because this next task will most likely have its own reader a prefetch already on
    /// the fly. (Not to mention that in fact we cannot reuse our own reader if initially we did
    /// not accounted this range into range request to object storage).
    for (auto thread_tasks_it = per_thread_tasks.begin(); thread_tasks_it != per_thread_tasks.end(); ++thread_tasks_it)
    {
        /// Prefer to steal tasks which have an initialized reader (with prefetched data). Thus we avoid
        /// losing a prefetch by creating our own reader (or resusing our own reader if the part
        /// is the same as last read by this thread).
        auto & thread_tasks = thread_tasks_it->second;

        auto task_it = std::find_if(
            thread_tasks.begin(), thread_tasks.end(),
            [](const auto & task) { return task->isValidReadersFuture(); });

        if (task_it == thread_tasks.end())
        {
            /// The follow back to non-prefetched task should lie on the thread which
            /// has more tasks than others.
            if (non_prefetched_tasks_to_steal == per_thread_tasks.end()
                || non_prefetched_tasks_to_steal->second.size() < thread_tasks.size())
                non_prefetched_tasks_to_steal = thread_tasks_it;
        }
        /// Try to steal task with the best (lowest) priority (because it will be executed faster).
        else if (prefetched_tasks_to_steal == per_thread_tasks.end()
            || (*task_it)->priority < best_prefetched_task_priority)
        {
            best_prefetched_task_priority = (*task_it)->priority;
            chassert(best_prefetched_task_priority >= 0);
            prefetched_tasks_to_steal = thread_tasks_it;
        }
    }

    if (prefetched_tasks_to_steal != per_thread_tasks.end())
    {
        auto & thread_tasks = prefetched_tasks_to_steal->second;
        assert(!thread_tasks.empty());

        auto task_it = std::find_if(
            thread_tasks.begin(), thread_tasks.end(),
            [](const auto & task) { return task->isValidReadersFuture(); });

        assert(task_it != thread_tasks.end());
        auto thread_task = std::move(*task_it);
        thread_tasks.erase(task_it);

        if (thread_tasks.empty())
            per_thread_tasks.erase(prefetched_tasks_to_steal);

        return createTask(*thread_task, previous_task);
    }

    /// TODO: it also makes sense to first try to steal from the next thread if it has ranges
    /// from the same part as current thread last read - to reuse the reader.
    if (non_prefetched_tasks_to_steal != per_thread_tasks.end())
    {
        auto & thread_tasks = non_prefetched_tasks_to_steal->second;
        assert(!thread_tasks.empty());

        /// Get second half of the tasks.
        const size_t total_tasks = thread_tasks.size();
        const size_t half = total_tasks / 2;
        auto half_it = thread_tasks.begin() + half;
        assert(half_it != thread_tasks.end());

        /// Give them to current thread, as current thread's tasks list is empty.
        auto & current_thread_tasks = per_thread_tasks[thread];
        current_thread_tasks.insert(
            current_thread_tasks.end(), make_move_iterator(half_it), make_move_iterator(thread_tasks.end()));

        /// Erase them from the thread from which we steal.
        thread_tasks.resize(half);
        if (thread_tasks.empty())
            per_thread_tasks.erase(non_prefetched_tasks_to_steal);

        auto thread_task = std::move(current_thread_tasks.front());
        current_thread_tasks.erase(current_thread_tasks.begin());
        if (current_thread_tasks.empty())
            per_thread_tasks.erase(thread);

        return createTask(*thread_task, previous_task);
    }

    return nullptr;
}

MergeTreeReadTaskPtr MergeTreePrefetchedReadPool::createTask(ThreadTask & task, MergeTreeReadTask * previous_task)
{
    if (task.isValidReadersFuture())
    {
        auto size_predictor = task.read_info->shared_size_predictor
            ? std::make_unique<MergeTreeBlockSizePredictor>(*task.read_info->shared_size_predictor)
            : nullptr;

        return std::make_unique<MergeTreeReadTask>(task.read_info, task.readers_future->get(), task.ranges, std::move(size_predictor));
    }

    return MergeTreeReadPoolBase::createTask(task.read_info, task.ranges, previous_task);
}

size_t getApproximateSizeOfGranule(const IMergeTreeDataPart & part, const Names & columns_to_read)
{
    ColumnSize columns_size{};
    for (const auto & col_name : columns_to_read)
        columns_size.add(part.getColumnSize(col_name));
    return columns_size.data_compressed / part.getMarksCount();
}

void MergeTreePrefetchedReadPool::fillPerPartStatistics()
{
    per_part_statistics.clear();
    per_part_statistics.reserve(parts_ranges.size());
    const auto & settings = getContext()->getSettingsRef();

    for (size_t i = 0; i < parts_ranges.size(); ++i)
    {
        auto & part_stat = per_part_statistics.emplace_back();
        const auto & read_info = *per_part_infos[i];

        /// Sum up total size of all mark ranges in a data part.
        for (const auto & range : parts_ranges[i].ranges)
            part_stat.sum_marks += range.end - range.begin;

        const auto & columns = settings.merge_tree_determine_task_size_by_prewhere_columns && prewhere_info
            ? prewhere_info->prewhere_actions->getRequiredColumnsNames()
            : column_names;

        part_stat.approx_size_of_mark = getApproximateSizeOfGranule(*read_info.data_part, columns);

        auto update_stat_for_column = [&](const auto & column_name)
        {
            size_t column_size = read_info.data_part->getColumnSize(column_name).data_compressed;
            part_stat.estimated_memory_usage_for_single_prefetch += std::min<size_t>(column_size, settings.prefetch_buffer_size);
            ++part_stat.required_readers_num;
        };

        /// adjustBufferSize(), which is done in MergeTreeReaderStream and MergeTreeReaderCompact,
        /// lowers buffer size if file size (or required read range) is less. So we know that the
        /// settings.prefetch_buffer_size will be lowered there, therefore we account it here as well.
        /// But here we make a more approximate lowering (because we do not have loaded marks yet),
        /// while in adjustBufferSize it will be presize.
        for (const auto & column : read_info.task_columns.columns)
            update_stat_for_column(column.name);

        if (reader_settings.apply_deleted_mask && read_info.data_part->hasLightweightDelete())
            update_stat_for_column(LightweightDeleteDescription::FILTER_COLUMN.name);

        for (const auto & pre_columns : read_info.task_columns.pre_columns)
            for (const auto & column : pre_columns)
                update_stat_for_column(column.name);
    }
}

void MergeTreePrefetchedReadPool::fillPerThreadTasks(size_t threads, size_t sum_marks)
{
    if (per_part_infos.empty())
        return;

    const auto & context = getContext();
    const auto & settings = context->getSettingsRef();

    size_t total_size_approx = 0;
    for (const auto & part : per_part_statistics)
        total_size_approx += part.sum_marks * part.approx_size_of_mark;

    size_t min_prefetch_step_marks = pool_settings.min_marks_for_concurrent_read;
    for (size_t i = 0; i < per_part_infos.size(); ++i)
    {
        auto & part_stat = per_part_statistics[i];

        if (settings.filesystem_prefetch_step_marks)
        {
            part_stat.prefetch_step_marks = settings.filesystem_prefetch_step_marks;
        }
        else if (settings.filesystem_prefetch_step_bytes && part_stat.approx_size_of_mark)
        {
            part_stat.prefetch_step_marks = std::max<size_t>(
                1, static_cast<size_t>(std::round(static_cast<double>(settings.filesystem_prefetch_step_bytes) / part_stat.approx_size_of_mark)));
        }

        /// This limit is important to avoid spikes of slow aws getObject requests when parallelizing within one file.
        /// (The default is taken from here https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html).
        if (part_stat.approx_size_of_mark
            && settings.filesystem_prefetch_min_bytes_for_single_read_task
            && part_stat.approx_size_of_mark < settings.filesystem_prefetch_min_bytes_for_single_read_task)
        {
            const size_t min_prefetch_step_marks_by_total_cols = static_cast<size_t>(
                std::ceil(static_cast<double>(settings.filesystem_prefetch_min_bytes_for_single_read_task) / part_stat.approx_size_of_mark));

            /// At least one task to start working on it right now and another one to prefetch in the meantime.
            const size_t new_min_prefetch_step_marks = std::min<size_t>(min_prefetch_step_marks_by_total_cols, sum_marks / threads / 2);
            if (min_prefetch_step_marks < new_min_prefetch_step_marks)
            {
                LOG_DEBUG(log, "Increasing min prefetch step from {} to {}", min_prefetch_step_marks, new_min_prefetch_step_marks);
                min_prefetch_step_marks = new_min_prefetch_step_marks;
            }
        }

        if (part_stat.prefetch_step_marks < min_prefetch_step_marks)
        {
            LOG_DEBUG(log, "Increasing prefetch step from {} to {}", part_stat.prefetch_step_marks, min_prefetch_step_marks);
            part_stat.prefetch_step_marks = min_prefetch_step_marks;
        }

        LOG_DEBUG(
            log,
            "Part: {}, sum_marks: {}, approx mark size: {}, prefetch_step_bytes: {}, prefetch_step_marks: {}, (ranges: {})",
            parts_ranges[i].data_part->name,
            part_stat.sum_marks,
            part_stat.approx_size_of_mark,
            settings.filesystem_prefetch_step_bytes,
            part_stat.prefetch_step_marks,
            toString(parts_ranges[i].ranges));
    }

    const size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;

    LOG_DEBUG(
        log,
        "Sum marks: {}, threads: {}, min_marks_per_thread: {}, min prefetch step marks: {}, prefetches limit: {}, total_size_approx: {}",
        sum_marks,
        threads,
        min_marks_per_thread,
        min_prefetch_step_marks,
        settings.filesystem_prefetches_limit,
        total_size_approx);

    size_t allowed_memory_usage = settings.filesystem_prefetch_max_memory_usage;
    if (!allowed_memory_usage)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `filesystem_prefetch_max_memory_usage` must be non-zero");

    std::optional<size_t> allowed_prefetches_num = settings.filesystem_prefetches_limit
        ? std::optional<size_t>(settings.filesystem_prefetches_limit)
        : std::nullopt;

    per_thread_tasks.clear();
    size_t total_tasks = 0;

    /// Make a copy to modify ranges.
    std::vector<MarkRanges> per_part_ranges;
    per_part_ranges.reserve(parts_ranges.size());
    for (const auto & part_with_ranges : parts_ranges)
    {
        auto & part_ranges = per_part_ranges.emplace_back(part_with_ranges.ranges);
        std::sort(part_ranges.begin(), part_ranges.end());
    }

    for (size_t i = 0, part_idx = 0; i < threads && part_idx < per_part_infos.size(); ++i)
    {
        int64_t need_marks = min_marks_per_thread;

        /// Priority is given according to the prefetch number for each thread,
        /// e.g. the first task of each thread has the same priority and is greater
        /// than the second task of each thread, and so on.
        /// Add 1 to query read priority because higher priority should be given to
        /// reads from pool which are from reader.
        Priority priority{reader_settings.read_settings.priority.value + 1};

        while (need_marks > 0 && part_idx < per_part_infos.size())
        {
            auto & part_stat = per_part_statistics[part_idx];
            auto & part_ranges = per_part_ranges[part_idx];

            if (part_stat.sum_marks == 0)
            {
                ++part_idx;
                continue;
            }

            MarkRanges ranges_to_get_from_part;
            size_t marks_to_get_from_part = std::min<size_t>(need_marks, part_stat.sum_marks);

            /// Split by prefetch step even if !allow_prefetch below. Because it will allow
            /// to make a better distribution of tasks which did not fill into memory limit
            /// or prefetches limit through tasks stealing.
            if (part_stat.prefetch_step_marks)
            {
                marks_to_get_from_part = std::min<size_t>(marks_to_get_from_part, part_stat.prefetch_step_marks);
            }

            if (part_stat.sum_marks == marks_to_get_from_part)
            {
                ranges_to_get_from_part = part_ranges;
            }
            else
            {
                if (part_stat.sum_marks < marks_to_get_from_part)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Requested {} marks from part {}, but part has only {} marks",
                        marks_to_get_from_part, per_part_infos[part_idx]->data_part->name, part_stat.sum_marks);
                }

                size_t num_marks_to_get = marks_to_get_from_part;
                while (num_marks_to_get > 0)
                {
                    MarkRange & range = part_ranges.front();
                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, num_marks_to_get);
                    num_marks_to_get -= marks_to_get_from_range;

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;

                    if (range.begin == range.end)
                    {
                        part_ranges.pop_front();
                    }
                    else if (!num_marks_to_get && part_stat.prefetch_step_marks && range.end - range.begin < part_stat.prefetch_step_marks)
                    {
                        /// We already have `num_marks_to_get` marks, but current mark range has
                        /// less than `prefetch_step_marks` marks, then add them too.
                        ranges_to_get_from_part.emplace_back(range.begin, range.end);
                        marks_to_get_from_part += range.end - range.begin;
                        part_ranges.pop_front();
                    }
                }
            }

            need_marks -= marks_to_get_from_part;
            sum_marks -= marks_to_get_from_part;
            part_stat.sum_marks -= marks_to_get_from_part;

            bool allow_prefetch = false;
            if (allowed_memory_usage
                && (!allowed_prefetches_num.has_value() || allowed_prefetches_num.value() > 0))
            {
                allow_prefetch = part_stat.estimated_memory_usage_for_single_prefetch <= allowed_memory_usage
                    && (!allowed_prefetches_num.has_value() || part_stat.required_readers_num <= allowed_prefetches_num.value());

                if (allow_prefetch)
                {
                    allowed_memory_usage -= part_stat.estimated_memory_usage_for_single_prefetch;
                    if (allowed_prefetches_num.has_value())
                        *allowed_prefetches_num -= part_stat.required_readers_num;
                }
            }

            auto thread_task = std::make_unique<ThreadTask>(per_part_infos[part_idx], ranges_to_get_from_part, priority);
            if (allow_prefetch)
                prefetch_queue.emplace(TaskHolder{thread_task.get(), i});

            per_thread_tasks[i].push_back(std::move(thread_task));

            ++priority.value;
            ++total_tasks;
        }
    }

    LOG_TEST(log, "Result tasks {} for {} threads: {}", total_tasks, threads, dumpTasks(per_thread_tasks));
}

std::string MergeTreePrefetchedReadPool::dumpTasks(const TasksPerThread & tasks)
{
    WriteBufferFromOwnString result;
    for (const auto & [thread_id, thread_tasks] : tasks)
    {
        result << "\tthread id: " << toString(thread_id) << ", tasks: " << toString(thread_tasks.size());
        if (!thread_tasks.empty())
        {
            size_t no = 0;
            for (const auto & task : thread_tasks)
            {
                result << '\t';
                result << ++no << ": ";
                result << "reader future: " << task->isValidReadersFuture() << ", ";
                result << "part: " << task->read_info->data_part->name << ", ";
                result << "ranges: " << toString(task->ranges);
            }
        }
    }
    return result.str();
}

bool MergeTreePrefetchedReadPool::checkReadMethodAllowed(LocalFSReadMethod method)
{
    return method == LocalFSReadMethod::pread_threadpool || method == LocalFSReadMethod::pread_fake_async;
}
bool MergeTreePrefetchedReadPool::checkReadMethodAllowed(RemoteFSReadMethod method)
{
    return method == RemoteFSReadMethod::threadpool;
}

}
