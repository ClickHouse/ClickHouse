#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreePrefetchedReadPool.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <base/getThreadId.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event MergeTreePrefetchedReadPoolInit;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

MergeTreePrefetchedReadPool::MergeTreePrefetchedReadPool(
    size_t threads,
    size_t sum_marks_,
    size_t min_marks_for_concurrent_read_,
    RangesInDataParts && parts_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const Names & column_names_,
    const Names & virtual_column_names_,
    size_t preferred_block_size_bytes_,
    const MergeTreeReaderSettings & reader_settings_,
    ContextPtr context_,
    bool use_uncompressed_cache_,
    bool is_remote_read_,
    const MergeTreeSettings & storage_settings_)
    : WithContext(context_)
    , log(&Poco::Logger::get("MergeTreePrefetchedReadPool(" + (parts_.empty() ? "" : parts_.front().data_part->storage.getStorageID().getNameForLogs()) + ")"))
    , header(storage_snapshot_->getSampleBlockForColumns(column_names_))
    , mark_cache(context_->getGlobalContext()->getMarkCache().get())
    , uncompressed_cache(use_uncompressed_cache_ ? context_->getGlobalContext()->getUncompressedCache().get() : nullptr)
    , profile_callback([this](ReadBufferFromFileBase::ProfileInfo info_) { profileFeedback(info_); })
    , index_granularity_bytes(storage_settings_.index_granularity_bytes)
    , fixed_index_granularity(storage_settings_.index_granularity)
    , storage_snapshot(storage_snapshot_)
    , column_names(column_names_)
    , virtual_column_names(virtual_column_names_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , reader_settings(reader_settings_)
    , is_remote_read(is_remote_read_)
    , prefetch_threadpool(getContext()->getPrefetchThreadpool())
{
    /// Tasks creation might also create a lost of readers - check they do not
    /// do any time consuming operations in ctor.
    ProfileEventTimeIncrement<Milliseconds> watch(ProfileEvents::MergeTreePrefetchedReadPoolInit);

    parts_infos = getPartsInfos(parts_, preferred_block_size_bytes_);
    threads_tasks = createThreadsTasks(threads, sum_marks_, min_marks_for_concurrent_read_);
}

struct MergeTreePrefetchedReadPool::PartInfo
{
    MergeTreeData::DataPartPtr data_part;
    AlterConversionsPtr alter_conversions;
    size_t part_index_in_query;
    size_t sum_marks = 0;
    MarkRanges ranges;

    NameSet column_name_set;
    MergeTreeReadTaskColumns task_columns;
    MergeTreeBlockSizePredictorPtr size_predictor;

    size_t approx_size_of_mark = 0;
    size_t prefetch_step_marks = 0;

    size_t estimated_memory_usage_for_single_prefetch = 0;
    size_t required_readers_num = 0;
};

std::future<MergeTreeReaderPtr> MergeTreePrefetchedReadPool::createPrefetchedReader(
    const IMergeTreeDataPart & data_part,
    const NamesAndTypesList & columns,
    const AlterConversionsPtr & alter_conversions,
    const MarkRanges & required_ranges,
    Priority priority) const
{
    auto reader = data_part.getReader(
        columns, storage_snapshot->metadata, required_ranges,
        uncompressed_cache, mark_cache, alter_conversions, reader_settings,
        IMergeTreeReader::ValueSizeMap{}, profile_callback);

    /// In order to make a prefetch we need to wait for marks to be loaded. But we just created
    /// a reader (which starts loading marks in its constructor), then if we do prefetch right
    /// after creating a reader, it will be very inefficient. We can do prefetch for all parts
    /// only inside this MergeTreePrefetchedReadPool, where read tasks are created and distributed,
    /// and we cannot block either, therefore make prefetch inside the pool and put the future
    /// into the read task (MergeTreeReadTask). When a thread calls getTask(), it will wait for
    /// it (if not yet ready) after getting the task.
    auto task = [=, my_reader = std::move(reader), context = getContext()]() mutable -> MergeTreeReaderPtr &&
    {
        /// For async read metrics in system.query_log.
        PrefetchIncrement watch(context->getAsyncReadCounters());

        my_reader->prefetchBeginOfRange(priority);
        return std::move(my_reader);
    };
    return scheduleFromThreadPool<IMergeTreeDataPart::MergeTreeReaderPtr>(std::move(task), prefetch_threadpool, "ReadPrepare", priority);
}

void MergeTreePrefetchedReadPool::createPrefetchedReaderForTask(MergeTreeReadTask & task) const
{
    if (task.reader.valid())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Task already has a reader");

    task.reader = createPrefetchedReader(*task.data_part, task.task_columns.columns, task.alter_conversions, task.mark_ranges, task.priority);

    if (reader_settings.apply_deleted_mask && task.data_part->hasLightweightDelete())
    {
        auto pre_reader = createPrefetchedReader(*task.data_part, {LightweightDeleteDescription::FILTER_COLUMN}, task.alter_conversions, task.mark_ranges, task.priority);
        task.pre_reader_for_step.push_back(std::move(pre_reader));
    }

    for (const auto & pre_columns_per_step : task.task_columns.pre_columns)
    {
        auto pre_reader = createPrefetchedReader(*task.data_part, pre_columns_per_step, task.alter_conversions, task.mark_ranges, task.priority);
        task.pre_reader_for_step.push_back(std::move(pre_reader));
    }
}

bool MergeTreePrefetchedReadPool::TaskHolder::operator <(const TaskHolder & other) const
{
    chassert(task->priority >= 0);
    chassert(other.task->priority >= 0);
    return task->priority > other.task->priority; /// Less is better.
    /// With default std::priority_queue, top() returns largest element.
    /// So closest to 0 will be on top with this comparator.
}

void MergeTreePrefetchedReadPool::startPrefetches() const
{
    if (prefetch_queue.empty())
        return;

    [[maybe_unused]] TaskHolder prev(nullptr, 0);
    [[maybe_unused]] const Priority highest_priority{reader_settings.read_settings.priority.value + 1};
    assert(prefetch_queue.top().task->priority == highest_priority);
    while (!prefetch_queue.empty())
    {
        const auto & top = prefetch_queue.top();
        createPrefetchedReaderForTask(*top.task);
#ifndef NDEBUG
        if (prev.task)
        {
            assert(top.task->priority >= highest_priority);
            if (prev.thread_id == top.thread_id)
            {
                assert(prev.task->priority < top.task->priority);
            }
        }
        prev = top;
#endif
        prefetch_queue.pop();
    }
}

MergeTreeReadTaskPtr MergeTreePrefetchedReadPool::getTask(size_t thread)
{
    std::lock_guard lock(mutex);

    if (threads_tasks.empty())
        return nullptr;

    if (!started_prefetches)
    {
        started_prefetches = true;
        startPrefetches();
    }

    auto it = threads_tasks.find(thread);
    if (it == threads_tasks.end())
    {
        ThreadsTasks::iterator non_prefetched_tasks_to_steal = threads_tasks.end();
        ThreadsTasks::iterator prefetched_tasks_to_steal = threads_tasks.end();
        int64_t best_prefetched_task_priority = -1;

        /// There is no point stealing in order (like in MergeTreeReadPool, where tasks can be stolen
        /// only from the next thread). Even if we steal task from the next thread, which reads from
        /// the same part as we just read, it might seem that we can reuse our own reader, do some
        /// seek avoiding and it will have a good result as we avoided seek (new request). But it is
        /// not so, because this next task will most likely have its own reader a prefetch already on
        /// the fly. (Not to mention that in fact we cannot reuse our own reader if initially we did
        /// not accounted this range into range request to object storage).
        for (auto thread_tasks_it = threads_tasks.begin(); thread_tasks_it != threads_tasks.end(); ++thread_tasks_it)
        {
            /// Prefer to steal tasks which have an initialized reader (with prefetched data). Thus we avoid
            /// losing a prefetch by creating our own reader (or resusing our own reader if the part
            /// is the same as last read by this thread).
            auto & thread_tasks = thread_tasks_it->second;
            auto task_it = std::find_if(
                thread_tasks.begin(), thread_tasks.end(),
                [](const auto & task) { return task->reader.valid(); });

            if (task_it == thread_tasks.end())
            {
                /// The follow back to non-prefetched task should lie on the thread which
                /// has more tasks than others.
                if (non_prefetched_tasks_to_steal == threads_tasks.end()
                    || non_prefetched_tasks_to_steal->second.size() < thread_tasks.size())
                    non_prefetched_tasks_to_steal = thread_tasks_it;
            }
            /// Try to steal task with the best (lowest) priority (because it will be executed faster).
            else if (prefetched_tasks_to_steal == threads_tasks.end()
                || (*task_it)->priority < best_prefetched_task_priority)
            {
                best_prefetched_task_priority = (*task_it)->priority;
                chassert(best_prefetched_task_priority >= 0);
                prefetched_tasks_to_steal = thread_tasks_it;
            }
        }

        if (prefetched_tasks_to_steal != threads_tasks.end())
        {
            auto & thread_tasks = prefetched_tasks_to_steal->second;
            assert(!thread_tasks.empty());

            auto task_it = std::find_if(
                thread_tasks.begin(), thread_tasks.end(),
                [](const auto & task) { return task->reader.valid(); });
            assert(task_it != thread_tasks.end());

            auto task = std::move(*task_it);
            thread_tasks.erase(task_it);

            if (thread_tasks.empty())
                threads_tasks.erase(prefetched_tasks_to_steal);

            return task;
        }

        /// TODO: it also makes sense to first try to steal from the next thread if it has ranges
        /// from the same part as current thread last read - to reuse the reader.

        if (non_prefetched_tasks_to_steal != threads_tasks.end())
        {
            auto & thread_tasks = non_prefetched_tasks_to_steal->second;
            assert(!thread_tasks.empty());

            /// Get second half of the tasks.
            const size_t total_tasks = thread_tasks.size();
            const size_t half = total_tasks / 2;
            auto half_it = thread_tasks.begin() + half;
            assert(half_it != thread_tasks.end());

            /// Give them to current thread, as current thread's tasks list is empty.
            auto & current_thread_tasks = threads_tasks[thread];
            current_thread_tasks.insert(
                current_thread_tasks.end(), make_move_iterator(half_it), make_move_iterator(thread_tasks.end()));

            /// Erase them from the thread from which we steal.
            thread_tasks.resize(half);
            if (thread_tasks.empty())
                threads_tasks.erase(non_prefetched_tasks_to_steal);

            auto task = std::move(current_thread_tasks.front());
            current_thread_tasks.erase(current_thread_tasks.begin());
            if (current_thread_tasks.empty())
                threads_tasks.erase(thread);

            return task;
        }

        return nullptr;
    }

    auto & thread_tasks = it->second;
    assert(!thread_tasks.empty());

    auto task = std::move(thread_tasks.front());
    thread_tasks.pop_front();

    if (thread_tasks.empty())
        threads_tasks.erase(it);

    return task;
}

size_t getApproximateSizeOfGranule(const IMergeTreeDataPart & part, const Names & columns_to_read)
{
    ColumnSize columns_size{};
    for (const auto & col_name : columns_to_read)
        columns_size.add(part.getColumnSize(col_name));
    return columns_size.data_compressed / part.getMarksCount();
}

MergeTreePrefetchedReadPool::PartsInfos MergeTreePrefetchedReadPool::getPartsInfos(
    const RangesInDataParts & parts, size_t preferred_block_size_bytes) const
{
    PartsInfos result;
    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    const auto & settings = getContext()->getSettingsRef();
    const bool predict_block_size_bytes = preferred_block_size_bytes > 0;

    for (const auto & part : parts)
    {
        auto part_info = std::make_unique<PartInfo>();

        part_info->data_part = part.data_part;
        part_info->alter_conversions = part.alter_conversions;
        part_info->part_index_in_query = part.part_index_in_query;
        part_info->ranges = part.ranges;
        std::sort(part_info->ranges.begin(), part_info->ranges.end());

        LoadedMergeTreeDataPartInfoForReader part_reader_info(part.data_part, part_info->alter_conversions);

        /// Sum up total size of all mark ranges in a data part.
        for (const auto & range : part.ranges)
            part_info->sum_marks += range.end - range.begin;

        part_info->approx_size_of_mark = getApproximateSizeOfGranule(*part_info->data_part, column_names);

        const auto task_columns = getReadTaskColumns(
            part_reader_info,
            storage_snapshot,
            column_names,
            virtual_column_names,
            prewhere_info,
            actions_settings,
            reader_settings,
            /* with_subcolumns */ true);

        part_info->size_predictor = !predict_block_size_bytes
            ? nullptr
            : IMergeTreeSelectAlgorithm::getSizePredictor(part.data_part, task_columns, sample_block);

        /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter.
        const auto & required_column_names = task_columns.columns.getNames();
        part_info->column_name_set = {required_column_names.begin(), required_column_names.end()};
        part_info->task_columns = task_columns;

        /// adjustBufferSize(), which is done in MergeTreeReaderStream and MergeTreeReaderCompact,
        /// lowers buffer size if file size (or required read range) is less. So we know that the
        /// settings.prefetch_buffer_size will be lowered there, therefore we account it here as well.
        /// But here we make a more approximate lowering (because we do not have loaded marks yet),
        /// while in adjustBufferSize it will be presize.
        for (const auto & col : task_columns.columns)
        {
            const auto col_size = part.data_part->getColumnSize(col.name).data_compressed;
            part_info->estimated_memory_usage_for_single_prefetch += std::min<size_t>(col_size, settings.prefetch_buffer_size);
            ++part_info->required_readers_num;
        }
        if (reader_settings.apply_deleted_mask && part.data_part->hasLightweightDelete())
        {
            const auto col_size = part.data_part->getColumnSize(
                LightweightDeleteDescription::FILTER_COLUMN.name).data_compressed;
            part_info->estimated_memory_usage_for_single_prefetch += std::min<size_t>(col_size, settings.prefetch_buffer_size);
            ++part_info->required_readers_num;
        }
        if (prewhere_info)
        {
            for (const auto & columns : task_columns.pre_columns)
            {
                for (const auto & col : columns)
                {
                    const size_t col_size = part.data_part->getColumnSize(col.name).data_compressed;
                    part_info->estimated_memory_usage_for_single_prefetch += std::min<size_t>(col_size, settings.prefetch_buffer_size);
                    ++part_info->required_readers_num;
                }
            }
        }

        result.push_back(std::move(part_info));
    }

    return result;
}

MergeTreePrefetchedReadPool::ThreadsTasks MergeTreePrefetchedReadPool::createThreadsTasks(
    size_t threads, size_t sum_marks, size_t /* min_marks_for_concurrent_read */) const
{
    if (parts_infos.empty())
        return {};

    const auto & context = getContext();
    const auto & settings = context->getSettingsRef();

    size_t total_size_approx = 0;
    for (const auto & part : parts_infos)
    {
        total_size_approx += part->sum_marks * part->approx_size_of_mark;
    }

    size_t min_prefetch_step_marks = 0;

    for (const auto & part : parts_infos)
    {
        if (settings.filesystem_prefetch_step_marks)
        {
            part->prefetch_step_marks = settings.filesystem_prefetch_step_marks;
        }
        else if (settings.filesystem_prefetch_step_bytes && part->approx_size_of_mark)
        {
            part->prefetch_step_marks = std::max<size_t>(
                1, static_cast<size_t>(std::round(static_cast<double>(settings.filesystem_prefetch_step_bytes) / part->approx_size_of_mark)));
        }

        /// This limit is important to avoid spikes of slow aws getObject requests when parallelizing within one file.
        /// (The default is taken from here https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html).
        if (part->approx_size_of_mark
            && settings.filesystem_prefetch_min_bytes_for_single_read_task
            && part->approx_size_of_mark < settings.filesystem_prefetch_min_bytes_for_single_read_task)
        {
            const size_t min_prefetch_step_marks_by_total_cols = static_cast<size_t>(
                std::ceil(static_cast<double>(settings.filesystem_prefetch_min_bytes_for_single_read_task) / part->approx_size_of_mark));
            /// At least one task to start working on it right now and another one to prefetch in the meantime.
            const size_t new_min_prefetch_step_marks = std::min<size_t>(min_prefetch_step_marks_by_total_cols, sum_marks / threads / 2);
            if (min_prefetch_step_marks < new_min_prefetch_step_marks)
            {
                LOG_DEBUG(log, "Increasing min prefetch step from {} to {}", min_prefetch_step_marks, new_min_prefetch_step_marks);

                min_prefetch_step_marks = new_min_prefetch_step_marks;
            }
        }

        if (part->prefetch_step_marks < min_prefetch_step_marks)
        {
            LOG_DEBUG(log, "Increasing prefetch step from {} to {}", part->prefetch_step_marks, min_prefetch_step_marks);

            part->prefetch_step_marks = min_prefetch_step_marks;
        }

        LOG_DEBUG(
            log,
            "Part: {}, sum_marks: {}, approx mark size: {}, prefetch_step_bytes: {}, prefetch_step_marks: {}, (ranges: {})",
            part->data_part->name,
            part->sum_marks,
            part->approx_size_of_mark,
            settings.filesystem_prefetch_step_bytes,
            part->prefetch_step_marks,
            toString(part->ranges));
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

    ThreadsTasks result_threads_tasks;
    size_t total_tasks = 0;
    for (size_t i = 0, part_idx = 0; i < threads && part_idx < parts_infos.size(); ++i)
    {
        int64_t need_marks = min_marks_per_thread;

        /// Priority is given according to the prefetch number for each thread,
        /// e.g. the first task of each thread has the same priority and is greater
        /// than the second task of each thread, and so on.
        /// Add 1 to query read priority because higher priority should be given to
        /// reads from pool which are from reader.
        Priority priority{reader_settings.read_settings.priority.value + 1};

        while (need_marks > 0 && part_idx < parts_infos.size())
        {
            auto & part = *parts_infos[part_idx];
            size_t & marks_in_part = part.sum_marks;

            if (marks_in_part == 0)
            {
                ++part_idx;
                continue;
            }

            MarkRanges ranges_to_get_from_part;
            size_t marks_to_get_from_part = std::min<size_t>(need_marks, marks_in_part);

            /// Split by prefetch step even if !allow_prefetch below. Because it will allow
            /// to make a better distribution of tasks which did not fill into memory limit
            /// or prefetches limit through tasks stealing.
            if (part.prefetch_step_marks)
            {
                marks_to_get_from_part = std::min<size_t>(marks_to_get_from_part, part.prefetch_step_marks);
            }

            if (marks_in_part == marks_to_get_from_part)
            {
                ranges_to_get_from_part = part.ranges;
            }
            else
            {
                if (part.sum_marks < marks_to_get_from_part)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Requested {} marks from part {}, but part has only {} marks",
                        marks_to_get_from_part, part.data_part->name, part.sum_marks);
                }

                size_t get_marks_num = marks_to_get_from_part;
                while (get_marks_num > 0)
                {
                    MarkRange & range = part.ranges.front();
                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, get_marks_num);
                    get_marks_num -= marks_to_get_from_range;

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;

                    if (range.begin == range.end)
                    {
                        part.ranges.pop_front();
                    }
                    else if (!get_marks_num && part.prefetch_step_marks && range.end - range.begin < part.prefetch_step_marks)
                    {
                        /// We already have `get_marks_num` marks, but current mark range has
                        /// less than `prefetch_step_marks` marks, then add them too.
                        ranges_to_get_from_part.emplace_back(range.begin, range.end);
                        marks_to_get_from_part += range.end - range.begin;
                        part.ranges.pop_front();
                    }
                }
            }

            need_marks -= marks_to_get_from_part;
            sum_marks -= marks_to_get_from_part;
            marks_in_part -= marks_to_get_from_part;

            auto curr_task_size_predictor = !part.size_predictor ? nullptr
                : std::make_unique<MergeTreeBlockSizePredictor>(*part.size_predictor); /// make a copy

            auto read_task = std::make_unique<MergeTreeReadTask>(
                part.data_part,
                part.alter_conversions,
                ranges_to_get_from_part,
                part.part_index_in_query,
                part.column_name_set,
                part.task_columns,
                std::move(curr_task_size_predictor));

            read_task->priority = priority;

            bool allow_prefetch = false;
            if (allowed_memory_usage
                && (allowed_prefetches_num.has_value() == false || allowed_prefetches_num.value() > 0))
            {
                allow_prefetch = part.estimated_memory_usage_for_single_prefetch <= allowed_memory_usage
                    && (allowed_prefetches_num.has_value() == false
                        || part.required_readers_num <= allowed_prefetches_num.value());

                if (allow_prefetch)
                {
                    allowed_memory_usage -= part.estimated_memory_usage_for_single_prefetch;
                    if (allowed_prefetches_num.has_value())
                        *allowed_prefetches_num -= part.required_readers_num;
                }
            }

            if (allow_prefetch)
            {
                prefetch_queue.emplace(TaskHolder(read_task.get(), i));
            }
            ++priority.value;

            result_threads_tasks[i].push_back(std::move(read_task));
            ++total_tasks;
        }
    }

    LOG_TEST(log, "Result tasks {} for {} threads: {}", total_tasks, threads, dumpTasks(result_threads_tasks));

    return result_threads_tasks;
}

std::string MergeTreePrefetchedReadPool::dumpTasks(const ThreadsTasks & tasks)
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
                result << "reader: " << task->reader.valid() << ", ";
                result << "part: " << task->data_part->name << ", ";
                result << "ranges: " << toString(task->mark_ranges);
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
