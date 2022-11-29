#include <Storages/MergeTree/MergeTreePrefetchedReadPool.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Interpreters/threadPoolCallbackRunner.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

MergeTreePrefetchedReadPool::MergeTreePrefetchedReadPool(
    size_t threads,
    size_t sum_marks_,
    size_t min_marks_for_concurrent_read_,
    RangesInDataParts && parts_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const Names & column_names_,
    const Names & virtual_column_names_,
    size_t preferred_block_size_bytes_,
    const MergeTreeReaderSettings & reader_settings_,
    ContextPtr context_,
    bool use_uncompressed_cache_)
    : WithContext(context_)
    , log(&Poco::Logger::get("MergeTreePrefetchedReadPool"))
    , header(storage_snapshot_->getSampleBlockForColumns(column_names_))
    , storage_snapshot(storage_snapshot_)
    , mark_cache(context_->getGlobalContext()->getMarkCache().get())
    , uncompressed_cache(use_uncompressed_cache_ ? context_->getGlobalContext()->getUncompressedCache().get() : nullptr)
    , reader_settings(reader_settings_)
    , profile_callback([this](ReadBufferFromFileBase::ProfileInfo info_) { profileFeedback(info_); })
    , prefetch_threadpool(getContext()->getPrefetchThreadpool())
    , column_names(column_names_)
    , parts_infos(getPartsInfos(
                      parts_, prewhere_info_, virtual_column_names_, preferred_block_size_bytes_))
    , threads_tasks(createThreadsTasks(
                        threads, sum_marks_, min_marks_for_concurrent_read_, prewhere_info_))
{
}

struct MergeTreePrefetchedReadPool::PartInfo
{
    MergeTreeData::DataPartPtr data_part;
    size_t part_index_in_query;
    size_t sum_marks = 0;
    MarkRanges ranges;

    NameSet column_name_set;
    MergeTreeReadTaskColumns task_columns;
    MergeTreeBlockSizePredictorPtr size_predictor;
};

std::future<MergeTreeReaderPtr> MergeTreePrefetchedReadPool::createReader(
    const PartInfo & part,
    const NamesAndTypesList & columns,
    const MarkRanges & required_ranges) const
{
    auto reader = part.data_part->getReader(
        columns, storage_snapshot->metadata, required_ranges,
        uncompressed_cache, mark_cache, reader_settings,
        IMergeTreeReader::ValueSizeMap{}, profile_callback);

    LOG_TEST(
        log,
        "Created reader for part {} with ranges: {}",
        part.data_part->name, toString(required_ranges));

    /// In order to make a prefetch we need to wait for marks to be loaded. But we just created
    /// a reader (which starts loading marks in its constructor), then if we do prefetch right
    /// after creating a buffer, it will be very inefficient. We can do prefetch for all parts
    /// only inside this MergeTreePrefetchedReadPool, where read tasks are created and distributed,
    /// and we cannot block either, therefore make prefetch inside the pool and put the future
    /// into the read task (MergeTreeReadTask). When a thread calls getTask(), it will wait for
    /// it (if not yet ready) after getting the task.
    auto task = [reader = std::move(reader)]() mutable -> MergeTreeReaderPtr &&
    {
        reader->prefetch();
        return std::move(reader);
    };
    return scheduleFromThreadPool<MergeTreeReaderPtr>(std::move(task), prefetch_threadpool, "ReadPrepare");
}

MergeTreeReadTaskPtr MergeTreePrefetchedReadPool::getTask(size_t /* min_marks_to_read */, size_t thread)
{
    std::unique_lock lock{mutex};

    if (threads_tasks.empty())
        return nullptr;

    auto it = threads_tasks.find(thread);
    if (it == threads_tasks.end())
    {
        /// There is no point stealing in order (like in MergeTreeReadPool, where tasks can be stolen
        /// only from the next thread). Even if we steal task from the next thread, which reads from
        /// the same part as we just read, it might seem that we can resuse our own reader, do some
        /// seek avoiding and it will have a good result as we avoided seek (new request). But it is
        /// not so, because this next task will most likely have its own reader a prefetch already on
        /// the fly. (Not to mention that in fact we cannot reuse our own reader if initially we did
        /// not accounted this range into range request to object storage).
        auto threads_tasks_it = threads_tasks.begin();

        /// Steal only tasks which have an initialized reader (with prefetched data). Thus we avoid
        /// loosing a prefetch by creating our own reader (or resusing our own reader if the part
        /// is the same as last read by this thread).
        for (auto tasks_it = threads_tasks_it; it != threads_tasks.end(); ++it)
        {
            auto & thread_tasks = tasks_it->second;
            auto task_it = std::find_if(
                thread_tasks.begin(), thread_tasks.end(),
                [](const auto & task) { return task->reader.valid(); });

            if (task_it == thread_tasks.end())
                continue;

            auto task = std::move(*task_it);
            thread_tasks.pop_back();

            if (thread_tasks.empty())
                threads_tasks.erase(tasks_it);

            return task;
        }

        return nullptr;
    }

    auto & thread_tasks = it->second;

    auto task = std::move(thread_tasks.front());
    thread_tasks.pop_front();

    if (thread_tasks.empty())
        threads_tasks.erase(it);

    return task;
}

MergeTreePrefetchedReadPool::PartsInfos MergeTreePrefetchedReadPool::getPartsInfos(
    const RangesInDataParts & parts,
    const PrewhereInfoPtr & prewhere_info,
    const Names & virtual_column_names,
    size_t preferred_block_size_bytes) const
{
    PartsInfos result;
    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    const bool predict_block_size_bytes = preferred_block_size_bytes > 0;

    for (const auto & part : parts)
    {
        auto part_info = std::make_unique<PartInfo>();

        part_info->data_part = part.data_part;
        part_info->part_index_in_query = part.part_index_in_query;
        part_info->ranges = part.ranges;
        std::sort(part_info->ranges.begin(), part_info->ranges.end());

        /// Sum up total size of all mark ranges in a data part.
        for (const auto & range : part.ranges)
            part_info->sum_marks += range.end - range.begin;

        const auto task_columns = getReadTaskColumns(
            LoadedMergeTreeDataPartInfoForReader(part.data_part),
            storage_snapshot,
            column_names,
            virtual_column_names,
            prewhere_info,
            /* with_subcolumns */true);

        part_info->size_predictor = !predict_block_size_bytes
            ? nullptr
            : MergeTreeBaseSelectProcessor::getSizePredictor(part.data_part, task_columns, sample_block);

        /// Will be used to distinguish between PREWHERE and WHERE columns when applying filter.
        const auto & required_column_names = task_columns.columns.getNames();
        part_info->column_name_set = {required_column_names.begin(), required_column_names.end()};
        part_info->task_columns = std::move(task_columns);

        result.push_back(std::move(part_info));
    }

    return result;
}

MarkRanges MergeTreePrefetchedReadPool::getMarkRangesToRead(size_t need_marks, PartInfo & part)
{
    MarkRanges ranges_to_get_from_part;

    while (need_marks > 0 && !part.ranges.empty())
    {
        MarkRange & range = part.ranges.front();
        const size_t marks_in_range = range.end - range.begin;
        const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);
        need_marks -= marks_to_get_from_range;

        ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
        range.begin += marks_to_get_from_range;

        if (range.begin == range.end)
            part.ranges.pop_front();
    }

    return ranges_to_get_from_part;
}

MergeTreePrefetchedReadPool::ThreadsTasks MergeTreePrefetchedReadPool::createThreadsTasks(
    size_t threads, size_t sum_marks, size_t min_marks_for_concurrent_read, const PrewhereInfoPtr & prewhere_info) const
{
    if (parts_infos.empty())
        return {};

    ThreadsTasks result_threads_tasks;
    const size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;
    const size_t max_marks_per_part_to_prefetch = getContext()->getSettingsRef().max_marks_per_part_to_prefetch;

    LOG_TEST(
        log,
        "Sum marks: {}, threads: {}, min_marks_per_thread: {}, min_marks_for_concurrent_read: {}",
        sum_marks, threads, min_marks_per_thread, min_marks_for_concurrent_read);

    for (size_t i = 0, part_idx = 0; i < threads && part_idx < parts_infos.size(); ++i)
    {
        auto need_marks = min_marks_per_thread;

        while (need_marks > 0 && part_idx < parts_infos.size())
        {
            auto & part = *parts_infos[part_idx];
            size_t & marks_in_part = part.sum_marks;

            if (marks_in_part == 0)
            {
                ++part_idx;
                continue;
            }

            /// Do not get too few rows from part.
            if (marks_in_part >= min_marks_for_concurrent_read &&
                need_marks < min_marks_for_concurrent_read)
                need_marks = min_marks_for_concurrent_read;

            /// Do not leave too few rows in part for next time.
            if (marks_in_part > need_marks &&
                marks_in_part - need_marks < min_marks_for_concurrent_read)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;
            size_t need_marks_for_task = std::min(need_marks, marks_in_part);

            /// Prefetch not only for each part, but also for a required number of mark
            /// ranges, which is controlled by setting max_marks_per_part_to_prefetch.
            if (max_marks_per_part_to_prefetch > 1)
            {
                need_marks_for_task = std::min(need_marks_for_task, max_marks_per_part_to_prefetch);
            }

            assert(marks_in_part >= need_marks_for_task);
            if (marks_in_part == need_marks_for_task)
            {
                ranges_to_get_from_part = part.ranges;
            }
            else
            {
                ranges_to_get_from_part = getMarkRangesToRead(need_marks_for_task, part);
            }

            need_marks -= need_marks_for_task;
            sum_marks -= need_marks_for_task;
            marks_in_part -= need_marks_for_task;

            auto curr_task_size_predictor = !part.size_predictor ? nullptr
                : std::make_unique<MergeTreeBlockSizePredictor>(*part.size_predictor); /// make a copy

            auto & current_thread_tasks = result_threads_tasks[i];
            std::vector<std::future<MergeTreeReaderPtr>> pre_reader_for_step;

            /// TODO: Implement a setting max_early_prefetches.
            auto reader = createReader(part, part.task_columns.columns, ranges_to_get_from_part);

            if (reader_settings.apply_deleted_mask && part.data_part->hasLightweightDelete())
            {
                auto pre_reader = createReader(part, {LightweightDeleteDescription::FILTER_COLUMN}, ranges_to_get_from_part);
                pre_reader_for_step.push_back(std::move(pre_reader));
            }

            if (prewhere_info)
            {
                for (const auto & pre_columns_per_step : part.task_columns.pre_columns)
                {
                    auto pre_reader = createReader(part, pre_columns_per_step, ranges_to_get_from_part);
                    pre_reader_for_step.push_back(std::move(pre_reader));
                }
            }

            auto read_task = std::make_unique<MergeTreeReadTask>(
                part.data_part, ranges_to_get_from_part, part.part_index_in_query, column_names,
                part.column_name_set, part.task_columns, prewhere_info && prewhere_info->remove_prewhere_column,
                std::move(curr_task_size_predictor), std::move(reader), std::move(pre_reader_for_step));

            current_thread_tasks.push_back(std::move(read_task));
        }
    }

    LOG_TEST(
        log,
        "Result tasks: {} (parts: {}, threads: {})",
        result_threads_tasks.size(), parts_infos.size(), threads);

    return result_threads_tasks;
}

}
