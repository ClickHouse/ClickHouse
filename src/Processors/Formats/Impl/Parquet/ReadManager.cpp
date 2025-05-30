#include <Processors/Formats/Impl/Parquet/ReadManager.h>

#include <Common/ProfileEvents.h>
#include <Formats/FormatParserGroup.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace ProfileEvents
{
    extern const Event ParquetDecodingTasks;
    extern const Event ParquetDecodingTaskBatches;
}

namespace DB::Parquet
{

void ReadManager::init(FormatParserGroupPtr parser_group_)
{
    parser_group = parser_group_;
    reader.file_metadata = Reader::readFileMetaData(reader.prefetcher);
    reader.prefilterAndInitRowGroups();
    reader.preparePrewhere();

    /// TODO [parquet]: distribute memory_target_fraction more carefully: 0 for skipped stages, higher for prewhere
    double sum = 0;
    stages[size_t(ReadStage::MainData)].memory_target_fraction *= 5;
    stages[size_t(ReadStage::NotStarted)].memory_target_fraction = 0;
    stages[size_t(ReadStage::Deliver)].memory_target_fraction = 0;
    for (size_t i = 0; i < stages.size(); ++i)
        sum += stages[i].memory_target_fraction;
    for (size_t i = 0; i < stages.size(); ++i)
        stages[i].memory_target_fraction /= sum;

    /// The NotStarted stage completed for all row groups, transition to next stage.
    for (size_t i = 0; i < reader.row_groups.size(); ++i)
        finishRowGroupStage(i, MemoryUsageDiff(ReadStage::NotStarted));
}

ReadManager::~ReadManager()
{
    shutdown->shutdown();
}

void ReadManager::cancel() noexcept
{
    {
        std::lock_guard lock(stages.at(size_t(ReadStage::Deliver)).mutex);
        if (exception)
            return;
        exception = std::make_exception_ptr(Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Cancelled"));
    }
    delivery_cv.notify_all();
}

void ReadManager::finishRowGroupStage(size_t row_group_idx, MemoryUsageDiff && diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];
    RowGroupReadStage row_group_stage = row_group.stage.load(std::memory_order_relaxed);

    /// Finish the stage.
    if (row_group_stage == RowGroupReadStage::BloomAndDictionaryFilters)
    {
        if (!reader.applyBloomAndDictionaryFilters(row_group))
            row_group_stage = RowGroupReadStage::Subgroups; // skip the row group
        for (auto & c : row_group.columns)
        {
            c.bloom_filter_header_prefetch.reset(&diff);
            c.bloom_filter_data_prefetch.reset(&diff);
            for (auto & b : c.bloom_filter_blocks)
                b.prefetch.reset(&diff);
            c.bloom_filter_blocks.clear();
        }
    }

    /// Determine what stage to transition to and which columns are involved.
    std::vector<Task> add_tasks;
    while (true) // loop over skipped stages
    {
        chassert(row_group_stage < RowGroupReadStage::Deallocated);
        row_group_stage = RowGroupReadStage(int(row_group_stage) + 1);

        /// Start the new stage.
        switch (row_group_stage)
        {
            case RowGroupReadStage::NotStarted:
                chassert(false);
                break;
            case RowGroupReadStage::BloomAndDictionaryFilters:
                for (size_t i = 0; i < row_group.columns.size(); ++i)
                {
                    const auto & c = row_group.columns[i];
                    if (c.bloom_filter_header_prefetch)
                        add_tasks.push_back(Task {
                            .stage = ReadStage::BloomFilterHeader, .scope = TaskScope::ColumnChunk,
                            .row_group_idx = row_group_idx, .column_idx = i});
                    else if (c.use_dictionary_filter)
                        add_tasks.push_back(Task {
                            .stage = ReadStage::BloomFilterBlocksOrDictionary,
                            .scope = TaskScope::RowGroup,
                            .row_group_idx = row_group_idx, .column_idx = i});
                }
                break;
            case RowGroupReadStage::ColumnIndex:
                for (size_t i = 0; i < row_group.columns.size(); ++i)
                    if (row_group.columns[i].use_column_index)
                        add_tasks.push_back(Task {
                            .stage = ReadStage::ColumnIndexAndOffsetIndex,
                            .scope = TaskScope::RowGroup,
                            .row_group_idx = row_group_idx, .column_idx = i});
                break;
            case RowGroupReadStage::Subgroups:
                reader.intersectColumnIndexResultsAndInitSubgroups(row_group);
                if (!row_group.subgroups.empty())
                {
                    row_group.stage.store(row_group_stage);
                    row_group.stage_tasks_remaining.store(row_group.subgroups.size(), std::memory_order_relaxed);
                    /// Start the first subgroup.
                    finishRowSubgroupStage(row_group_idx, /*row_subgroup_idx=*/ 0, std::move(diff));
                    return;
                }
                /// The whole row group was filtered out.
                break;
            case RowGroupReadStage::Deallocated:
                for (auto & c : row_group.columns)
                    clearColumnChunk(c, &diff);
                for (auto & s : row_group.subgroups)
                    clearRowSubgroup(s, &diff);
                break;
        }

        if (!add_tasks.empty() || row_group_stage == RowGroupReadStage::Deallocated)
            break;

        /// Nothing needs to be done for this stage, skip to next stage.
    }

    row_group.stage.store(row_group_stage);
    row_group.stage_tasks_remaining.store(add_tasks.size(), std::memory_order_relaxed);

    if (row_group_stage == RowGroupReadStage::Deallocated)
    {
        size_t i = first_incomplete_row_group.load();
        while (i < reader.row_groups.size() && reader.row_groups[i].stage.load() == RowGroupReadStage::Deallocated)
        {
            if (first_incomplete_row_group.compare_exchange_weak(i, i + 1))
            {
                diff.retry_scheduling_for_all_stages = true;

                if (i + 1 == reader.row_groups.size())
                {
                    /// Notify read() that everything is done.
                    {
                        /// Lock and unlock to avoid race condition on condition variable.
                        /// (Otherwise the notify_all() may happen after read() saw the old
                        ///  first_incomplete_row_group value but before it started waiting
                        ///  on delivery_cv.)
                        std::lock_guard lock(stages.at(size_t(ReadStage::Deliver)).mutex);
                    }
                    delivery_cv.notify_all();
                }
            }
        }
    }

    /// If we freed some memory, see if more tasks need to be scheduled.
    flushMemoryUsageDiff(std::move(diff));

    if (!add_tasks.empty())
        addTasks(std::move(add_tasks));
}

void ReadManager::addTasks(std::vector<Task> add_tasks)
{
    chassert(!add_tasks.empty());
    /// Group tasks by stage.
    std::sort(add_tasks.begin(), add_tasks.end(), [](const Task & a, const Task & b)
        { return std::tie(a.stage, a.column_idx) < std::tie(b.stage, b.column_idx); });
    for (size_t start = 0; start < add_tasks.size(); ) // loop over stages
    {
        ReadStage stage = add_tasks[start].stage;
        Stage & stage_state = stages.at(size_t(stage));

        std::unique_lock lock(stage_state.mutex);

        /// Enqueue tasks for stage.
        size_t i;
        for (i = start; i < add_tasks.size() && add_tasks[i].stage == stage; ++i)
            stage_state.tasks_to_schedule.push(add_tasks[i]);
        start = i;

        scheduleTasksIfNeeded(stage, lock);
    }
}

std::vector<ReadManager::Task> ReadManager::makeTasksToReadColumns(size_t row_group_idx, size_t row_subgroup_idx, bool is_prewhere)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];

    std::vector<Task> add_tasks;
    for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
    {
        if (reader.primitive_columns[i].use_prewhere != is_prewhere)
            continue;
        ColumnChunk & c = row_group.columns.at(i);
        if (c.offset_index_prefetch && c.offset_index.page_locations.empty())
        {
            /// If offset index for this column wasn't read by previous stages, make a task
            /// to read it before reading data.
            add_tasks.push_back(Task {
                .stage = is_prewhere ? ReadStage::PrewhereOffsetIndex : ReadStage::MainOffsetIndex,
                .scope = TaskScope::ColumnChunk,
                .row_group_idx = row_group_idx,
                .row_subgroup_idx = row_subgroup_idx,
                .column_idx = i});
        }
        else
        {
            add_tasks.push_back(Task {
                .stage = is_prewhere ? ReadStage::PrewhereData : ReadStage::MainData,
                .scope = TaskScope::RowSubgroup,
                .row_group_idx = row_group_idx,
                .row_subgroup_idx = row_subgroup_idx,
                .column_idx = i});
        }
    }

    if (add_tasks.empty())
        /// If PREWHERE expression doesn't use any columns, we must still run it.
        /// Add a task that will just call finishRowGroupStage().
        /// (Why not just run prewhere for all row subgroups right away and skip task scheduling?
        ///  Because the resulting row masks may, in principle, use a lot of memory.)
        /// (This code also applies for non-prewhere columns, where there's no advantage in going
        ///  through task scheduling like this. But it's less code this way.)
        add_tasks.push_back(Task {
            .stage = is_prewhere ? ReadStage::PrewhereData : ReadStage::MainData,
            .scope = TaskScope::RowSubgroup,
            .row_group_idx = row_group_idx,
            .row_subgroup_idx = row_subgroup_idx,
            .column_idx = UINT64_MAX});

    return add_tasks;
}

void ReadManager::finishRowSubgroupStage(size_t row_group_idx, size_t row_subgroup_idx, MemoryUsageDiff && diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];
    RowSubgroup & row_subgroup = row_group.subgroups[row_subgroup_idx];
    RowSubgroupReadStage row_subgroup_stage = row_subgroup.stage.load(std::memory_order_relaxed);

    switch (row_subgroup_stage)
    {
        case RowSubgroupReadStage::NotStarted:
            if (!reader.prewhere_steps.empty())
            {
                /// Start prewhere.
                flushMemoryUsageDiff(std::move(diff));
                auto add_tasks = makeTasksToReadColumns(row_group_idx, row_subgroup_idx, /*is_prewhere=*/ true);
                row_subgroup.stage.store(RowSubgroupReadStage::Prewhere, std::memory_order_relaxed);
                row_subgroup.stage_tasks_remaining.store(add_tasks.size(), std::memory_order_relaxed);
                addTasks(std::move(add_tasks));
                return;
            }

            /// No prewhere.
            chassert(row_subgroup_idx == 0);
            row_group.prewhere_ptr.store(row_group.subgroups.size(), std::memory_order_relaxed);
            break; // proceed to advancing read_ptr (because we moved prewhere_ptr)
        case RowSubgroupReadStage::Prewhere:
        {
            chassert(!reader.prewhere_steps.empty());
            reader.applyPrewhere(row_subgroup);
            size_t prev = row_group.prewhere_ptr.exchange(row_subgroup_idx + 1);
            chassert(prev == row_subgroup_idx);
            if (row_subgroup_idx + 1 == row_group.subgroups.size())
            {
                /// Finished prewhere in all subgroups.
                for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                    if (reader.primitive_columns[i].use_prewhere)
                        clearColumnChunk(row_group.columns.at(i), &diff);
            }
            break; // proceed to advancing read_ptr (because we moved prewhere_ptr)
        }
        case RowSubgroupReadStage::MainColumns:
        {
            size_t prev = row_group.read_ptr.exchange(row_subgroup_idx + 1);
            chassert(prev == row_subgroup_idx);
            row_subgroup.stage.store(RowSubgroupReadStage::Deliver, std::memory_order::relaxed);
            Stage & stage_state = stages.at(size_t(ReadStage::Deliver));
            {
                std::unique_lock lock(stage_state.mutex);
                stage_state.tasks_to_schedule.push(Task {
                    .stage = ReadStage::Deliver,
                    .scope = TaskScope::RowSubgroup,
                    .row_group_idx = row_group_idx,
                    .row_subgroup_idx = row_subgroup_idx});
            }
            delivery_cv.notify_one();
            break; // proceed to advancing read_ptr (because we moved read_ptr)
        }
        case RowSubgroupReadStage::Deliver:
        {
            row_subgroup.stage.store(RowSubgroupReadStage::Deallocated);
            clearRowSubgroup(row_subgroup, &diff);
            advanceDeliveryPtrIfNeeded(row_group_idx, diff);
            size_t remaining = row_group.stage_tasks_remaining.fetch_sub(1);
            chassert(remaining > 0);
            if (remaining == 1)
                finishRowGroupStage(row_group_idx, std::move(diff));
            else
                flushMemoryUsageDiff(std::move(diff));
            return;
        }
        case RowSubgroupReadStage::Deallocated:
            chassert(false);
            break;
    }

    /// Start reading the next row subgroup if needed.
    /// Skip subgroups that were fully filtered out by prewhere.
    size_t read_ptr = row_group.read_ptr.load();
    size_t prewhere_ptr = row_group.prewhere_ptr.load();
    while (read_ptr < row_group.subgroups.size() && read_ptr < prewhere_ptr)
    {
        RowSubgroup & next_subgroup = row_group.subgroups[read_ptr];
        RowSubgroupReadStage next_subgroup_stage = next_subgroup.stage.load();
        if (next_subgroup_stage >= RowSubgroupReadStage::MainColumns)
            break;

        if (!next_subgroup.stage.compare_exchange_strong(
                next_subgroup_stage, RowSubgroupReadStage::MainColumns))
            break;
        if (next_subgroup.filter.rows_pass > 0)
        {
            auto add_tasks = makeTasksToReadColumns(row_group_idx, read_ptr, /*is_prewhere=*/ false);
            next_subgroup.stage_tasks_remaining.store(add_tasks.size(), std::memory_order_relaxed);
            addTasks(std::move(add_tasks));
            break;
        }

        /// Skip subgroup that was filtered out by prewhere.

        size_t prev = row_group.read_ptr.exchange(read_ptr + 1);
        chassert(prev == read_ptr);
        read_ptr += 1;

        next_subgroup.stage.store(RowSubgroupReadStage::Deallocated);
        clearRowSubgroup(next_subgroup, &diff);
        advanceDeliveryPtrIfNeeded(row_group_idx, diff);
        size_t remaining = row_group.stage_tasks_remaining.fetch_sub(1);
        chassert(remaining > 0);
        if (remaining == 1)
        {
            chassert(read_ptr == row_group.subgroups.size());
            finishRowGroupStage(row_group_idx, std::move(diff));
            return;
        }
    }

    if (row_subgroup_stage == RowSubgroupReadStage::Prewhere && row_subgroup_idx + 1 < row_group.subgroups.size())
    {
        /// Start prewhere in the next row group. (This recursion is at most one level deep.)
        chassert(row_group.subgroups.at(row_subgroup_idx + 1).stage == RowSubgroupReadStage::NotStarted);
        finishRowSubgroupStage(row_group_idx, row_subgroup_idx + 1, std::move(diff));
    }
    else
    {
        flushMemoryUsageDiff(std::move(diff));
    }
}

void ReadManager::advanceDeliveryPtrIfNeeded(size_t row_group_idx, MemoryUsageDiff & diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];
    size_t i = row_group.delivery_ptr.load();
    while (i < row_group.subgroups.size() && row_group.subgroups[i].stage.load() == RowSubgroupReadStage::Deallocated)
    {
        if (!row_group.delivery_ptr.compare_exchange_weak(i, i + 1))
            continue;
        i += 1;
        if (first_incomplete_row_group.load() == row_group_idx)
            diff.retry_scheduling_for_all_stages = true;
    }
}

static bool checkTaskSchedulingLimits(size_t memory_usage, size_t added_memory, size_t batches_in_progress, size_t added_tasks, const ParserGroupExt::Limits & limits)
{
    if (added_tasks == 0)
    {
        return memory_usage < limits.memory_low_watermark ||
            (memory_usage <= limits.memory_high_watermark && batches_in_progress < limits.parsing_threads);
    }
    else
    {
        /// If we're going to pay the cost of adding tasks to the queue, prefer to add many at once.
        return added_memory < limits.memory_low_watermark ||
               (memory_usage + added_memory <= limits.memory_high_watermark &&
                added_tasks < limits.parsing_threads);
    }
}

void ReadManager::flushMemoryUsageDiff(MemoryUsageDiff && diff)
{
    chassert(!diff.finalized);
    diff.finalized = true;
    for (size_t i = 0; i < diff.by_stage.size(); ++i)
    {
        ssize_t d = diff.by_stage[i];
        if (i == size_t(ReadStage::Deliver))
        {
            chassert(d == 0);
            continue;
        }
        if (d != 0)
            stages[i].memory_usage.fetch_add(d, std::memory_order_relaxed);

        bool should_schedule = diff.retry_scheduling_for_all_stages ||
            (diff.retry_scheduling_for_cur_stage && i == size_t(diff.cur_stage));
        if (!should_schedule && d < 0)
        {
            const auto & stage = stages[i];
            auto limits = ParserGroupExt::getLimitsPerReader(*parser_group, stage.memory_target_fraction);
            should_schedule = checkTaskSchedulingLimits(
                stage.memory_usage.load(std::memory_order_relaxed), 0,
                stage.batches_in_progress.load(std::memory_order_relaxed), 0, limits);
        }
        if (should_schedule)
        {
            size_t status = stages[i].task_scheduling_status.fetch_or(Stage::TASK_SCHEDULING_NEEDED, std::memory_order_release);
            if (status == 0)
            {
                std::unique_lock lock(stages[i].mutex);
                scheduleTasksIfNeeded(ReadStage(i), lock);
            }
        }
    }
}

void ReadManager::scheduleTasksIfNeeded(ReadStage stage_idx, std::unique_lock<std::mutex> & /*stage_lock*/)
{
    chassert(stage_idx < ReadStage::Deliver);

    Stage & stage = stages.at(size_t(stage_idx));

    while (true)
    {
        stage.task_scheduling_status.exchange(Stage::TASK_SCHEDULING_IN_PROGRESS, std::memory_order_acquire);

        if (!stage.tasks_to_schedule.empty())
        {
            MemoryUsageDiff diff(stage_idx);
            std::vector<Task> tasks;
            auto limits = ParserGroupExt::getLimitsPerReader(*parser_group, stage.memory_target_fraction);
            size_t memory_usage = stage.memory_usage.load(std::memory_order_relaxed);
            size_t batches_in_progress = stage.batches_in_progress.load(std::memory_order_relaxed);
            /// Need to be careful to avoid getting deadlocked in a situation where tasks can't be scheduled
            /// because memory usage is high, while memory usage can't decrease because tasks can't be scheduled.
            /// The way we prevent it is by always allowing scheduling tasks for the lowest-numbered
            /// <row group, row subgroup> pair that hasn't been completed (delivered or skipped) yet.
            /// TODO [parquet]: Surely there's a simpler way to avoid getting stuck, and to do scheduling in general?
            auto is_privileged_task = [&](const Task & task)
            {
                size_t i = first_incomplete_row_group.load();
                if (task.row_group_idx != i)
                    return false;
                if (task.row_subgroup_idx == UINT64_MAX)
                    return true;
                return task.row_subgroup_idx == reader.row_groups.at(i).delivery_ptr.load();
            };
            while (!stage.tasks_to_schedule.empty() &&
                   (checkTaskSchedulingLimits(
                       memory_usage, size_t(diff.by_stage[size_t(stage_idx)]),
                       batches_in_progress, tasks.size(),
                       limits) ||
                    is_privileged_task(stage.tasks_to_schedule.top())))
            {
                /// Kicks off prefetches and adds their (and other) memory usage estimate to `diff`.
                scheduleTask(stage.tasks_to_schedule.top(), &diff, tasks);
                stage.tasks_to_schedule.pop();
            }

            chassert(!diff.finalized);
            diff.finalized = true;
            for (size_t i = 0; i < diff.by_stage.size(); ++i)
            {
                chassert(diff.by_stage[i] >= 0); // scheduleTask doesn't do tracked deallocations
                if (diff.by_stage[i] != 0)
                {
                    chassert(i != size_t(ReadStage::Deliver));
                    stages[i].memory_usage.fetch_add(diff.by_stage[i], std::memory_order_relaxed);
                }
            }

            if (!tasks.empty())
            {
                /// Group tiny tasks into batches to reduce scheduling overhead.
                std::vector<std::function<void()>> funcs;
                funcs.reserve(std::min(tasks.size(), limits.parsing_threads) + 1);
                size_t bytes_per_batch = size_t(diff.by_stage[size_t(stage_idx)]) / limits.parsing_threads;
                size_t tasks_per_batch = tasks.size() / limits.parsing_threads;
                size_t i = 0;
                while (i < tasks.size())
                {
                    size_t bytes = 0;
                    size_t n = 0;
                    std::vector<Task> batch;
                    while (i < tasks.size() && bytes <= bytes_per_batch && n <= tasks_per_batch)
                    {
                        batch.push_back(tasks[i]);
                        bytes += tasks[i].cost_estimate_bytes;
                        n += 1;
                        ++i;
                    }
                    funcs.push_back([this, batch_ = std::move(batch)]
                    {
                        std::shared_lock shutdown_lock(*shutdown, std::try_to_lock);
                        if (!shutdown_lock.owns_lock())
                            return;
                        runBatchOfTasks(batch_);
                    });
                }
                stage.batches_in_progress.fetch_add(funcs.size(), std::memory_order_relaxed);
                ProfileEvents::increment(ProfileEvents::ParquetDecodingTasks, tasks.size());
                ProfileEvents::increment(ProfileEvents::ParquetDecodingTaskBatches, funcs.size());
                parser_group->parsing_runner.bulkSchedule(std::move(funcs));
            }
        }

        /// Check if another thread asked us to retry scheduling.
        /// (If TASK_SCHEDULING_IN_PROGRESS was unset by another thread, it's that thread's responsibility.)
        if (stage.task_scheduling_status.exchange(0, std::memory_order_relaxed) != (Stage::TASK_SCHEDULING_IN_PROGRESS | Stage::TASK_SCHEDULING_NEEDED))
            break;
    }
}

void ReadManager::scheduleTask(Task task, MemoryUsageDiff * diff, std::vector<Task> & out_tasks)
{
    /// Kick off prefetches and count estimated memory usage.
    std::vector<PrefetchHandle *> prefetches;
    RowGroup & row_group = reader.row_groups[task.row_group_idx];
    ssize_t memory_before = diff->by_stage[size_t(diff->cur_stage)];
    if (task.column_idx != UINT64_MAX)
    {
        ColumnChunk & column = row_group.columns.at(task.column_idx);
        switch (task.stage)
        {
            case ReadStage::BloomFilterHeader:
                prefetches.push_back(&column.bloom_filter_header_prefetch);
                break;
            case ReadStage::BloomFilterBlocksOrDictionary:
                if (column.use_dictionary_filter)
                    prefetches.push_back(&column.dictionary_page_prefetch);
                for (auto & b : column.bloom_filter_blocks)
                    prefetches.push_back(&b.prefetch);
                break;
            case ReadStage::ColumnIndexAndOffsetIndex:
                prefetches.push_back(&column.column_index_prefetch);
                prefetches.push_back(&column.offset_index_prefetch);
                break;
            case ReadStage::PrewhereOffsetIndex:
            case ReadStage::MainOffsetIndex:
                prefetches.push_back(&column.offset_index_prefetch);
                break;
            case ReadStage::PrewhereData:
            case ReadStage::MainData:
            {
                RowSubgroup & row_subgroup = row_group.subgroups.at(task.row_subgroup_idx);
                ColumnSubchunk & subchunk = row_subgroup.columns.at(task.column_idx);

                reader.determinePagesToPrefetch(column, row_subgroup, row_group, prefetches);

                /// Side note: would be nice to avoid reading the dictionary if all dictionary-encoded
                /// pages were filtered out (e.g. if it's a 100 MB column chunk with unique long strings,
                /// typically only the first ~1 MB would be dictionary-encoded; if we only need a few
                /// rows, we likely won't hit that 1 MB). But AFAICT parquet metadata doesn't have
                /// enough information for that (there's no page encoding in offset/column indexes).
                if (!column.dictionary.isInitialized() && column.dictionary_page_prefetch)
                    prefetches.push_back(&column.dictionary_page_prefetch);

                if (column.data_pages.empty())
                    prefetches.push_back(&column.data_pages_prefetch);

                double bytes_per_row = reader.estimateColumnMemoryBytesPerRow(column, row_group, reader.primitive_columns.at(task.column_idx));
                size_t column_memory = size_t(bytes_per_row * row_subgroup.filter.rows_pass);
                subchunk.column_and_offsets_memory = MemoryUsageToken(column_memory, diff);
                break;
            }
            case ReadStage::NotStarted:
            case ReadStage::Deliver:
            case ReadStage::Deallocated:
                chassert(false);
                break;
        }
    }

    if (task.stage == ReadStage::PrewhereData)
    {
        RowSubgroup & row_subgroup = row_group.subgroups.at(task.row_subgroup_idx);
        /// (This is not a data race because the caller holds `stages[...].mutex`.)
        if (!row_subgroup.filter.memory)
            row_subgroup.filter.memory = MemoryUsageToken(row_subgroup.filter.rows_total, diff);
    }

    reader.prefetcher.startPrefetch(prefetches, diff);

    /// We want to detect tiny tasks to group them together to reduce scheduling overhead.
    /// Use the predicted memory usage as a rough estimate of how long a task will take.
    /// E.g. main data read task's memory estimate consists of the input page sizes and the output
    /// column size; the run time is also roughly proportional to these sizes.
    /// Hope it's a good enough proxy in all cases.
    ssize_t memory_after = diff->by_stage[size_t(diff->cur_stage)];
    task.cost_estimate_bytes = size_t(std::max(0l, memory_after - memory_before));

    out_tasks.push_back(task);
}

void ReadManager::runBatchOfTasks(const std::vector<Task> & tasks) noexcept
{
    ReadStage stage = tasks.at(0).stage;
    size_t column_idx = UINT64_MAX;

    std::exception_ptr exc;
    std::optional<MemoryUsageDiff> diff;
    try
    {
        for (size_t i = 0; i < tasks.size(); ++i)
        {
            chassert(tasks[i].stage == stage);
            if (!diff.has_value())
                diff.emplace(stage);
            column_idx = tasks[i].column_idx;

            runTask(tasks[i], i + 1 == tasks.size(), *diff);

            if (diff->finalized)
                diff.reset();
        }
        if (diff.has_value())
            flushMemoryUsageDiff(std::move(*diff));
    }
    catch (DB::Exception & e)
    {
        e.addMessage("read stage: {}", magic_enum::enum_name(stage));
        if (column_idx != UINT64_MAX)
            e.addMessage("column: {}", reader.primitive_columns[column_idx].name);
        exc = std::current_exception();
    }
    catch (...)
    {
        exc = std::current_exception();
    }
    if (exc)
    {
        {
            std::lock_guard lock(stages.at(size_t(ReadStage::Deliver)).mutex);
            exception = exc;
        }
        delivery_cv.notify_all();
    }
}

void ReadManager::runTask(Task task, bool last_in_batch, MemoryUsageDiff & diff)
{
    RowGroup & row_group = reader.row_groups.at(task.row_group_idx);
    RowSubgroup * row_subgroup = task.row_subgroup_idx == UINT64_MAX ? nullptr : &row_group.subgroups.at(task.row_subgroup_idx);
    if (task.column_idx != UINT64_MAX)
    {
        ColumnChunk & column = row_group.columns.at(task.column_idx);
        const PrimitiveColumnInfo & column_info = reader.primitive_columns.at(task.column_idx);

        switch (task.stage)
        {
            case ReadStage::BloomFilterHeader:
                reader.processBloomFilterHeader(column, column_info);
                break;
            case ReadStage::BloomFilterBlocksOrDictionary:
                if (column.use_dictionary_filter)
                    reader.decodeDictionaryPage(column, column_info);
                else
                    reader.decodeBloomFilterBlocks(column, column_info);
                column.bloom_filter_header_prefetch.reset(&diff);
                break;
            case ReadStage::ColumnIndexAndOffsetIndex:
                reader.decodeOffsetIndex(column, row_group);
                column.offset_index_prefetch.reset(&diff);
                reader.applyColumnIndex(column, column_info);
                column.column_index_prefetch.reset(&diff);
                break;
            case ReadStage::PrewhereOffsetIndex:
            case ReadStage::MainOffsetIndex:
                reader.decodeOffsetIndex(column, row_group);
                column.offset_index_prefetch.reset(&diff);
                break;
            case ReadStage::PrewhereData:
            case ReadStage::MainData:
            {
                if (!column.dictionary.isInitialized() && column.dictionary_page_prefetch)
                    reader.decodeDictionaryPage(column, column_info);
                size_t prev_page_idx = column.data_pages_idx;

                reader.decodePrimitiveColumn(
                    column, column_info, row_subgroup->columns.at(task.column_idx),
                    row_group, *row_subgroup);

                for (size_t i = prev_page_idx; i < column.data_pages_idx; ++i)
                    column.data_pages.at(i).prefetch.reset(&diff);
                break;
            }
            case ReadStage::NotStarted:
            case ReadStage::Deliver:
            case ReadStage::Deallocated:
                chassert(false);
                break;
        }
    }

    if (last_in_batch)
    {
        /// Decrement it before scheduling more tasks.
        size_t prev_batches_in_progress = stages.at(size_t(task.stage)).batches_in_progress.fetch_sub(1, std::memory_order_relaxed);
        chassert(prev_batches_in_progress > 0);
        diff.retry_scheduling_for_cur_stage = true;
    }

    switch (task.scope)
    {
        case TaskScope::ColumnChunk:
        {
            ReadStage new_stage = ReadStage(size_t(task.stage) + 1);
            row_group.columns.at(task.column_idx).stage = new_stage;
            Task new_task = task;
            new_task.stage = new_stage;
            new_task.scope = new_stage == ReadStage::BloomFilterBlocksOrDictionary
                ? TaskScope::RowGroup : TaskScope::RowSubgroup;
            Stage & new_stage_state = stages.at(size_t(new_stage));
            {
                std::unique_lock lock(new_stage_state.mutex);
                new_stage_state.tasks_to_schedule.push(new_task);
                scheduleTasksIfNeeded(new_stage, lock);
            }
            break;
        }
        case TaskScope::RowSubgroup:
        {
            size_t remaining = row_subgroup->stage_tasks_remaining.fetch_sub(1);
            chassert(remaining > 0);
            if (remaining == 1)
                finishRowSubgroupStage(task.row_group_idx, task.row_subgroup_idx, std::move(diff));
            break;
        }
        case TaskScope::RowGroup:
        {
            size_t remaining = row_group.stage_tasks_remaining.fetch_sub(1);
            chassert(remaining > 0);
            if (remaining == 1)
                finishRowGroupStage(task.row_group_idx, std::move(diff));
            break;
        }
    }
}

void ReadManager::clearColumnChunk(ColumnChunk & column, MemoryUsageDiff * diff)
{
    /// Many of these are usually cleared after the corresponding stages, but we clear them here too
    /// because stages can be skipped e.g. if the row group was filtered out by bloom filter.

    column.data_pages_prefetch.reset(diff);
    column.dictionary.reset();
    for (auto & page : column.data_pages)
        page.prefetch.reset(diff);
    column.bloom_filter_header_prefetch.reset(diff);
    column.bloom_filter_data_prefetch.reset(diff);
    column.dictionary_page_prefetch.reset(diff);
    column.column_index_prefetch.reset(diff);
    column.offset_index_prefetch.reset(diff);
    column.data_pages_prefetch.reset(diff);
    for (auto & block : column.bloom_filter_blocks)
        block.prefetch.reset(diff);

    column = {};
}

void ReadManager::clearRowSubgroup(RowSubgroup & row_subgroup, MemoryUsageDiff * diff)
{
    row_subgroup.filter.memory.reset(diff);
    row_subgroup.output.clear();
    for (ColumnSubchunk & col : row_subgroup.columns)
        col.column_and_offsets_memory.reset(diff);
}

Chunk ReadManager::read()
{
    Task task;

    Stage & stage = stages.at(size_t(ReadStage::Deliver));
    {
        std::unique_lock lock(stage.mutex);
        size_t consecutive_timeouts_with_no_running_tasks = 0;
        while (true)
        {
            if (exception)
                std::rethrow_exception(std::move(exception));

            if (!stage.tasks_to_schedule.empty())
            {
                task = stage.tasks_to_schedule.top();
                stage.tasks_to_schedule.pop();
                break;
            }

            if (first_incomplete_row_group.load(std::memory_order_relaxed) == reader.row_groups.size())
            {
                /// All done. Check for memory accounting leaks.
                for (size_t i = 0; i < stages.size(); ++i)
                {
                    size_t mem = stages[i].memory_usage.load(std::memory_order_relaxed);
                    size_t batches = stages[i].batches_in_progress.load(std::memory_order_relaxed);
                    size_t unsched = stages[i].tasks_to_schedule.size();
                    if (mem != 0 || batches != 0 || unsched != 0)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Leak in memory or task accounting in parquet reader: got {} bytes, {} batches, {} tasks in stage {}", mem, batches, unsched, i);
                }
                return {};
            }

            /// Wait for progress.
            if (parser_group->parsing_runner.isManual())
            {
                lock.unlock();
                if (!parser_group->parsing_runner.runTaskInline())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in Parquet::ReadManager");
                lock.lock();
            }
            else
            {
                auto wait_result = delivery_cv.wait_for(lock, std::chrono::seconds(10));

                if (wait_result == std::cv_status::timeout)
                {
                    /// Task scheduling code is complicated and error-prone. In particular it's easy to
                    /// have a bug where tasks stop getting scheduled under some conditions (see is_privileged_task).
                    /// So let's have this hacky check to detect if nothing is running for a while.
                    ++consecutive_timeouts_with_no_running_tasks;
                    for (const Stage & s : stages)
                    {
                        if (s.batches_in_progress.load(std::memory_order_relaxed) != 0)
                        {
                            consecutive_timeouts_with_no_running_tasks = 0;
                            break;
                        }
                    }
                    if (consecutive_timeouts_with_no_running_tasks >= 3)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Parquet task scheduling appears to be stuck");
                }
            }
        }
    }

    auto & row_subgroup = reader.row_groups.at(task.row_group_idx).subgroups.at(task.row_subgroup_idx);
    chassert(row_subgroup.stage == RowSubgroupReadStage::Deliver);
    size_t num_final_columns = reader.sample_block->columns();
    for (size_t i = 0; i < reader.output_columns.size(); ++i)
    {
        const auto & idx_in_output_block = reader.output_columns[i].idx_in_output_block;
        if (!idx_in_output_block.has_value() || idx_in_output_block.value() >= num_final_columns)
            continue;
        bool already_formed = row_subgroup.output.at(*idx_in_output_block) != nullptr;
        chassert(already_formed == reader.output_columns[i].use_prewhere);
        if (already_formed)
            continue;
        row_subgroup.output.at(*idx_in_output_block) =
            reader.formOutputColumn(row_subgroup, i);
    }
    row_subgroup.output.resize(num_final_columns); // remove prewhere-only columns
    chassert(row_subgroup.filter.rows_pass > 0);
    Chunk chunk(std::move(row_subgroup.output), row_subgroup.filter.rows_pass);

    /// This updates `memory_usage` of previous stages, which may allow more tasks to be scheduled.
    finishRowSubgroupStage(task.row_group_idx, task.row_subgroup_idx, MemoryUsageDiff(ReadStage::Deliver));

    return chunk;
}

}
