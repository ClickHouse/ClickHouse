#include <Processors/Formats/Impl/Parquet/ReadManager.h>

#include <Common/BitHelpers.h>
#include <Common/ProfileEvents.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Processors/Formats/IInputFormat.h>

#include <shared_mutex>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace ProfileEvents
{
    extern const Event ParquetDecodingTasks;
    extern const Event ParquetDecodingTaskBatches;
    extern const Event ParquetReadRowGroups;
    extern const Event ParquetPrunedRowGroups;
}

namespace DB::Parquet
{

void AtomicBitSet::resize(size_t bits)
{
    a = std::vector<std::atomic<UInt64>>((bits + 63) / 64);
}

std::optional<size_t> AtomicBitSet::findFirst()
{
    for (size_t i = 0; i < a.size(); ++i)
    {
        UInt64 x = a[i].load(std::memory_order_relaxed);
        if (x)
            return (i << 6) + getTrailingZeroBitsUnsafe(x);
    }
    return std::nullopt;
}

void ReadManager::init(FormatParserSharedResourcesPtr parser_shared_resources_)
{
    parser_shared_resources = parser_shared_resources_;
    reader.file_metadata = Reader::readFileMetaData(reader.prefetcher);
    reader.prefilterAndInitRowGroups();
    reader.preparePrewhere();

    ProfileEvents::increment(ProfileEvents::ParquetReadRowGroups, reader.row_groups.size());
    ProfileEvents::increment(ProfileEvents::ParquetPrunedRowGroups, reader.file_metadata.row_groups.size() - reader.row_groups.size());

    size_t num_row_groups = reader.row_groups.size();
    for (size_t i = size_t(ReadStage::NotStarted) + 1; i < size_t(ReadStage::Deliver); ++i)
    {
        stages[i].schedulable_row_groups.resize(num_row_groups);
        stages[i].row_group_tasks_to_schedule.resize(num_row_groups);
    }

    /// Distribute memory budget among stages.
    /// The distribution is static to make sure no stage gets starved if others eat all the memory.
    /// E.g. if the budget was shared among all stages, maybe PrewhereData could run far ahead and
    /// eat all memory, and MainData would have to execute in one thread to minimize memory usage.
    double sum = 0;
    stages[size_t(ReadStage::MainData)].memory_target_fraction *= 10;
    if (reader.format_filter_info->prewhere_info || reader.format_filter_info->row_level_filter)
        stages[size_t(ReadStage::PrewhereData)].memory_target_fraction *= 5;
    else
    {
        stages[size_t(ReadStage::PrewhereOffsetIndex)].memory_target_fraction = 0;
        stages[size_t(ReadStage::PrewhereData)].memory_target_fraction = 0;
    }
    stages[size_t(ReadStage::NotStarted)].memory_target_fraction = 0;
    stages[size_t(ReadStage::Deliver)].memory_target_fraction = 0;
    for (const Stage & stage : stages)
        sum += stage.memory_target_fraction;
    for (Stage & stage : stages)
        stage.memory_target_fraction /= sum;

    /// The NotStarted stage completed for all row groups, transition to next stage.
    MemoryUsageDiff diff(ReadStage::NotStarted);
    for (size_t i = 0; i < reader.row_groups.size(); ++i)
        finishRowGroupStage(i, ReadStage::NotStarted, diff);
    flushMemoryUsageDiff(std::move(diff));
}

ReadManager::~ReadManager()
{
    shutdown->shutdown();
}

void ReadManager::cancel() noexcept
{
    {
        std::lock_guard lock(delivery_mutex);
        if (exception)
            return;
        exception = std::make_exception_ptr(Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Cancelled"));
    }
    delivery_cv.notify_all();
}

void ReadManager::finishRowGroupStage(size_t row_group_idx, ReadStage stage, MemoryUsageDiff & diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];

    /// Finish the stage.
    if (stage == ReadStage::BloomFilterBlocksOrDictionary)
    {
        if (!reader.applyBloomAndDictionaryFilters(row_group))
            stage = ReadStage::Deliver; // skip the row group
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
        chassert(stage < ReadStage::Deallocated);
        stage = ReadStage(int(stage) + 1);

        /// Start the new stage.
        switch (stage)
        {
            case ReadStage::NotStarted:
            case ReadStage::PrewhereData:
            case ReadStage::MainOffsetIndex:
            case ReadStage::MainData:
            case ReadStage::Deliver:
                chassert(false);
                break;

            case ReadStage::BloomFilterHeader:
                for (size_t i = 0; i < row_group.columns.size(); ++i)
                    if (row_group.columns[i].bloom_filter_header_prefetch)
                        add_tasks.push_back(Task {
                            .stage = ReadStage::BloomFilterHeader,
                            .row_group_idx = row_group_idx, .column_idx = i});
                break;
            case ReadStage::BloomFilterBlocksOrDictionary:
                for (size_t i = 0; i < row_group.columns.size(); ++i)
                {
                    const auto & c = row_group.columns[i];
                    if (!c.bloom_filter_blocks.empty() || c.use_dictionary_filter)
                        add_tasks.push_back(Task {
                            .stage = ReadStage::BloomFilterBlocksOrDictionary,
                            .row_group_idx = row_group_idx, .column_idx = i});
                }
                break;
            case ReadStage::ColumnIndexAndOffsetIndex:
                for (size_t i = 0; i < row_group.columns.size(); ++i)
                    if (row_group.columns[i].use_column_index)
                        add_tasks.push_back(Task {
                            .stage = ReadStage::ColumnIndexAndOffsetIndex,
                            .row_group_idx = row_group_idx, .column_idx = i});
                break;
            case ReadStage::PrewhereOffsetIndex: // (first of the per-row-subgroup stages)
                reader.intersectColumnIndexResultsAndInitSubgroups(row_group);
                if (!row_group.subgroups.empty())
                {
                    row_group.stage.store(ReadStage::MainData);
                    row_group.stage_tasks_remaining.store(row_group.subgroups.size(), std::memory_order_relaxed);
                    /// Start the first subgroup.
                    finishRowSubgroupStage(row_group_idx, /*row_subgroup_idx=*/ 0, ReadStage::NotStarted, diff);
                    return;
                }
                /// The whole row group was filtered out.
                stage = ReadStage::Deliver;
                break;
            case ReadStage::Deallocated:
                /// We should be careful which row_group fields we access here. Other threads may
                /// still be mutating subgroups or columns. In particular, if `!subgroups.empty()`,
                /// clearColumnChunk is called by finishRowSubgroupStage (after all subgroups are read),
                /// which can run in parallel with finishRowGroupStage (after all subgroups are delivered).
                /// It may be tempting to do things like `row_group.subgroups.clear()`, but we can't,
                /// not without adding some mutexes.
                if (row_group.subgroups.empty())
                {
                    for (auto & c : row_group.columns)
                        clearColumnChunk(c, diff);
                }
                break;
        }

        if (!add_tasks.empty() || stage == ReadStage::Deallocated)
            break;

        /// Nothing needs to be done for this stage, skip to next stage.
    }

    row_group.stage.store(stage);
    row_group.stage_tasks_remaining.store(add_tasks.size(), std::memory_order_relaxed);

    if (stage == ReadStage::Deallocated)
    {
        size_t i = first_incomplete_row_group.load();
        while (i < reader.row_groups.size() && reader.row_groups[i].stage.load() == ReadStage::Deallocated)
        {
            if (first_incomplete_row_group.compare_exchange_weak(i, i + 1))
            {
                diff.scheduleAllStages();

                /// Notify read() if everything is done or if it's relying on
                /// first_incomplete_row_group to deliver chunks in order.
                if (i + 1 == reader.row_groups.size() || reader.options.format.parquet.preserve_order)
                {
                    {
                        /// Lock and unlock to avoid race condition on condition variable.
                        /// (Otherwise the notify_all() may happen after read() saw the old
                        ///  first_incomplete_row_group value but before it started waiting
                        ///  on delivery_cv.)
                        std::lock_guard lock(delivery_mutex);
                    }
                    delivery_cv.notify_all();
                }
            }
        }
    }

    if (!add_tasks.empty())
        setTasksToSchedule(row_group_idx, stage, std::move(add_tasks), diff);
}

void ReadManager::setTasksToSchedule(size_t row_group_idx, ReadStage stage, std::vector<Task> add_tasks, MemoryUsageDiff & diff)
{
    chassert(!add_tasks.empty());
    Stage & stage_state = stages.at(size_t(stage));
    auto & tasks = stage_state.row_group_tasks_to_schedule.at(row_group_idx);
    chassert(tasks.empty());
    tasks = std::move(add_tasks);
    bool changed = stage_state.schedulable_row_groups.set(row_group_idx, std::memory_order_release);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
    chassert(changed);
    diff.scheduleStage(stage);
}

void ReadManager::addTasksToReadColumns(size_t row_group_idx, size_t row_subgroup_idx, ReadStage stage, MemoryUsageDiff & diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];
    RowSubgroup & row_subgroup = row_group.subgroups[row_subgroup_idx];
    std::vector<Task> add_tasks;

    while (true) // offset index, then data
    {
        bool is_prewhere = stage == ReadStage::PrewhereOffsetIndex || stage == ReadStage::PrewhereData;
        bool is_offset_index = stage == ReadStage::PrewhereOffsetIndex || stage == ReadStage::MainOffsetIndex;

        for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
        {
            if (reader.primitive_columns[i].use_prewhere != is_prewhere)
                continue;
            ColumnChunk & c = row_group.columns.at(i);
            if (is_offset_index)
            {
                if (c.offset_index_prefetch && c.offset_index.page_locations.empty())
                {
                    /// If offset index for this column wasn't read by previous stages, make a task
                    /// to read it before reading data.
                    add_tasks.push_back(Task {
                        .stage = is_prewhere ? ReadStage::PrewhereOffsetIndex : ReadStage::MainOffsetIndex,
                        .row_group_idx = row_group_idx,
                        .row_subgroup_idx = row_subgroup_idx,
                        .column_idx = i});
                }
            }
            else
            {
                add_tasks.push_back(Task {
                    .stage = is_prewhere ? ReadStage::PrewhereData : ReadStage::MainData,
                    .row_group_idx = row_group_idx,
                    .row_subgroup_idx = row_subgroup_idx,
                    .column_idx = i});
            }
        }

        if (add_tasks.empty() && is_offset_index)
        {
            /// Don't need to read offset index, move on to next stage (PrewhereData or MainData).
            stage = ReadStage(size_t(stage) + 1);
            continue;
        }

        if (add_tasks.empty())
            /// If we don't need to read any columns, add a task that will just call finishRowGroupStage().
            /// (Why go through the task queue instead of skipping the stage at this function's call site?
            ///  Because (a) less code this way, (b) to make memory usage limiting for PREWHERE filter mask
            ///  (RowSubgroup.filter.memory) work correctly when PREWHERE expression doesn't use any
            ///  columns (note: the expression may still be nontrivial, e.g. `rand()%2=0`).)
            add_tasks.push_back(Task {
                .stage = is_prewhere ? ReadStage::PrewhereData : ReadStage::MainData,
                .row_group_idx = row_group_idx,
                .row_subgroup_idx = row_subgroup_idx,
                .column_idx = UINT64_MAX});

        ReadStage prev_stage = row_subgroup.stage.exchange(stage, std::memory_order_relaxed);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(prev_stage <= stage);
        row_subgroup.stage_tasks_remaining.store(add_tasks.size(), std::memory_order_relaxed);
        setTasksToSchedule(row_group_idx, stage, std::move(add_tasks), diff);

        break;
    }
}

void ReadManager::finishRowSubgroupStage(size_t row_group_idx, size_t row_subgroup_idx, ReadStage stage, MemoryUsageDiff & diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];
    RowSubgroup & row_subgroup = row_group.subgroups[row_subgroup_idx];
    std::optional<size_t> advanced_read_ptr;

    if (stage == ReadStage::PrewhereOffsetIndex || stage == ReadStage::MainOffsetIndex)
    {
        addTasksToReadColumns(row_group_idx, row_subgroup_idx, ReadStage(size_t(stage) + 1), diff);
        return;
    }

    switch (stage)
    {
        case ReadStage::NotStarted:
            if (!reader.prewhere_steps.empty())
            {
                /// Start prewhere.
                addTasksToReadColumns(row_group_idx, row_subgroup_idx, ReadStage::PrewhereOffsetIndex, diff);
                return;
            }

            /// No prewhere.
            chassert(row_subgroup_idx == 0);
            row_group.prewhere_ptr.store(row_group.subgroups.size(), std::memory_order_relaxed);
            break; // proceed to advancing read_ptr (because we moved prewhere_ptr)
        case ReadStage::PrewhereData:
        {
            chassert(!reader.prewhere_steps.empty());
            reader.applyPrewhere(row_subgroup);
            size_t prev = row_group.prewhere_ptr.exchange(row_subgroup_idx + 1);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
            chassert(prev == row_subgroup_idx);
            if (row_subgroup_idx + 1 < row_group.subgroups.size())
            {
                /// Can start prewhere in next subgroup.
                addTasksToReadColumns(row_group_idx, row_subgroup_idx + 1, ReadStage::PrewhereOffsetIndex, diff);
            }
            else
            {
                /// Finished prewhere in all subgroups.
                for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                    if (reader.primitive_columns[i].use_prewhere)
                        clearColumnChunk(row_group.columns.at(i), diff);
            }
            break; // proceed to advancing read_ptr (because we moved prewhere_ptr)
        }
        case ReadStage::MainData:
        {
            row_subgroup.stage.store(ReadStage::Deliver, std::memory_order::relaxed);

            /// Must add to delivery_queue before advancing read_ptr to deliver subgroups in order.
            /// (If we advanced read_ptr first, another thread could start and finish reading the
            ///  next subgroup before we add this one to delivery_queue, and ReadManager::read could
            ///  pick up the later subgroup before we add this one.)
            {
                std::lock_guard lock(delivery_mutex);
                delivery_queue.push(Task {.stage = ReadStage::Deliver, .row_group_idx = row_group_idx, .row_subgroup_idx = row_subgroup_idx});
            }

            size_t prev = row_group.read_ptr.exchange(row_subgroup_idx + 1);
            chassert(prev == row_subgroup_idx);
            advanced_read_ptr = prev + 1;
            delivery_cv.notify_one();
            break; // proceed to advancing read_ptr
        }
        case ReadStage::Deliver:
        {
            row_subgroup.stage.store(ReadStage::Deallocated);
            clearRowSubgroup(row_subgroup, diff);
            advanceDeliveryPtrIfNeeded(row_group_idx, diff);
            return;
        }
        case ReadStage::BloomFilterHeader:
        case ReadStage::BloomFilterBlocksOrDictionary:
        case ReadStage::ColumnIndexAndOffsetIndex:
        case ReadStage::PrewhereOffsetIndex:
        case ReadStage::MainOffsetIndex:
        case ReadStage::Deallocated:
            chassert(false);
            break;
    }

    /// Start reading the next row subgroup if ready.
    /// Skip subgroups that were fully filtered out by prewhere.
    size_t read_ptr = row_group.read_ptr.load();
    size_t prewhere_ptr = row_group.prewhere_ptr.load();
    while (read_ptr < row_group.subgroups.size() && read_ptr < prewhere_ptr)
    {
        RowSubgroup & next_subgroup = row_group.subgroups[read_ptr];
        ReadStage next_subgroup_stage = next_subgroup.stage.load();
        if (next_subgroup_stage >= ReadStage::MainOffsetIndex)
            break; // already reading

        if (!next_subgroup.stage.compare_exchange_strong(
                next_subgroup_stage, ReadStage::MainOffsetIndex))
            break; // another thread got here first

        if (next_subgroup.filter.rows_pass > 0)
        {
            addTasksToReadColumns(row_group_idx, read_ptr, ReadStage::MainOffsetIndex, diff);
            break;
        }

        /// Skip subgroup that was filtered out by prewhere.

        size_t prev = row_group.read_ptr.exchange(read_ptr + 1);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(prev == read_ptr);
        read_ptr += 1;
        advanced_read_ptr = read_ptr;

        next_subgroup.stage.store(ReadStage::Deallocated);
        clearRowSubgroup(next_subgroup, diff);
    }

    if (advanced_read_ptr.has_value())
    {
        advanceDeliveryPtrIfNeeded(row_group_idx, diff);

        if (*advanced_read_ptr == row_group.subgroups.size())
        {
            /// If we've read (not necessarily delivered) all subgroups, we can deallocate things
            /// like dictionary page and offset index.
            /// Only do it in the thread that has advanced row_group.read_ptr to the final value -
            /// there can only be one such thread.
            /// (I.e. avoid this race condition: one thread increments read_ptr, another thread sees the
            ///  new value, both threads call clearColumnChunk in parallel, the computer explodes.)
            /// Don't touch columns with use_prewhere == true, they're cleared by
            /// ReadStage::PrewhereData instead, which might be happening in parallel with us
            /// (but doesn't prewhere happen before MainData read? yes, but the clearColumnChunk call
            ///  happens after advancing prewhere_ptr, so another thread may do MainData+clearColumnChunk
            ///  before the thread that did prewhere is still clearing the corresponding columns).
            for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                if (!reader.primitive_columns[i].use_prewhere)
                    clearColumnChunk(row_group.columns.at(i), diff);
        }
    }
}

void ReadManager::advanceDeliveryPtrIfNeeded(size_t row_group_idx, MemoryUsageDiff & diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];
    size_t delivery_ptr = row_group.delivery_ptr.load();
    while (delivery_ptr < row_group.subgroups.size() &&
           row_group.subgroups[delivery_ptr].stage.load() == ReadStage::Deallocated)
    {
        if (!row_group.delivery_ptr.compare_exchange_weak(delivery_ptr, delivery_ptr + 1))
            continue;
        delivery_ptr += 1;
        if (delivery_ptr == row_group.subgroups.size()) // only if *this thread* incremented it
            finishRowGroupStage(row_group_idx, ReadStage::Deliver, diff);
        else if (first_incomplete_row_group.load() == row_group_idx)
            diff.scheduleAllStages();
    }
}

static bool checkTaskSchedulingLimits(size_t memory_usage, size_t added_memory, size_t batches_in_progress, size_t added_tasks, const SharedResourcesExt::Limits & limits)
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

        bool should_schedule = (diff.stages_to_schedule & (1ul << i)) != 0;
        if (!should_schedule && d < 0)
        {
            const auto & stage = stages[i];
            auto limits = SharedResourcesExt::getLimitsPerReader(*parser_shared_resources, stage.memory_target_fraction);
            should_schedule = checkTaskSchedulingLimits(
                stage.memory_usage.load(std::memory_order_relaxed), 0,
                stage.batches_in_progress.load(std::memory_order_relaxed), 0, limits);
        }
        if (should_schedule)
            scheduleTasksIfNeeded(ReadStage(i));
    }
}

void ReadManager::scheduleTasksIfNeeded(ReadStage stage_idx)
{
    chassert(stage_idx < ReadStage::Deliver);

    Stage & stage = stages.at(size_t(stage_idx));
    MemoryUsageDiff diff(stage_idx);
    std::vector<Task> tasks;

    auto limits = SharedResourcesExt::getLimitsPerReader(*parser_shared_resources, stage.memory_target_fraction);
    size_t memory_usage = stage.memory_usage.load(std::memory_order_relaxed);
    size_t batches_in_progress = stage.batches_in_progress.load(std::memory_order_relaxed);
    /// Need to be careful to avoid getting deadlocked in a situation where tasks can't be scheduled
    /// because memory usage is high, while memory usage can't decrease because tasks can't be scheduled.
    /// The way we prevent it is by always allowing scheduling tasks for the lowest-numbered
    /// <row group, row subgroup> pair that hasn't been completed (delivered or skipped) yet.
    auto is_privileged_task = [&](size_t row_group_idx)
    {
        size_t i = first_incomplete_row_group.load();
        if (row_group_idx != i)
            return false;
        const RowGroup & row_group = reader.row_groups[row_group_idx];
        return row_group.read_ptr.load() == row_group.delivery_ptr.load();
    };

    while (true)
    {
        auto row_group_maybe = stage.schedulable_row_groups.findFirst();
        if (!row_group_maybe.has_value())
            break;
        size_t row_group_idx = *row_group_maybe;

        if (!checkTaskSchedulingLimits(
                memory_usage, size_t(diff.by_stage[size_t(stage_idx)]),
                batches_in_progress, tasks.size(), limits) &&
            !is_privileged_task(row_group_idx))
            break;

        if (!stage.schedulable_row_groups.unset(row_group_idx, std::memory_order_acquire))
            continue; // another thread picked up this row group while we were checking limits

        /// Kicks off prefetches and adds their (and other) memory usage estimate to `diff`.
        auto & stage_tasks = stage.row_group_tasks_to_schedule[row_group_idx];
        chassert(!stage_tasks.empty());
        for (size_t i = 0; i < stage_tasks.size(); ++i)
            scheduleTask(stage_tasks[i], i == 0, diff, tasks);
        stage_tasks.clear();
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
        /// TODO [parquet]: Try removing this (along with cost_estimate_bytes field).
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
            funcs.push_back([this, _batch = std::move(batch), _shutdown = shutdown]
            {
                std::shared_lock shutdown_lock(*_shutdown, std::try_to_lock);
                if (!shutdown_lock.owns_lock())
                    return;
                runBatchOfTasks(_batch);
            });
        }
        stage.batches_in_progress.fetch_add(funcs.size(), std::memory_order_relaxed);
        ProfileEvents::increment(ProfileEvents::ParquetDecodingTasks, tasks.size());
        ProfileEvents::increment(ProfileEvents::ParquetDecodingTaskBatches, funcs.size());
        parser_shared_resources->parsing_runner.bulkSchedule(std::move(funcs));
    }
}

void ReadManager::scheduleTask(Task task, bool is_first_in_group, MemoryUsageDiff & diff, std::vector<Task> & out_tasks)
{
    /// Kick off prefetches and count estimated memory usage.
    std::vector<PrefetchHandle *> prefetches;
    RowGroup & row_group = reader.row_groups[task.row_group_idx];
    ssize_t memory_before = diff.by_stage[size_t(diff.cur_stage)];
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
                subchunk.column_and_offsets_memory = MemoryUsageToken(column_memory, &diff);
                break;
            }
            case ReadStage::NotStarted:
            case ReadStage::Deliver:
            case ReadStage::Deallocated:
                chassert(false);
                break;
        }
    }

    if (task.stage == ReadStage::PrewhereData && is_first_in_group)
    {
        RowSubgroup & row_subgroup = row_group.subgroups.at(task.row_subgroup_idx);
        row_subgroup.filter.memory = MemoryUsageToken(row_subgroup.filter.rows_total, &diff);
    }

    reader.prefetcher.startPrefetch(prefetches, &diff);

    /// We want to detect tiny tasks to group them together to reduce scheduling overhead.
    /// Use the predicted memory usage as a rough estimate of how long a task will take.
    /// E.g. main data read task's memory estimate consists of the input page sizes and the output
    /// column size; the run time is also roughly proportional to these sizes.
    /// Hope it's a good enough proxy in all cases.
    ssize_t memory_after = diff.by_stage[size_t(diff.cur_stage)];
    task.cost_estimate_bytes = size_t(std::max(0l, memory_after - memory_before));

    out_tasks.push_back(task);
}

void ReadManager::runBatchOfTasks(const std::vector<Task> & tasks) noexcept
{
    ReadStage stage = tasks.at(0).stage;
    size_t column_idx = UINT64_MAX;

    std::exception_ptr exc;
    try
    {
        MemoryUsageDiff diff(stage);
        for (size_t i = 0; i < tasks.size(); ++i)
        {
            chassert(tasks[i].stage == stage);
            column_idx = tasks[i].column_idx;

            runTask(tasks[i], i + 1 == tasks.size(), diff);
        }
        flushMemoryUsageDiff(std::move(diff));
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
            std::lock_guard lock(delivery_mutex);
            exception = exc;
        }
        delivery_cv.notify_all();
    }
}

void ReadManager::runTask(Task task, bool last_in_batch, MemoryUsageDiff & diff)
{
    RowGroup & row_group = reader.row_groups.at(task.row_group_idx);
    if (task.column_idx != UINT64_MAX)
    {
        ColumnChunk & column = row_group.columns.at(task.column_idx);
        const PrimitiveColumnInfo & column_info = reader.primitive_columns.at(task.column_idx);

        switch (task.stage)
        {
            case ReadStage::BloomFilterHeader: /// TODO [parquet]: do all columns in one task
                reader.processBloomFilterHeader(column, column_info);
                column.bloom_filter_header_prefetch.reset(&diff);
                break;
            case ReadStage::BloomFilterBlocksOrDictionary:
                if (column.use_dictionary_filter)
                {
                    bool ok = reader.decodeDictionaryPage(column, column_info);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
                    chassert(ok);
                }
                break;
            case ReadStage::ColumnIndexAndOffsetIndex:
                reader.decodeOffsetIndex(column, row_group);
                column.offset_index_prefetch.reset(&diff);
                reader.applyColumnIndex(column, column_info, row_group);
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
                {
                    if (!reader.decodeDictionaryPage(column, column_info))
                        column.dictionary_page_prefetch.reset(&diff);
                }
                size_t prev_page_idx = column.data_pages_idx;

                chassert(task.row_subgroup_idx != UINT64_MAX);
                RowSubgroup & row_subgroup = row_group.subgroups.at(task.row_subgroup_idx);
                reader.decodePrimitiveColumn(
                    column, column_info, row_subgroup.columns.at(task.column_idx),
                    row_group, row_subgroup);

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
        size_t prev_batches_in_progress = stages.at(size_t(task.stage)).batches_in_progress.fetch_sub(1, std::memory_order_relaxed);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(prev_batches_in_progress > 0);
        diff.scheduleStage(task.stage);
    }

    if (task.row_subgroup_idx != UINT64_MAX)
    {
        size_t remaining = row_group.subgroups.at(task.row_subgroup_idx).stage_tasks_remaining.fetch_sub(1);
        chassert(remaining > 0);
        if (remaining == 1)
            finishRowSubgroupStage(task.row_group_idx, task.row_subgroup_idx, task.stage, diff);
    }
    else
    {
        size_t remaining = row_group.stage_tasks_remaining.fetch_sub(1);
        chassert(remaining > 0);
        if (remaining == 1)
            finishRowGroupStage(task.row_group_idx, task.stage, diff);
    }
}

void ReadManager::clearColumnChunk(ColumnChunk & column, MemoryUsageDiff & diff)
{
    /// Many of these are usually cleared after the corresponding stages, but we clear them here too
    /// because stages can be skipped e.g. if the row group was filtered out by bloom filter.

    column.data_pages_prefetch.reset(&diff);
    column.dictionary.reset();
    for (auto & page : column.data_pages)
        page.prefetch.reset(&diff);
    column.bloom_filter_header_prefetch.reset(&diff);
    column.bloom_filter_data_prefetch.reset(&diff);
    column.dictionary_page_prefetch.reset(&diff);
    column.column_index_prefetch.reset(&diff);
    column.offset_index_prefetch.reset(&diff);
    column.data_pages_prefetch.reset(&diff);
    for (auto & block : column.bloom_filter_blocks)
        block.prefetch.reset(&diff);

    column = {};
}

void ReadManager::clearRowSubgroup(RowSubgroup & row_subgroup, MemoryUsageDiff & diff)
{
    row_subgroup.filter.clear(&diff);
    row_subgroup.output.clear();
    for (ColumnSubchunk & col : row_subgroup.columns)
        col.column_and_offsets_memory.reset(&diff);
}

ReadManager::ReadResult ReadManager::read()
{
    Task task;
    {
        std::unique_lock lock(delivery_mutex);

        while (true)
        {
            bool thread_pool_was_idle = parser_shared_resources->parsing_runner.isIdle();

            if (exception)
                std::rethrow_exception(exception);

            /// If `preserve_order`, only deliver chunks from `first_incomplete_row_group`.
            /// This ensures that row groups are delivered in order. Within a row group, row
            /// subgroups are read and added to `delivery_queue` in order.
            if (!delivery_queue.empty() &&
                (!reader.options.format.parquet.preserve_order ||
                 delivery_queue.top().row_group_idx ==
                    first_incomplete_row_group.load(std::memory_order_relaxed)))
            {
                task = delivery_queue.top();
                delivery_queue.pop();
                break;
            }

            if (first_incomplete_row_group.load(std::memory_order_relaxed) == reader.row_groups.size())
            {
                /// All done. Check for memory accounting leaks.
                /// First join the threads because they might still be decrementing memory_usage.
                lock.unlock();
                shutdown->shutdown();
                lock.lock();

                for (const RowGroup & row_group : reader.row_groups)
                {
                    chassert(row_group.stage.load(std::memory_order_relaxed) == ReadStage::Deallocated);
                    chassert(row_group.prewhere_ptr.load(std::memory_order_relaxed) == row_group.subgroups.size());
                    chassert(row_group.read_ptr.load(std::memory_order_relaxed) == row_group.subgroups.size());
                    chassert(row_group.delivery_ptr.load(std::memory_order_relaxed) == row_group.subgroups.size());
                    for (const RowSubgroup & subgroup : row_group.subgroups)
                        chassert(subgroup.stage.load(std::memory_order_relaxed) == ReadStage::Deallocated);
                }
                for (size_t i = 0; i < stages.size(); ++i)
                {
                    size_t mem = stages[i].memory_usage.load(std::memory_order_relaxed);
                    size_t batches = stages[i].batches_in_progress.load(std::memory_order_relaxed);
                    size_t unsched = 0;
                    for (const auto & tasks : stages[i].row_group_tasks_to_schedule)
                        unsched += tasks.size();
                    if (mem != 0 || batches != 0 || unsched != 0)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Leak in memory or task accounting in parquet reader: got {} bytes, {} batches, {} tasks in stage {}", mem, batches, unsched, i);
                }
                return {};
            }

            if (parser_shared_resources->parsing_runner.isManual())
            {
                /// Pump the manual executor.
                lock.unlock();
                /// Note: the executor can be shared among multiple files, so we may execute someone
                /// else's task, and someone else may execute our task.
                /// Hence the thread_pool_was_idle check.
                if (!parser_shared_resources->parsing_runner.runTaskInline() && thread_pool_was_idle)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in Parquet::ReadManager (single-threaded)");
                lock.lock();
            }
            else if (thread_pool_was_idle)
            {
                /// Task scheduling code is complicated and error-prone. In particular it's easy to
                /// have a bug where tasks stop getting scheduled under some conditions
                /// (see is_privileged_task). So we specifically check for getting stuck.
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in Parquet::ReadManager (thread pool)");
            }
            else
            {
                /// Wait for progress. Re-check parsing_runner.isIdle() every few seconds.
                delivery_cv.wait_for(lock, std::chrono::seconds(10));
            }
        }
    }

    RowGroup & row_group = reader.row_groups.at(task.row_group_idx);
    RowSubgroup & row_subgroup = row_group.subgroups.at(task.row_subgroup_idx);
    chassert(row_subgroup.stage == ReadStage::Deliver);
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
            reader.formOutputColumn(row_subgroup, i, row_subgroup.filter.rows_pass);
    }
    row_subgroup.output.resize(num_final_columns); // remove prewhere-only columns
    chassert(row_subgroup.filter.rows_pass > 0);
    Chunk chunk(std::move(row_subgroup.output), row_subgroup.filter.rows_pass);
    BlockMissingValues block_missing_values = std::move(row_subgroup.block_missing_values);

    auto row_numbers_info = std::make_shared<ChunkInfoRowNumbers>(
        row_subgroup.start_row_idx + row_group.start_global_row_idx);
    if (row_subgroup.filter.rows_pass != row_subgroup.filter.rows_total)
    {
        chassert(row_subgroup.filter.rows_pass > 0);
        chassert(!row_subgroup.filter.filter.empty());
        chassert(std::accumulate(row_subgroup.filter.filter.begin(), row_subgroup.filter.filter.end(), size_t(0)) == chunk.getNumRows());

        row_numbers_info->applied_filter = std::move(row_subgroup.filter.filter);
    }
    chunk.getChunkInfos().add(std::move(row_numbers_info));

    /// This is a terrible hack to make progress indication kind of work.
    ///
    /// TODO: Fix progress bar in many ways:
    ///        1. use number of rows instead of bytes;
    ///           don't lie about number of bytes read (getApproxBytesReadForChunk()),
    ///        2. estimate total rows to read after filtering row groups;
    ///           for rows filtered out by PREWHERE, either report them as read or reduce the
    ///           estimate of number of rows to read (make it signed),
    ///        3. report uncompressed deserialized IColumn bytes instead of file bytes, for
    ///           consistency with MergeTree reads,
    ///        4. correctly extrapolate progress when reading many files in sequence, e.g.
    ///           file('part{1..1000}.parquet'),
    ///        5. correctly merge progress info when a query reads both from MergeTree and files, or
    ///           parquet and text files.
    ///       Probably get rid of getApproxBytesReadForChunk() and use the existing
    ///       ISource::progress()/addTotalRowsApprox instead.
    ///       For (4) and (5), either add things to struct Progress or make progress bar use
    ///       ProfileEvents instead of Progress.
    size_t virtual_bytes_read = size_t(row_group.meta->total_compressed_size) * row_subgroup.filter.rows_total / std::max(size_t(1), size_t(row_group.meta->num_rows));

    /// This updates `memory_usage` of previous stages, which may allow more tasks to be scheduled.
    MemoryUsageDiff diff(ReadStage::Deliver);
    finishRowSubgroupStage(task.row_group_idx, task.row_subgroup_idx, ReadStage::Deliver, diff);
    flushMemoryUsageDiff(std::move(diff));

    return {std::move(chunk), std::move(block_missing_values), virtual_bytes_read};
}

}
