#include <Processors/Formats/Impl/Parquet/ReadManager.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::Parquet
{

void ReadManager::init(SharedParsingThreadPoolPtr thread_pool_)
{
    //TODO: distribute memory_target_fraction more carefully: 0 for skipped stages, higher for prewhere
    double sum = 0;
    stages[size_t(ParsingStage::ParsingRemainingColumns)].memory_target_fraction *= 5;
    for (size_t i = 0; i < stages.size(); ++i)
        sum += stages[i].memory_target_fraction;
    for (size_t i = 0; i < stages.size(); ++i)
        stages[i].memory_target_fraction /= sum;

    thread_pool = thread_pool_;
    reader.file_metadata = Reader::readFileMetaData(reader.prefetcher);
    reader.prefilterAndInitRowGroups();

    /// The NotStarted stage completed for all row groups, transition to next stage.
    for (size_t i = 0; i < reader.row_groups.size(); ++i)
        finishRowGroupStage(i, MemoryUsageDiff(ParsingStage::NotStarted));
}

ReadManager::~ReadManager()
{
    shutdown->shutdown();
}

void ReadManager::finishRowGroupStage(size_t row_group_idx, MemoryUsageDiff && diff)
{
    RowGroup & row_group = reader.row_groups[row_group_idx];
    ParsingStage stage = row_group.stage.load(std::memory_order_relaxed);
    chassert(stage == diff.cur_stage);

    /// Finish the stage.
    switch (stage)
    {
        case ParsingStage::NotStarted:
        case ParsingStage::Delivering:
            break;
        case ParsingStage::ApplyingBloomFilters:
            //TODO: eval KeyCondition
            for (auto & c : row_group.columns)
                c.bloom_filter_prefetch.reset(&diff);
            break;
        case ParsingStage::ApplyingColumnIndex:
            //TODO: intersect RowSet-s
            break;
        case ParsingStage::ParsingPrewhereColumns:
            //TODO: run PREWHERE
            break;
        case ParsingStage::ApplyingOffsetIndex:
            break;
        case ParsingStage::ParsingRemainingColumns:
            break;
        default:
            chassert(false);
    }

    /// Determine what stage to transition to and which columns are involved.
    std::vector<size_t> add_tasks_for_columns;
    while (true)
    {
        if (row_group.filter.rows_pass == 0 || stage == ParsingStage::Delivering)
        {
            stage = ParsingStage::Deallocated;

            row_group.filter.clear(&diff);
            for (auto & column : row_group.columns)
            {
                column.column_and_offsets_memory.reset(&diff);
                /// (Unlike other prefetch handles, this one is not reset by the same stage that
                /// starts it, so it may still be active at this point.)
                column.offset_index_prefetch.reset(&diff);
            }
            row_group.columns.clear();
            row_group.columns.shrink_to_fit();

            break;
        }

        chassert(stage < ParsingStage::Delivering);
        stage = ParsingStage(int(stage) + 1);

        if (stage == ParsingStage::Delivering)
            break;

        switch (stage)
        {
            case ParsingStage::ApplyingBloomFilters:
                for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                    if (row_group.columns[i].bloom_filter_prefetch)
                        add_tasks_for_columns.push_back(i);
                break;
            case ParsingStage::ApplyingColumnIndex:
                for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                    if (row_group.columns[i].column_index_prefetch)
                        add_tasks_for_columns.push_back(i);
                break;
            case ParsingStage::ParsingPrewhereColumns:
                for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                    if (reader.primitive_columns[i].use_prewhere)
                        add_tasks_for_columns.push_back(i);
                break;
            case ParsingStage::ApplyingOffsetIndex:
                if (row_group.filter.rows_pass < row_group.filter.rows_total)
                {
                    for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                        if (row_group.columns[i].offset_index.page_locations.empty() &&
                            row_group.columns[i].offset_index_prefetch)
                        {
                            add_tasks_for_columns.push_back(i);
                        }
                }
                break;
            case ParsingStage::ParsingRemainingColumns:
                for (size_t i = 0; i < reader.primitive_columns.size(); ++i)
                    if (!reader.primitive_columns[i].use_prewhere)
                        add_tasks_for_columns.push_back(i);
                break;
            default:
                chassert(false);
        }

        if (!add_tasks_for_columns.empty())
            break;

        /// Nothing needs to be done for this stage, skip to next stage.
    }

    row_group.stage.store(stage, std::memory_order_relaxed);

    /// If we freed some memory, see if more tasks need to be scheduled.
    flushMemoryUsageDiff(std::move(diff));

    /// Enqueue tasks for next stage.
    Stage & stage_state = stages.at(size_t(stage));
    if (stage < ParsingStage::Delivering)
    {
        std::unique_lock lock(stage_state.mutex);

        chassert(!add_tasks_for_columns.empty());

        for (size_t column_idx : add_tasks_for_columns)
            stage_state.tasks_to_schedule.push(Task {
                .stage = stage, .row_group_idx = row_group_idx, .column_idx = column_idx});

        row_group.stage_tasks_remaining.store(add_tasks_for_columns.size());

        scheduleTasksIfNeeded(stage, lock);
    }
    else if (stage == ParsingStage::Delivering)
    {
        chassert(add_tasks_for_columns.empty());
        row_group.rows_per_chunk = reader.decideNumRowsPerChunk(row_group);
        {
            std::unique_lock lock(delivery_mutex);
            deliverable_row_groups.push_back(row_group_idx);
        }
        delivery_cv.notify_one();
    }
    else
    {
        chassert(stage == ParsingStage::Deallocated);
        {
            std::unique_lock lock(delivery_mutex);

            /// If all row groups are Deallocated, wake up read().
            while (first_incomplete_row_group < reader.row_groups.size() &&
                   reader.row_groups[first_incomplete_row_group].stage.load(std::memory_order_relaxed) == ParsingStage::Deallocated)
            {
                first_incomplete_row_group += 1;
            }
            if (first_incomplete_row_group == reader.row_groups.size())
                delivery_cv.notify_one();
        }
    }
}

void ReadManager::flushMemoryUsageDiff(MemoryUsageDiff && diff)
{
    for (size_t i = 0; i < diff.by_stage.size(); ++i)
    {
        ssize_t d = diff.by_stage[i];
        if (d == 0)
            continue;
        stages[i].memory_usage.fetch_add(d, std::memory_order_relaxed);

        if (d < 0 && stages[i].memory_usage.load(std::memory_order_relaxed) <= thread_pool->getMemoryTargetPerReader() * stages[i].memory_target_fraction)
        {
            std::unique_lock lock(stages[i].mutex);
            scheduleTasksIfNeeded(ParsingStage(i), lock);
        }
    }
}

void ReadManager::scheduleTasksIfNeeded(ParsingStage stage_idx, std::unique_lock<std::mutex> & /*stage_lock*/)
{
    chassert(stage_idx < ParsingStage::Delivering);

    Stage & stage = stages.at(size_t(stage_idx));
    MemoryUsageDiff diff(stage_idx);
    size_t memory_target = size_t(stage.memory_target_fraction * thread_pool->getMemoryTargetPerReader());
    /// Need to be careful to avoid getting deadlocked in a situation where tasks can't be scheduled
    /// because memory usage is high, while memory usage can't decrease because tasks can't be scheduled.
    /// In particular:
    ///  * If memory target is smaller than a row group, we'll still allow scheduling at least
    ///    one task per stage (by checking `memory_usage` that doesn't include the new task's memory).
    ///  * We schedule tasks for all columns of a row group together, even if a subset of them is
    ///    enough to exceed the memory target. Otherwise we may get stuck in a situation where the
    ///    already-read columns are holding memory and preventing any tasks from being scheduled in
    ///    this stage indefinitely. If we schedule the whole row group, it guarantees that the row
    ///    group will be able to advance to the next stage. (Advancing to the next stage doesn't
    ///    necessarily free up memory in this stage. But, by induction, the row group will be able
    ///    to proceed through later stages too, all the way to delivery, at which point it's
    ///    guaranteed to release its memory from all stages.)
    ///  * Can two (or more) row groups both block each other by holding memory in different `Stage`s?
    ///    No, because row group's stage never decreases. At least one row group with the highest
    ///    stage will be able to schedule a task.
    while (!stage.tasks_to_schedule.empty() &&
           stage.memory_usage.load(std::memory_order_relaxed) + diff.by_stage[size_t(stage_idx)] <= memory_target)
    {
        size_t row_group_idx = stage.tasks_to_schedule.front().row_group_idx;
        do
        {
            Task task = stage.tasks_to_schedule.front();
            stage.tasks_to_schedule.pop();
            /// Kicks off prefetches and adds their (and other) memory usage estimate to `diff`.
            scheduleTask(task, &diff);
        }
        while (!stage.tasks_to_schedule.empty() &&
               stage.tasks_to_schedule.front().row_group_idx == row_group_idx);
    }

    for (size_t i = 0; i < diff.by_stage.size(); ++i)
    {
        chassert(diff.by_stage[i] >= 0); // scheduleTask doesn't do significant deallocations
        if (diff.by_stage[i] != 0)
            stages[i].memory_usage.fetch_add(diff.by_stage[i], std::memory_order_relaxed);
    }
}

void ReadManager::scheduleTask(Task task, MemoryUsageDiff * diff)
{
    /// Kick off prefetches and count estimated memory usage.
    std::vector<Prefetcher::RequestHandle *> prefetches;
    RowGroup & row_group = reader.row_groups[task.row_group_idx];
    ColumnChunk & column = row_group.columns.at(task.column_idx);
    switch (task.stage)
    {
        case ParsingStage::ApplyingBloomFilters:
            prefetches.push_back(&column.bloom_filter_prefetch);
            break;
        case ParsingStage::ApplyingColumnIndex:
            prefetches.push_back(&column.column_index_prefetch);
            prefetches.push_back(&column.offset_index_prefetch);
            column.partial_filter.memory = MemoryUsageToken(row_group.meta->num_rows, diff);
            break;
        case ParsingStage::ApplyingOffsetIndex:
            prefetches.push_back(&column.offset_index_prefetch);
            break;
        case ParsingStage::ParsingPrewhereColumns:
        case ParsingStage::ParsingRemainingColumns:
        {
            //TODO: split into pages
            prefetches.push_back(&column.data_prefetch);
            size_t column_memory = reader.estimateColumnMemoryUsage(column);
            column.column_and_offsets_memory = MemoryUsageToken(column_memory, diff);
            break;
        }
        case ParsingStage::Delivering:
        case ParsingStage::Deallocated:
            break;
        case ParsingStage::NotStarted:
        case ParsingStage::COUNT:
            chassert(false);
            break;
    }
    reader.prefetcher.startPrefetch(prefetches, diff);

    thread_pool->parsing_runner([this, task]
        {
            std::shared_lock shutdown_lock(*shutdown, std::try_to_lock);
            if (!shutdown_lock.owns_lock())
                return;
            runTask(task);
        });
}

void ReadManager::runTask(Task task)
{
    try
    {
        MemoryUsageDiff diff(task.stage);

        RowGroup & row_group = reader.row_groups.at(task.row_group_idx);
        ColumnChunk & column = row_group.columns.at(task.column_idx);
        const PrimitiveColumnInfo & column_info = reader.primitive_columns.at(task.column_idx);

        switch (task.stage)
        {
            case ParsingStage::ApplyingBloomFilters:
                //TODO
                break;
            case ParsingStage::ApplyingColumnIndex:
                //TODO
                column.column_index_prefetch.reset(&diff);
                break;
            case ParsingStage::ApplyingOffsetIndex:
                //TODO
                column.offset_index_prefetch.reset(&diff);
                break;
            case ParsingStage::ParsingPrewhereColumns:
            case ParsingStage::ParsingRemainingColumns:
                reader.parsePrimitiveColumn(column, column_info, row_group.filter);
                column.data_prefetch.reset(&diff);
                for (auto & page : column.pages)
                    page.prefetch.reset(&diff);
                column.pages.clear();
                column.pages.shrink_to_fit();
                break;
            case ParsingStage::NotStarted:
            case ParsingStage::Delivering:
            case ParsingStage::Deallocated:
            case ParsingStage::COUNT:
                chassert(false);
                break;
        }

        size_t remaining = reader.row_groups[task.row_group_idx].stage_tasks_remaining.fetch_sub(1);
        chassert(remaining > 0);
        if (remaining == 1)
            finishRowGroupStage(task.row_group_idx, std::move(diff));
        else
            flushMemoryUsageDiff(std::move(diff));
    }
    catch (...)
    {
        {
            std::lock_guard lock(delivery_mutex);
            exception = std::current_exception();
        }
        delivery_cv.notify_all();
    }
}

Chunk ReadManager::read()
{
    if (reader.row_groups.empty())
        /// All row groups were filtered out just from file metadata.
        return {};

    size_t row_group_idx;
    RowGroup * row_group;
    size_t start_row;
    size_t num_rows;
    {
        std::unique_lock lock(delivery_mutex);
        while (true)
        {
            if (exception)
                std::rethrow_exception(std::move(exception));

            /// See if anything can be delivered.
            if (!deliverable_row_groups.empty())
            {
                row_group_idx = deliverable_row_groups.front();
                row_group = &reader.row_groups[row_group_idx];

                /// Bite off a chunk.
                start_row = row_group->rows_delivered;
                num_rows = std::min(row_group->rows_per_chunk, row_group->filter.rows_pass - row_group->rows_delivered);
                chassert(num_rows > 0);
                row_group->rows_delivered += num_rows;
                if (row_group->rows_delivered == row_group->filter.rows_pass)
                    deliverable_row_groups.pop_front();

                break;
            }

            /// See if all row groups finished all stages.
            if (first_incomplete_row_group == reader.row_groups.size())
            {
                for (const auto & s : stages)
                    chassert(s.memory_usage.load() == 0);
                return {};
            }

            /// Wait for progress.
            if (thread_pool->parsing_runner.isManual())
            {
                lock.unlock();
                if (!thread_pool->parsing_runner.runTaskInline())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Deadlock in Parquet::ReadManager");
                lock.lock();
            }
            else
            {
                delivery_cv.wait(lock);
            }
        }
    }

    Columns columns(reader.sample_block->columns());
    for (size_t i = 0; i < reader.output_columns.size(); ++i)
    {
        const auto & idx_in_output_block = reader.output_columns[i].idx_in_output_block;
        if (!idx_in_output_block.has_value())
            continue;
        columns.at(idx_in_output_block.value()) =
            reader.formOutputColumn(*row_group, i, start_row, num_rows);
    }
    Chunk chunk(std::move(columns), num_rows);

    if (row_group->rows_delivered == row_group->filter.rows_pass)
        /// Deallocate.
        finishRowGroupStage(row_group_idx, MemoryUsageDiff(ParsingStage::Delivering));

    return chunk;
}

}
