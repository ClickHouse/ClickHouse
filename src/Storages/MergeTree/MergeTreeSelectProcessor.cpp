#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

#include <Columns/ColumnLazy.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSplitPrewhereIntoReadSteps.h>

namespace
{

template <typename Func>
struct TelemetryWrapper
{
    TelemetryWrapper(Func callback_, ProfileEvents::Event event_, std::string span_name_)
        : callback(std::move(callback_)), event(event_), span_name(std::move(span_name_))
    {
    }

    template <typename... Args>
    auto operator()(Args &&... args)
    {
        DB::OpenTelemetry::SpanHolder span(span_name);
        DB::ProfileEventTimeIncrement<DB::Time::Microseconds> increment(event);
        return callback(std::forward<Args>(args)...);
    }

private:
    Func callback;
    ProfileEvents::Event event;
    std::string span_name;
};

}

namespace ProfileEvents
{
extern const Event ParallelReplicasAnnouncementMicroseconds;
extern const Event ParallelReplicasReadRequestMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

ParallelReadingExtension::ParallelReadingExtension(
    MergeTreeAllRangesCallback all_callback_,
    MergeTreeReadTaskCallback callback_,
    size_t number_of_current_replica_,
    size_t total_nodes_count_)
    : number_of_current_replica(number_of_current_replica_), total_nodes_count(total_nodes_count_)
{
    all_callback = TelemetryWrapper<MergeTreeAllRangesCallback>{
        std::move(all_callback_), ProfileEvents::ParallelReplicasAnnouncementMicroseconds, "ParallelReplicasAnnouncement"};

    callback = TelemetryWrapper<MergeTreeReadTaskCallback>{
        std::move(callback_), ProfileEvents::ParallelReplicasReadRequestMicroseconds, "ParallelReplicasReadRequest"};
}

void ParallelReadingExtension::sendInitialRequest(CoordinationMode mode, RangesInDataPartsDescription description, size_t mark_segment_size, size_t min_marks_per_request) const
{
    all_callback(InitialAllRangesAnnouncement{mode, std::move(description), number_of_current_replica, mark_segment_size, min_marks_per_request});
}

std::optional<ParallelReadResponse> ParallelReadingExtension::sendReadRequest(
    CoordinationMode mode, size_t min_marks_per_request, const RangesInDataPartsDescription & description) const
{
    return callback(ParallelReadRequest{mode, number_of_current_replica, min_marks_per_request, description});
}

MergeTreeIndexBuildContext::MergeTreeIndexBuildContext(
    RangesByIndex read_ranges_,
    ProjectionRangesByIndex projection_read_ranges_,
    MergeTreeIndexReadResultPoolPtr index_reader_pool_,
    PartRemainingMarks part_remaining_marks_)
    : read_ranges(std::move(read_ranges_))
    , projection_read_ranges(std::move(projection_read_ranges_))
    , index_reader_pool(std::move(index_reader_pool_))
    , part_remaining_marks(std::move(part_remaining_marks_))
{
    chassert(index_reader_pool);
}

MergeTreeIndexReadResultPtr MergeTreeIndexBuildContext::getPreparedIndexReadResult(const MergeTreeReadTask & task) const
{
    const auto & part_ranges = read_ranges.at(task.getInfo().part_index_in_query);
    auto it = projection_read_ranges.find(task.getInfo().part_index_in_query);
    static RangesInDataParts empty_parts_ranges;
    const auto & projection_parts_ranges = it != projection_read_ranges.end() ? it->second : empty_parts_ranges;
    auto & remaining_marks = part_remaining_marks.at(task.getInfo().part_index_in_query).value;
    auto index_read_result = index_reader_pool->getOrBuildIndexReadResult(part_ranges, projection_parts_ranges);

    /// Atomically subtract the number of marks this task will read from the total remaining marks. If the
    /// remaining marks after subtraction reach zero, this is the last task for the part, and we can trigger
    /// cleanup of any per-part cached resources (e.g., skip index read result or projection index bitmaps).
    size_t task_marks = task.getNumMarksToRead();
    bool part_last_task = remaining_marks.fetch_sub(task_marks, std::memory_order_acq_rel) == task_marks;

    if (part_last_task)
        index_reader_pool->clear(task.getInfo().data_part);

    return index_read_result;
}

MergeTreeSelectProcessor::MergeTreeSelectProcessor(
    MergeTreeReadPoolPtr pool_,
    MergeTreeSelectAlgorithmPtr algorithm_,
    const FilterDAGInfoPtr & row_level_filter_,
    const PrewhereInfoPtr & prewhere_info_,
    const IndexReadTasks & index_read_tasks_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    MergeTreeIndexBuildContextPtr merge_tree_index_build_context_,
    LazyMaterializingRowsPtr lazy_materializing_rows_)
    : pool(std::move(pool_))
    , algorithm(std::move(algorithm_))
    , row_level_filter(row_level_filter_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , prewhere_actions(getPrewhereActions(
          row_level_filter,
          prewhere_info,
          index_read_tasks_,
          actions_settings,
          reader_settings_.enable_multiple_prewhere_read_steps,
          reader_settings_.force_short_circuit_execution))
    , reader_settings(reader_settings_)
    , result_header(transformHeader(pool->getHeader(), row_level_filter, prewhere_info))
    , merge_tree_index_build_context(std::move(merge_tree_index_build_context_))
    , lazy_materializing_rows(std::move(lazy_materializing_rows_))
{
    bool has_prewhere_actions_steps = !prewhere_actions.steps.empty();
    if (has_prewhere_actions_steps)
        LOG_TEST(log, "PREWHERE condition was split into {} steps", prewhere_actions.steps.size());

    if (prewhere_info || has_prewhere_actions_steps)
        LOG_TEST(log, "PREWHERE conditions: {}, Original PREWHERE DAG:\n{}\nPREWHERE actions:\n{}",
            has_prewhere_actions_steps ? prewhere_actions.dumpConditions() : std::string("<nullptr>"),
            prewhere_info ? prewhere_info->prewhere_actions.dumpDAG() : std::string("<nullptr>"),
            has_prewhere_actions_steps ? prewhere_actions.dump() : std::string("<nullptr>"));
}

String MergeTreeSelectProcessor::getName() const
{
    return fmt::format("MergeTreeSelect(pool: {}, algorithm: {})", pool->getName(), algorithm->getName());
}

PrewhereExprInfo MergeTreeSelectProcessor::getPrewhereActions(
    const FilterDAGInfoPtr & row_level_filter,
    const PrewhereInfoPtr & prewhere_info,
    const IndexReadTasks & index_read_tasks,
    const ExpressionActionsSettings & actions_settings,
    bool enable_multiple_prewhere_read_steps,
    bool force_short_circuit_execution)
{
    PrewhereExprInfo prewhere_actions;

    if (row_level_filter)
    {
        PrewhereExprStep row_level_filter_step
        {
            .type = PrewhereExprStep::Filter,
            .actions = std::make_shared<ExpressionActions>(row_level_filter->actions.clone(), actions_settings),
            .filter_column_name = row_level_filter->column_name,
            .remove_filter_column = row_level_filter->do_remove_column,
            .need_filter = true,
            .perform_alter_conversions = true,
            .mutation_version = std::nullopt,
        };

        prewhere_actions.steps.emplace_back(std::make_shared<PrewhereExprStep>(std::move(row_level_filter_step)));
    }

    /// Add steps for reading virtual columns for indexes.
    /// Those must be separate steps, because index readers
    /// cannot read physical columns from table.
    for (const auto & [_, index_task] : index_read_tasks)
    {
        auto index_read_step = std::make_shared<PrewhereExprStep>();
        index_read_step->type = PrewhereExprStep::None;
        index_read_step->actions = std::make_shared<ExpressionActions>(ActionsDAG(index_task.columns), actions_settings);
        prewhere_actions.steps.emplace_back(std::move(index_read_step));
    }

    if (prewhere_info &&
        (!enable_multiple_prewhere_read_steps || !tryBuildPrewhereSteps(prewhere_info, actions_settings, prewhere_actions, force_short_circuit_execution)))
    {
        PrewhereExprStep prewhere_step
        {
            .type = PrewhereExprStep::Filter,
            .actions = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions.clone(), actions_settings),
            .filter_column_name = prewhere_info->prewhere_column_name,
            .remove_filter_column = prewhere_info->remove_prewhere_column,
            .need_filter = prewhere_info->need_filter,
            .perform_alter_conversions = true,
            .mutation_version = std::nullopt,
        };

        prewhere_actions.steps.emplace_back(std::make_shared<PrewhereExprStep>(std::move(prewhere_step)));
    }

    return prewhere_actions;
}

ChunkAndProgress
MergeTreeSelectProcessor::readCurrentTask(MergeTreeReadTask & current_task, IMergeTreeSelectAlgorithm & task_algorithm) const
{
    if (!current_task.getReadersChain().isInitialized())
        current_task.initializeReadersChain(prewhere_actions, merge_tree_index_build_context, lazy_materializing_rows, read_steps_performance_counters);

    auto res = task_algorithm.readFromTask(current_task);

    if (res.row_count)
    {
        /// Reorder the columns according to result_header
        Columns ordered_columns;
        ordered_columns.reserve(result_header.columns());
        for (size_t i = 0; i < result_header.columns(); ++i)
        {
            auto name = result_header.getByPosition(i).name;
            ordered_columns.push_back(res.block.getByName(name).column);
        }

        auto chunk = Chunk(ordered_columns, res.row_count);
        const auto & data_part = current_task.getInfo().data_part;
        if (add_part_level)
            chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(data_part->info.level));

        if (reader_settings.use_query_condition_cache)
        {
            String part_name
                = data_part->isProjectionPart() ? fmt::format("{}:{}", data_part->getParentPartName(), data_part->name) : data_part->name;
            chunk.getChunkInfos().add(std::make_shared<MarkRangesInfo>(
                data_part->storage.getStorageID().uuid,
                part_name,
                data_part->index_granularity->getMarksCount(),
                data_part->index_granularity->hasFinalMark(),
                res.read_mark_ranges));
        }

        return ChunkAndProgress{
            .chunk = std::move(chunk), .num_read_rows = res.num_read_rows, .num_read_bytes = res.num_read_bytes,
            .is_finished = false, .read_mark_ranges = std::move(res.read_mark_ranges)};
    }

    if (reader_settings.use_query_condition_cache && prewhere_info)
        current_task.addPrewhereUnmatchedMarks(res.read_mark_ranges);

    return {Chunk(), res.num_read_rows, res.num_read_bytes, false, std::move(res.read_mark_ranges)};
}

void MergeTreeSelectProcessor::setVirtualRowConversions(
    ExpressionActionsPtr virtual_row_conversions_, Block pk_block_header_, bool read_in_reverse_order_)
{
    virtual_row_conversions = std::move(virtual_row_conversions_);
    pk_block_header = std::move(pk_block_header_);
    read_in_reverse_order = read_in_reverse_order_;
}

ChunkAndProgress MergeTreeSelectProcessor::buildVirtualRowFromIndex(
    const MergeTreeReadTask & current_task, const MarkRanges & read_mark_ranges) const
{
    if (!virtual_row_conversions || read_mark_ranges.empty())
        return {};

    const auto & data_part = current_task.getInfo().data_part;
    const auto & index = data_part->getIndex();

    /// Forward order: the source will next produce data starting at back().end.
    /// Reverse order: MergeTreeInReverseOrderSelectAlgorithm returns chunks in reverse.
    /// After returning a chunk from marks [a, b), the next chunk covers earlier marks ending at a.
    /// So front().begin is the boundary of the next output.
    size_t next_mark = read_in_reverse_order
        ? read_mark_ranges.front().begin
        : read_mark_ranges.back().end;

    size_t num_pk_columns = pk_block_header.columns();
    if (index->size() < num_pk_columns)
        return {};

    bool has_value = std::ranges::all_of(*index, [&](const auto & col) { return col->size() > next_mark; });
    if (!has_value)
        return {};

    ColumnsWithTypeAndName pk_columns;
    pk_columns.reserve(num_pk_columns);
    for (size_t j = 0; j < num_pk_columns; ++j)
    {
        const auto & header_col = pk_block_header.getByPosition(j);
        auto column = header_col.column->cloneEmpty();
        column->insert((*(*index)[j])[next_mark]);
        pk_columns.push_back({std::move(column), header_col.type, header_col.name});
    }
    Block pk_block(std::move(pk_columns));

    Columns empty_columns;
    empty_columns.reserve(result_header.columns());
    for (size_t i = 0; i < result_header.columns(); ++i)
        empty_columns.push_back(result_header.getByPosition(i).type->createColumn()->cloneEmpty());

    Chunk chunk(std::move(empty_columns), 0);
    auto part_level = data_part->info.level;
    chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(part_level, pk_block, virtual_row_conversions));

    return {std::move(chunk), 0, 0, false, {}};
}

ChunkAndProgress MergeTreeSelectProcessor::read()
{
    if (pending_virtual_row)
    {
        auto result = std::move(*pending_virtual_row);
        pending_virtual_row.reset();
        return result;
    }

    while (!is_cancelled)
    {
        try
        {
            if (!task || algorithm->needNewTask(*task))
            {
                /// Update the query condition cache for filters in PREWHERE stage
                if (reader_settings.use_query_condition_cache && task && prewhere_info)
                {
                    for (const auto * output : prewhere_info->prewhere_actions.getOutputs())
                    {
                        if (output->result_name == prewhere_info->prewhere_column_name)
                        {
                            if (!VirtualColumnUtils::isDeterministic(output))
                                continue;

                            auto query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();
                            auto data_part = task->getInfo().data_part;

                            String part_name = data_part->isProjectionPart()
                                ? fmt::format("{}:{}", data_part->getParentPartName(), data_part->name)
                                : data_part->name;
                            query_condition_cache->write(
                                data_part->storage.getStorageID().uuid,
                                part_name,
                                output->getHash(),
                                prewhere_info->prewhere_actions.getNames()[0],
                                task->getPrewhereUnmatchedMarks(),
                                data_part->index_granularity->getMarksCount(),
                                data_part->index_granularity->hasFinalMark());

                            break;
                        }
                    }
                }

                task = algorithm->getNewTask(*pool, task.get());
            }

            if (!task)
                break;
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED || e.code() == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT)
                break;
            throw;
        }

        auto result = readCurrentTask(*task, *algorithm);

        /// Emit a virtual row update after each block, carrying the next mark's PK boundary.
        /// This allows MergingSortedTransform to reprioritize sources when:
        /// - PREWHERE filters all rows (merge gets updated position without actual data)
        /// - A downstream filter (WHERE, JOIN) removes all rows (virtual row passes through filters)
        if (virtual_row_conversions && !result.is_finished)
        {
            auto vrow = buildVirtualRowFromIndex(*task, result.read_mark_ranges);
            if (vrow.chunk)
                pending_virtual_row.emplace(std::move(vrow));
        }

        return result;
    }

    return {Chunk(), 0, 0, true, {}};
}

/// Cancels all internal operations for this select processor, including cancelling any ongoing index reads.
void MergeTreeSelectProcessor::cancel() noexcept
{
    is_cancelled = true;

    if (merge_tree_index_build_context)
        merge_tree_index_build_context->index_reader_pool->cancel();
}

Block MergeTreeSelectProcessor::transformHeader(
    Block block,
    const FilterDAGInfoPtr & row_level_filter,
    const PrewhereInfoPtr & prewhere_info)
{
    auto transformed = SourceStepWithFilter::applyPrewhereActions(std::move(block), row_level_filter, prewhere_info);
    return transformed;
}

static String dumpStatistics(const ReadStepsPerformanceCounters & counters)
{
    WriteBufferFromOwnString out;
    const auto & index_counter = counters.getIndexCounter();
    if (index_counter)
        out << fmt::format("index step rows_read: {}, ", index_counter->rows_read.load());

    const auto & all_counters = counters.getCounters();
    for (size_t i = 0; i < all_counters.size(); ++i)
    {
        out << fmt::format("step {} rows_read: {}", i, all_counters[i]->rows_read.load());
        if (i + 1 < all_counters.size())
            out << ", ";
    }
    return out.str();
}

void MergeTreeSelectProcessor::onFinish() const
{
    LOG_TEST(log, "Read steps statistics: {}", dumpStatistics(read_steps_performance_counters));
}

}
