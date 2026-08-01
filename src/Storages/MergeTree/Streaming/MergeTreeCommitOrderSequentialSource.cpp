#include <Storages/MergeTree/Streaming/MergeTreeCommitOrderSequentialSource.h>
#include <Storages/MergeTree/Streaming/StreamingChunkCursor.h>
#include <Storages/MergeTree/Streaming/CursorUtils.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>

#include <IO/WriteBufferFromString.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>

#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <algorithm>
#include <memory>

namespace DB
{

namespace
{

struct ClassifiedPartitions
{
    std::vector<std::string> readable_partitions;
};

ClassifiedPartitions getPartitionsClassification(
    const std::map<String, Int64> & safe_block_numbers,
    const std::map<String, PartitionCursor> & last_emitted_positions)
{
    ClassifiedPartitions classification;
    for (const auto & [partition_id, safe_block_number] : safe_block_numbers)
    {
        if (!last_emitted_positions.contains(partition_id))
            classification.readable_partitions.push_back(partition_id);

        else if (last_emitted_positions.at(partition_id).block_number <= safe_block_number)
            classification.readable_partitions.push_back(partition_id);
    }

    return classification;
}

std::string explainPlan(const QueryPlan & plan)
{
    WriteBufferFromOwnString plan_buffer;
    ExplainPlanOptions explain_options{.header = true, .actions = true, .indexes = true, .compact = true, .pretty = true};
    plan.explainPlan(plan_buffer, explain_options);
    return plan_buffer.str();
}

std::string explainPipeline(const Pipe & pipe)
{
    WriteBufferFromOwnString pipeline_buffer;
    printPipeline(pipe.getProcessors(), pipeline_buffer);
    return pipeline_buffer.str();
}

struct PipeWithResources
{
    Pipe pipe;
    QueryPlanResourceHolder resources;
};

/// Returns safe snapshot reading plan from the specified partition.
QueryPlanPtr buildPartitionReadingPlan(
    const String & partition_id,
    const PartitionCursor & last_emitted_position,
    const Int64 & safe_block_number,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const PrewhereInfoPtr & initial_prewhere_info,
    const ContextPtr & context,
    const Names & inner_columns,
    const size_t & requested_num_streams,
    const UInt64 & max_block_size,
    const SharedHeader & output_header)
{
    auto plan = MergeTreeDataSelectExecutor(storage).read(
        inner_columns,
        storage_snapshot,
        query_info,
        context,
        max_block_size,
        requested_num_streams,
        /*max_block_numbers_to_read=*/nullptr,
        /*enable_parallel_reading=*/false);

    if (!plan || !plan->getRootNode())
        return nullptr;

    /// Add cursor filter to read only safe snapshot.
    auto cursor_filter = buildPartitionFilter(partition_id, last_emitted_position, safe_block_number, *plan->getCurrentHeader(), context);
    plan->addStep(std::make_unique<FilterStep>(
        plan->getCurrentHeader(),
        std::move(cursor_filter.actions),
        cursor_filter.column_name,
        cursor_filter.do_remove_column));

    /// Add prewhere built from the outer query analysis.
    if (initial_prewhere_info)
    {
        plan->addStep(std::make_unique<FilterStep>(
            plan->getCurrentHeader(),
            initial_prewhere_info->prewhere_actions.clone(),
            initial_prewhere_info->prewhere_column_name,
            initial_prewhere_info->remove_prewhere_column));
    }

    /// Add commit-order sorting (_block_number, _block_offset)
    SortDescription sort_desc;
    sort_desc.emplace_back(BlockNumberColumn::name, 1);
    sort_desc.emplace_back(BlockOffsetColumn::name, 1);
    SortingStep::Settings sort_settings(context->getSettingsRef());
    plan->addStep(std::make_unique<SortingStep>(
        plan->getCurrentHeader(),
        std::move(sort_desc),
        /*limit=*/ 0,
        sort_settings,
        /*is_sorting_for_merge_join=*/false));

    /// Add cursor calculation step.
    plan->addStep(std::make_unique<BuildStreamingChunkCursorStep>(plan->getCurrentHeader()));

    /// Add projection to required header.
    auto convert = ActionsDAG::makeConvertingActions(
        plan->getCurrentHeader()->getColumnsWithTypeAndName(),
        output_header->getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);
    plan->addStep(std::make_unique<ExpressionStep>(plan->getCurrentHeader(), std::move(convert)));

    return plan;
}

Names extendWithAuxiliaryColumns(Names columns)
{
    for (const auto & aux_name : {PartitionIdColumn::name, BlockNumberColumn::name, BlockOffsetColumn::name})
        if (!std::ranges::contains(columns, aux_name))
            columns.push_back(aux_name);

    return columns;
}

std::optional<PipeWithResources> buildNextSnapshotReadingPipeline(
    const ClassifiedPartitions & partitions_classification,
    const std::map<String, Int64> & safe_block_numbers,
    const std::map<String, PartitionCursor> & last_emitted_positions,
    const MergeTreeData & storage,
    const SelectQueryInfo & query_info,
    const PrewhereInfoPtr & initial_prewhere_info,
    const ContextPtr & context,
    const Names & user_requested_columns,
    const size_t & requested_num_streams,
    const UInt64 & max_block_size,
    const SharedHeader & output_header,
    const LoggerPtr & log)
{
    LOG_DEBUG(log, "Building new snapshot for {} partition(s): {}", partitions_classification.readable_partitions.size(), partitions_classification.readable_partitions);

    /// Fresh storage snapshot reused by every per-partition subplan in this iteration.
    const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/true);
    const auto storage_snapshot = storage.getStorageSnapshot(metadata, context);
    const auto columns_to_read = extendWithAuxiliaryColumns(user_requested_columns);

    std::vector<QueryPlanPtr> per_partition_plans;
    SharedHeaders per_partition_headers;

    for (const auto & partition_id : partitions_classification.readable_partitions)
    {
        const PartitionCursor last_emitted_position = last_emitted_positions.contains(partition_id) ? last_emitted_positions.at(partition_id) : PartitionCursor{};
        const Int64 safe_block_number = safe_block_numbers.at(partition_id);

        auto plan = buildPartitionReadingPlan(
            partition_id,
            last_emitted_position,
            safe_block_number,
            storage,
            storage_snapshot,
            query_info,
            initial_prewhere_info,
            context,
            columns_to_read,
            requested_num_streams,
            max_block_size,
            output_header);

        if (plan)
        {
            per_partition_headers.push_back(plan->getCurrentHeader());
            per_partition_plans.emplace_back(std::move(plan));
        }
    }

    if (per_partition_plans.empty())
        return std::nullopt;

    QueryPlan unified;
    unified.unitePlans(std::make_unique<UnionStep>(std::move(per_partition_headers)), std::move(per_partition_plans));

    QueryPlanOptimizationSettings opt_settings(context);
    unified.optimize(opt_settings);

    LOG_TEST(log, "Unified snapshot plan:\n{}", explainPlan(unified));

    auto builder = unified.buildQueryPipeline(opt_settings, BuildQueryPipelineSettings(context), /*do_optimize=*/false);

    PipeWithResources result;
    result.pipe = QueryPipelineBuilder::getPipe(std::move(*builder), result.resources);
    result.pipe.resize(1);

    LOG_TEST(log, "Unified snapshot pipeline:\n{}", explainPipeline(result.pipe));

    return result;
}

ContextPtr makeStreamingContext(ContextPtr context_)
{
    auto copy = Context::createCopy(context_);
    copy->makeQueryContext();
    copy->setQueryMetadataCache(nullptr);
    return copy;
}

SelectQueryInfo makeStreamingSelectQueryInfo(SelectQueryInfo info)
{
    info.table_expression_modifiers = std::nullopt;
    info.merge_tree_enable_remove_parts_from_snapshot_optimization = false;

    info.prewhere_info.reset();
    info.filter_actions_dag.reset();

    info.order_optimizer.reset();
    info.input_order_info.reset();

    info.trivial_limit = 0;
    info.optimize_trivial_count = false;

    info.has_window = false;
    info.has_order_by = false;
    info.need_aggregate = false;
    info.has_aggregates = false;

    return info;
}

PrewhereInfoPtr makeStreamingPrewhereInfo(PrewhereInfoPtr info)
{
    if (!info)
        return nullptr;

    auto patched_info = std::make_shared<PrewhereInfo>(info->clone());

    /// These columns are needed for cursor calculation.
    patched_info->prewhere_actions.tryRestoreColumn(PartitionIdColumn::name);
    patched_info->prewhere_actions.tryRestoreColumn(BlockNumberColumn::name);
    patched_info->prewhere_actions.tryRestoreColumn(BlockOffsetColumn::name);

    return patched_info;
}

}

MergeTreeCommitOrderSequentialSource::MergeTreeCommitOrderSequentialSource(
    SharedHeader header_,
    const MergeTreeData & storage_,
    const SelectQueryInfo & query_info_,
    ContextPtr context_,
    Names user_requested_columns_,
    size_t requested_num_streams_,
    UInt64 max_block_size_,
    MergeTreeBoundsSubscriptionPtr subscription_)
    : IProcessor({}, {Block(*header_)})
    , header(std::move(header_))
    , storage(storage_)
    , query_info(makeStreamingSelectQueryInfo(query_info_))
    , initial_prewhere_info(makeStreamingPrewhereInfo(query_info_.prewhere_info))
    , context(makeStreamingContext(std::move(context_)))
    , user_requested_columns(std::move(user_requested_columns_))
    , requested_num_streams(requested_num_streams_)
    , max_block_size(max_block_size_)
    , subscription(std::move(subscription_))
    , log(getLogger("MergeTreeCommitOrderSequentialSource"))
    , last_emitted_positions(buildMergeTreeCursor(query_info_.table_expression_modifiers->getStreamSettings()->cursor))
{
}

IProcessor::Status MergeTreeCommitOrderSequentialSource::handleRunningPipeline()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    auto chunk = input.pull(/*set_not_needed=*/true);
    chassert(!chunk.hasRows() || chunk.getChunkInfos().has<StreamingChunkCursorInfo>());

    if (auto cursor = chunk.getChunkInfos().extract<StreamingChunkCursorInfo>())
    {
        auto & position = last_emitted_positions[cursor->partition_id];
        position.block_number = cursor->last_block_number;
        position.block_offset = cursor->last_block_offset;
        LOG_TEST(log, "Cursor for partition '{}' updated from chunk to ({}, {})", cursor->partition_id, position.block_number, position.block_offset);
    }

    if (!input.isFinished())
        input.setNeeded();

    output.push(std::move(chunk));
    return Status::PortFull;
}

IProcessor::Status MergeTreeCommitOrderSequentialSource::handleReconfiguration()
{
    auto & output = outputs.front();

    if (output.isFinished())
        return Status::Finished;

    if (pending_snapshot.has_value())
        return Status::UpdatePipeline;

    if (subscription->isDisabled())
    {
        output.finish();
        return Status::Finished;
    }

    const auto safe_block_numbers = subscription->snapshot();
    const auto classification = getPartitionsClassification(safe_block_numbers, last_emitted_positions);

    if (!classification.readable_partitions.empty())
        return Status::Ready;

    if (!current_sub_pipeline.empty())
        return Status::UpdatePipeline;

    return Status::Async;
}

void MergeTreeCommitOrderSequentialSource::handlePipelineEnd()
{
    for (const auto & [partition_id, safe_block_number] : reading_up_to_block_numbers)
    {
        auto & position = last_emitted_positions[partition_id];
        position.block_number = safe_block_number + 1;
        position.block_offset = -1;
        LOG_TEST(log, "Cursor for partition '{}' promoted on pipeline end to ({}, {})", partition_id, position.block_number, position.block_offset);
    }

    reading_up_to_block_numbers.clear();
}

IProcessor::Status MergeTreeCommitOrderSequentialSource::prepare()
{
    const bool has_running_sub_pipeline = !inputs.empty() && inputs.front().isConnected() && !inputs.front().isFinished();
    if (has_running_sub_pipeline)
        return handleRunningPipeline();

    if (!pending_snapshot.has_value())
        handlePipelineEnd();

    return handleReconfiguration();
}

void MergeTreeCommitOrderSequentialSource::work()
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeCommitOrderSequentialSource::work");

    chassert(!pending_snapshot.has_value());

    subscription->drain();
    const auto safe_block_numbers = subscription->snapshot();
    const auto classification = getPartitionsClassification(safe_block_numbers, last_emitted_positions);

    if (subscription->isDisabled())
        return;

    if (classification.readable_partitions.empty())
        return;

    for (const auto & partition_id : classification.readable_partitions)
        reading_up_to_block_numbers[partition_id] = safe_block_numbers.at(partition_id);

    auto result = buildNextSnapshotReadingPipeline(
        classification,
        safe_block_numbers,
        last_emitted_positions,
        storage,
        query_info,
        initial_prewhere_info,
        context,
        user_requested_columns,
        requested_num_streams,
        max_block_size,
        header,
        log);

    if (result)
    {
        pending_snapshot = std::move(result->pipe);
        pending_resources = std::make_unique<QueryPlanResourceHolder>(std::move(result->resources));
    }
}

int MergeTreeCommitOrderSequentialSource::schedule()
{
    return subscription->fd();
}

IProcessor::PipelineUpdate MergeTreeCommitOrderSequentialSource::updatePipeline()
{
    chassert(pending_snapshot.has_value() || !current_sub_pipeline.empty());

    PipelineUpdate update;

    /// Tear down the previous snapshot reading sub-pipeline.
    if (!current_sub_pipeline.empty())
    {
        chassert(!inputs.empty());
        chassert(inputs.front().isConnected());
        LOG_TEST(log, "Tear down previous snapshot reading sub-pipeline");

        auto & input = inputs.front();
        disconnect(input.getOutputPort(), input);

        update.to_remove = std::exchange(current_sub_pipeline, {});
        current_resources.reset();
    }

    /// Attach the next snapshot reading sub-pipeline if one is ready.
    if (pending_snapshot.has_value())
    {
        auto sub_pipe = std::exchange(pending_snapshot, std::nullopt);
        chassert(sub_pipe->numOutputPorts() == 1);
        LOG_TEST(log, "Connecting next snapshot reading sub-pipeline");

        if (inputs.empty())
            inputs.emplace_back(*header, this);

        auto * sub_output = sub_pipe->getOutputPort(0);
        auto sub_processors = Pipe::detachProcessors(std::move(sub_pipe.value()));

        auto & input = inputs.front();
        connect(*sub_output, input);
        input.reopen();
        input.setNeeded();

        current_sub_pipeline = sub_processors;
        current_resources = std::move(pending_resources);
        update.to_add = std::move(sub_processors);
    }

    return update;
}

void MergeTreeCommitOrderSequentialSource::onUpdatePorts()
{
    if (outputs.front().isFinished())
        subscription->disable();
}

void MergeTreeCommitOrderSequentialSource::onCancel() noexcept
{
    /// disable() notifies through the wakeup pipe and may throw (e.g. on fd corruption);
    /// propagating from noexcept would terminate the server.
    try
    {
        subscription->disable();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

}
