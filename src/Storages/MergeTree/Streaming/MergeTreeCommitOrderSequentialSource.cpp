#include <Storages/MergeTree/Streaming/MergeTreeCommitOrderSequentialSource.h>
#include <Storages/MergeTree/Streaming/StreamingChunkCursor.h>
#include <Storages/MergeTree/Streaming/CursorUtils.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>

#include <IO/WriteBufferFromString.h>

#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>

#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>

#include <Common/EventFD.h>
#include <Common/logger_useful.h>

#include <memory>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_block_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

std::vector<std::string> getPartitionsCanBeRead(const std::map<String, Int64> & safe_block_numbers, const MergeTreeCursor & last_emitted_positions)
{
    std::vector<std::string> partitions_to_read;
    for (const auto & [partition_id, safe_block_number] : safe_block_numbers)
    {
        auto it = last_emitted_positions.find(partition_id);

        if (it == last_emitted_positions.end())
            partitions_to_read.push_back(partition_id);

        else if (it->second.block_number <= safe_block_number)
            partitions_to_read.push_back(partition_id);
    }

    return partitions_to_read;
}

bool canConstructReadingPipeline(const std::map<String, Int64> & safe_block_numbers, const MergeTreeCursor & last_emitted_positions)
{
    return !getPartitionsCanBeRead(safe_block_numbers, last_emitted_positions).empty();
}

struct PipeWithResources
{
    Pipe pipe;
    QueryPlanResourceHolder resources;
};

/// Returns safe snapshot reading plan from the specified partition.
std::optional<PipeWithResources> buildPartitionReadingPipeline(
    const String & partition_id,
    const PartitionCursor & last_emitted_position,
    Int64 safe_block_number,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const PrewhereInfoPtr & initial_prewhere_info,
    const ContextPtr & context,
    const Names & inner_columns,
    size_t requested_num_streams,
    UInt64 max_block_size,
    const SharedHeader & output_header,
    const LoggerPtr & log)
{
    auto sub = MergeTreeDataSelectExecutor(storage).read(
        inner_columns,
        storage_snapshot,
        query_info,
        context,
        max_block_size,
        requested_num_streams,
        /*max_block_numbers_to_read=*/nullptr,
        /*enable_parallel_reading=*/false);

    if (!sub || !sub->getRootNode())
        return std::nullopt;

    QueryPlan plan = std::move(*sub);

    /// Add cursor filter to read only safe snapshot.
    auto cursor_filter = buildPartitionFilter(partition_id, last_emitted_position, safe_block_number, *plan.getCurrentHeader(), context);
    plan.addStep(std::make_unique<FilterStep>(
        plan.getCurrentHeader(),
        std::move(cursor_filter.actions),
        cursor_filter.column_name,
        cursor_filter.do_remove_column));

    /// Add prewhere built from the outer query analysis.
    if (initial_prewhere_info)
    {
        plan.addStep(std::make_unique<FilterStep>(
            plan.getCurrentHeader(),
            initial_prewhere_info->prewhere_actions.clone(),
            initial_prewhere_info->prewhere_column_name,
            initial_prewhere_info->remove_prewhere_column));
    }

    /// Add commit-order sorting (_block_number, _block_offset)
    SortDescription sort_desc;
    sort_desc.emplace_back(BlockNumberColumn::name, 1);
    sort_desc.emplace_back(BlockOffsetColumn::name, 1);
    SortingStep::Settings sort_settings(context->getSettingsRef());
    plan.addStep(std::make_unique<SortingStep>(
        plan.getCurrentHeader(),
        std::move(sort_desc),
        /*limit=*/ 0,
        sort_settings,
        /*is_sorting_for_merge_join=*/false));

    /// Add cursor calculation step.
    plan.addStep(std::make_unique<BuildStreamingChunkCursorStep>(plan.getCurrentHeader()));

    /// Add projection to required header.
    auto convert = ActionsDAG::makeConvertingActions(
        plan.getCurrentHeader()->getColumnsWithTypeAndName(),
        output_header->getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);
    plan.addStep(std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(convert)));

    /// TODO(michicosun): somehow force projection usage here
    QueryPlanOptimizationSettings opt_settings(context);
    plan.optimize(opt_settings);

    if (log->test())
    {
        WriteBufferFromOwnString plan_buffer;
        ExplainPlanOptions explain_options{.header = true, .actions = true, .indexes = true, .compact = true, .pretty = true};
        plan.explainPlan(plan_buffer, explain_options);
        LOG_TEST(log, "Snapshot subplan for partition '{}' (safe_block_number={}):\n{}", partition_id, safe_block_number, plan_buffer.str());
    }

    auto builder = plan.buildQueryPipeline(opt_settings, BuildQueryPipelineSettings(context));

    PipeWithResources result;
    result.pipe = QueryPipelineBuilder::getPipe(std::move(*builder), result.resources);

    if (log->test())
    {
        WriteBufferFromOwnString pipeline_buffer;
        printPipeline(result.pipe.getProcessors(), pipeline_buffer);
        LOG_TEST(log, "Snapshot pipeline for partition '{}' (safe_block_number={}):\n{}", partition_id, safe_block_number, pipeline_buffer.str());
    }

    return result;
}

std::optional<PipeWithResources> buildNextSnapshotReadingPipeline(
    const std::map<String, Int64> & safe_block_numbers,
    const MergeTreeCursor & last_emitted_positions,
    const MergeTreeData & storage,
    const SelectQueryInfo & query_info,
    const PrewhereInfoPtr & initial_prewhere_info,
    const ContextPtr & context,
    const Names & user_requested_columns,
    size_t requested_num_streams,
    UInt64 max_block_size,
    const SharedHeader & output_header,
    const LoggerPtr & log)
{
    const auto partitions_to_read = getPartitionsCanBeRead(safe_block_numbers, last_emitted_positions);
    chassert(!partitions_to_read.empty());

    LOG_DEBUG(log, "Building new snapshot for {} partition(s): [{}]", partitions_to_read.size(), fmt::join(partitions_to_read, ", "));

    /// Fresh storage snapshot reused by every per-partition subplan in this iteration.
    const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/true);
    const auto storage_snapshot = storage.getStorageSnapshot(metadata, context);

    /// We need all columns user requested + columns needed to recalculate cursors.
    const auto columns_to_read = extendWithAuxiliaryColumns(user_requested_columns);

    Pipes per_partition_pipes;
    QueryPlanResourceHolder accumulated_resources;

    for (const auto & partition_id : partitions_to_read)
    {
        PartitionCursor last_emitted_position = last_emitted_positions.contains(partition_id) ? last_emitted_positions.at(partition_id) : PartitionCursor{};
        int64_t safe_block_number = safe_block_numbers.at(partition_id);

        auto result = buildPartitionReadingPipeline(
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
            output_header,
            log);

        if (result)
        {
            per_partition_pipes.emplace_back(std::move(result->pipe));
            accumulated_resources.append(result->resources);
        }
    }

    if (per_partition_pipes.empty())
        return std::nullopt;

    Pipe united = Pipe::unitePipes(std::move(per_partition_pipes));
    united.resize(1);

    PipeWithResources snapshot;
    snapshot.pipe = std::move(united);
    snapshot.resources = std::move(accumulated_resources);
    return snapshot;
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

}

MergeTreeCommitOrderSequentialSource::MergeTreeCommitOrderSequentialSource(
    SharedHeader header_,
    const MergeTreeData & storage_,
    const SelectQueryInfo & query_info_,
    ContextPtr context_,
    Names user_requested_columns_,
    size_t requested_num_streams_,
    UInt64 max_block_size_,
    MergeTreeBoundsSubscriptionPtr subscription_,
    MergeTreeCursor starting_positions_)
    : IProcessor({}, {Block(*header_)})
    , header(std::move(header_))
    , storage(storage_)
    , query_info(makeStreamingSelectQueryInfo(query_info_))
    , initial_prewhere_info(query_info_.prewhere_info)
    , context(makeStreamingContext(std::move(context_)))
    , user_requested_columns(std::move(user_requested_columns_))
    , requested_num_streams(requested_num_streams_)
    , max_block_size(max_block_size_)
    , subscription(std::move(subscription_))
    , log(getLogger("MergeTreeCommitOrderSequentialSource"))
    , last_emitted_positions(std::move(starting_positions_))
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

    if (auto cursor = chunk.getChunkInfos().get<StreamingChunkCursorInfo>())
    {
        auto & position = last_emitted_positions[cursor->partition_id];
        position.block_number = cursor->last_block_number;
        position.block_offset = cursor->last_block_offset;
        LOG_TEST(log, "Cursor for partition '{}' updated from chunk to ({}, {})", cursor->partition_id, position.block_number, position.block_offset);
    }

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

    if (canConstructReadingPipeline(subscription->snapshot(), last_emitted_positions))
        return Status::Ready;

    if (subscription->fd())
        return Status::Async;

    return Status::Ready;
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
    const bool has_running_sub_pipeline = !inputs.empty() && !inputs.front().isFinished();
    if (has_running_sub_pipeline)
        return handleRunningPipeline();

    if (!pending_snapshot.has_value())
        handlePipelineEnd();

    return handleReconfiguration();
}

void MergeTreeCommitOrderSequentialSource::work()
{
    chassert(!pending_snapshot.has_value());

    if (EventFD * fd = subscription->fd())
        fd->read();

    auto safe_block_numbers = subscription->snapshot();
    if (subscription->fd())
    {
        if (!canConstructReadingPipeline(safe_block_numbers, last_emitted_positions))
            return;
    }
    else
    {
        while (!canConstructReadingPipeline(safe_block_numbers, last_emitted_positions))
        {
            subscription->wait();
            safe_block_numbers = subscription->snapshot();
        }
    }

    const auto partitions_to_read = getPartitionsCanBeRead(safe_block_numbers, last_emitted_positions);
    for (const auto & partition_id : partitions_to_read)
        reading_up_to_block_numbers[partition_id] = safe_block_numbers.at(partition_id);

    auto result = buildNextSnapshotReadingPipeline(
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
        resources.append(result->resources);
    }
}

int MergeTreeCommitOrderSequentialSource::schedule()
{
    return subscription->fd()->fd;
}

IProcessor::PipelineUpdate MergeTreeCommitOrderSequentialSource::updatePipeline()
{
    chassert(pending_snapshot.has_value());

    PipelineUpdate update;

    /// Disconnect from the old snapshot reading plan
    if (!current_sub_pipeline.empty())
    {
        chassert(!inputs.empty());
        chassert(inputs.front().isConnected());

        auto & input = inputs.front();
        disconnect(input.getOutputPort(), input);

        update.to_remove = std::exchange(current_sub_pipeline, {});
    }

    /// Take next snapshot reading plan
    auto sub_pipe = std::exchange(pending_snapshot, std::nullopt);
    chassert(sub_pipe->numOutputPorts() == 1);

    /// Connect to new snapshot reading plan
    if (inputs.empty())
        inputs.emplace_back(*header, this);

    auto * sub_output = sub_pipe->getOutputPort(0);
    auto sub_processors = Pipe::detachProcessors(std::move(sub_pipe.value()));

    auto & input = inputs.front();
    connect(*sub_output, input);
    input.reopen();
    input.setNeeded();

    /// Register next reading plan in pipeline extension
    current_sub_pipeline = sub_processors;
    update.to_add = std::move(sub_processors);
    return update;
}

void MergeTreeCommitOrderSequentialSource::onCancel() noexcept
{
    subscription->disable();
}

}
