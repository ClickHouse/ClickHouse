#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Storages/MergeTree/Streaming/MergeTreeCommitOrderSource.h>
#include <Storages/MergeTree/Streaming/PartitionsClassification.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionCursors.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionWatermarks.h>

#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <Parsers/IAST.h>

#include <Interpreters/Context.h>
#include <Interpreters/Streaming/Utils.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/printPipeline.h>

#include <IO/WriteBufferFromString.h>

#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Streaming/Markers.h>

#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Core/Block.h>
#include <Core/Streaming/Settings.h>
#include <Core/Streaming/StreamingVirtualColumns.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/Epoll.h>

#include <base/defines.h>

#include <algorithm>
#include <memory>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_sort;
    extern const SettingsUInt64 max_bytes_to_sort;
    extern const SettingsOverflowMode sort_overflow_mode;
}

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS;
    extern const int TOO_MANY_BYTES;
    extern const int TOO_MANY_ROWS_OR_BYTES;
}

namespace
{

ContextPtr makeStreamingContext(ContextPtr context_)
{
    auto copy = Context::createCopy(context_);
    copy->makeQueryContext();
    copy->setQueryMetadataCache(nullptr);

    /// Read limits must not affect the read round plans.
    copy->setSetting("max_rows_to_read", Field(0));
    copy->setSetting("max_bytes_to_read", Field(0));
    copy->setSetting("max_rows_to_read_leaf", Field(0));
    copy->setSetting("max_bytes_to_read_leaf", Field(0));
    copy->setSetting("max_rows_to_sort", Field(0));
    copy->setSetting("max_bytes_to_sort", Field(0));

    return copy;
}

SelectQueryInfo makeStreamingSelectQueryInfo(SelectQueryInfo info)
{
    info.table_expression_modifiers = std::nullopt;

    info.query_tree.reset();
    info.table_expression.reset();
    info.planner_context.reset();

    info.storage_limits = std::make_shared<StorageLimitsList>();

    info.prewhere_info.reset();
    info.filter_actions_dag.reset();
    info.row_level_filter.reset();

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

void restoreStreamingAuxiliaryColumns(ActionsDAG & actions, const StreamSettings & stream_settings, const MergeTreeData & storage, const ContextPtr & context)
{
    /// These columns are needed for cursor calculation.
    actions.tryRestoreColumn(PartitionIdColumn::name);
    actions.tryRestoreColumn(BlockNumberColumn::name);
    actions.tryRestoreColumn(BlockOffsetColumn::name);

    /// These columns are needed for watermark calculation.
    if (stream_settings.watermark)
    {
        actions.tryRestoreColumn(stream_settings.watermark->column);

        const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
        const auto source_columns = collectWatermarkSourceColumns(stream_settings.watermark->expression, metadata->getColumns().getAllPhysical(), context);
        for (const auto & source_column : source_columns)
            actions.tryRestoreColumn(source_column);
    }
}

PrewhereInfoPtr makeReadRoundPrewhereInfo(PrewhereInfoPtr info, const StreamSettings & stream_settings, const MergeTreeData & storage, const ContextPtr & context)
{
    if (!info)
        return nullptr;

    auto patched_info = std::make_shared<PrewhereInfo>(info->clone());
    restoreStreamingAuxiliaryColumns(patched_info->prewhere_actions, stream_settings, storage, context);

    return patched_info;
}

FilterDAGInfoPtr makeReadRoundRowLevelFilter(FilterDAGInfoPtr info, const StreamSettings & stream_settings, const MergeTreeData & storage, const ContextPtr & context)
{
    if (!info)
        return nullptr;

    auto patched_info = std::make_shared<FilterDAGInfo>(info->actions.clone(), info->column_name, info->do_remove_column);
    restoreStreamingAuxiliaryColumns(patched_info->actions, stream_settings, storage, context);
    for (const auto & required_column : patched_info->actions.getRequiredColumnsNames())
        patched_info->actions.tryRestoreColumn(required_column);

    return patched_info;
}

Names filterStreamingVirtualColumns(Names columns)
{
    if (auto it = std::find(columns.begin(), columns.end(), TimeAttributeColumn::name); it != columns.end())
        columns.erase(it);

    if (auto it = std::find(columns.begin(), columns.end(), WatermarkColumn::name); it != columns.end())
        columns.erase(it);

    return columns;
}

bool isSortLimitReached(const SizeLimits & sort_limits, const StreamReadProgress & read_progress)
{
    return !sort_limits.check(
        read_progress.round_read_rows, read_progress.round_read_bytes, "rows or bytes to sort",
        ErrorCodes::TOO_MANY_ROWS_OR_BYTES);
}

bool isReadLimitReached(const StorageLimitsListPtr & storage_limits, const StreamReadProgress & read_progress)
{
    if (!storage_limits)
        return false;

    for (const auto & limits : *storage_limits)
    {
        if (limits.local_limits.mode == LimitsMode::LIMITS_TOTAL)
        {
            const bool within_limits = limits.local_limits.size_limits.check(
                read_progress.read_rows, read_progress.read_bytes, "rows or bytes to read",
                ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES);

            if (!within_limits)
                return true;
        }

        const bool within_leaf_limits = limits.leaf_limits.check(
            read_progress.read_rows, read_progress.read_bytes, "rows or bytes to read on leaf node",
            ErrorCodes::TOO_MANY_ROWS, ErrorCodes::TOO_MANY_BYTES);

        if (!within_leaf_limits)
            return true;
    }

    return false;
}

}

MergeTreeCommitOrderSource::MergeTreeCommitOrderSource(
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
    , subscription(std::move(subscription_))
    , stream_settings(*query_info_.table_expression_modifiers->getStreamSettings())
    , storage_limits(query_info_.storage_limits)
    , sort_limits(
          context_->getSettingsRef()[Setting::max_rows_to_sort],
          context_->getSettingsRef()[Setting::max_bytes_to_sort],
          context_->getSettingsRef()[Setting::sort_overflow_mode])
    , reading_context{
          .storage = storage_,
          .query_info = makeStreamingSelectQueryInfo(query_info_),
          .prewhere_info = makeReadRoundPrewhereInfo(query_info_.prewhere_info, stream_settings, storage_, context_),
          .row_level_filter = makeReadRoundRowLevelFilter(query_info_.row_level_filter, stream_settings, storage_, context_),
          .stream_settings = stream_settings,
          .context = makeStreamingContext(std::move(context_)),
          .user_requested_columns = filterStreamingVirtualColumns(std::move(user_requested_columns_)),
          .requested_num_streams = requested_num_streams_,
          .max_block_size = max_block_size_,
          .output_header = header}
    , log(getLogger(fmt::format("MergeTreeCommitOrderSource::{}", UUIDHelpers::generateV4())))
    , read_state(stream_settings)
{
}

IProcessor::Status MergeTreeCommitOrderSource::handleRunningPipeline()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (!output.canPush())
        return Status::PortFull;

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    auto chunk = input.pull(/*set_not_needed=*/true);
    read_progress.accountChunk(chunk);

    if (!input.isFinished())
        input.setNeeded();

    if (auto global_watermark = chunk.getChunkInfos().get<WatermarkMarker>())
        read_state.updateGlobalWatermark(global_watermark->watermark);

    if (auto partition_cursor = chunk.getChunkInfos().extract<PartitionCursorInfo>())
        read_state.updatePartitionCursor(partition_cursor->partition_id, partition_cursor->cursor);

    if (auto partition_marker = chunk.getChunkInfos().extract<PartitionWatermarkInfo>())
        read_state.updatePartitionWatermark(partition_marker->partition_id, std::move(partition_marker->watermark));

    if (chunk.getNumRows() == 0 && chunk.getChunkInfos().empty())
    {
        /// The dropped chunk was the last one - the sub-pipeline is exhausted.
        if (input.isFinished())
            return Status::Finished;

        return Status::NeedData;
    }

    output.push(std::move(chunk));
    return Status::PortFull;
}

IProcessor::Status MergeTreeCommitOrderSource::handleShutdown()
{
    auto & output = outputs.front();
    chassert(output.isFinished());

    if (inputs.empty() || !inputs.front().isConnected())
        return Status::Finished;

    auto & input = inputs.front();
    input.close();

    return Status::Finished;
}

IProcessor::Status MergeTreeCommitOrderSource::handleReconfiguration(const ClassifiedPartitions & partitions, bool subscription_updated)
{
    auto & output = outputs.front();

    if (output.isFinished())
        return Status::Finished;

    if (pending_round.has_value())
        return Status::UpdatePipeline;

    if (subscription->isDisabled())
    {
        output.finish();
        return Status::Finished;
    }

    if (subscription_updated && read_state.hasWork(partitions))
        return Status::Ready;

    if (current_round.has_value())
        return Status::UpdatePipeline;

    return Status::Async;
}

IProcessor::Status MergeTreeCommitOrderSource::handleBoundedReconfiguration(const ClassifiedPartitions & partitions, bool subscription_updated)
{
    const auto result = handleReconfiguration(partitions, subscription_updated);

    // Finish after the first completed read round, or once the first enrichment shows nothing (more) to read.
    if (subscription_updated && (read_progress.finished_rounds > 0 || result == Status::Async))
    {
        outputs.front().finish();
        return Status::Finished;
    }

    return result;
}

bool MergeTreeCommitOrderSource::needToEmitGlobalIdle(const ClassifiedPartitions & partitions, bool subscription_updated)
{
    if (!stream_settings.watermark)
        return false;

    /// Idle decisions are meaningful only against an applied partition assignment.
    if (!subscription_updated)
        return false;

    /// The idle marker must not overtake the watermark extension emitted by the last idle-triggered rebuild.
    if (read_state.hasWork(partitions) || pending_round.has_value())
        return false;

    const bool all_non_idle_empty = partitions.changed_partitions.empty() && partitions.unchanged_partitions.empty();
    return !read_state.isSourceMarkedIdle() && all_non_idle_empty;
}

IProcessor::Status MergeTreeCommitOrderSource::handleEmitGlobalIdle()
{
    auto & output = outputs.front();

    if (!output.canPush())
        return Status::PortFull;

    LOG_DEBUG(log, "Source is idle - emitting an idle marker");
    output.push(IdleMarker::create(*header));
    read_state.markSourceIdle();
    return Status::PortFull;
}

IProcessor::Status MergeTreeCommitOrderSource::prepare()
{
    subscription->drain();

    const bool is_upstream_finished = outputs.front().isFinished();
    if (is_upstream_finished)
        return handleShutdown();

    const bool limits_reached = isReadLimitReached(storage_limits, read_progress) || isSortLimitReached(sort_limits, read_progress);
    if (limits_reached)
    {
        outputs.front().finish();
        return handleShutdown();
    }

    const bool has_running_sub_pipeline = !inputs.empty() && inputs.front().isConnected() && !inputs.front().isFinished();
    if (has_running_sub_pipeline)
        if (auto sub_pipeline_status = handleRunningPipeline(); sub_pipeline_status != Status::Finished)
            return sub_pipeline_status;

    const bool has_unfinalized_pipeline = !pending_round.has_value() && read_state.readRoundInProgress();
    if (has_unfinalized_pipeline)
    {
        read_state.finalizeReadRound();
        read_progress.accountRound();
        LOG_TEST(log, "Finished read round #{}", read_progress.finished_rounds);
    }

    const auto [safe_block_numbers, subscription_updated] = subscription->snapshot();
    const auto classification = classifyPartitions(read_state, safe_block_numbers, stream_settings);
    if (subscription_updated)
        read_state.updatePartitionSet(classification);

    const bool need_mark_source_idle = needToEmitGlobalIdle(classification, subscription_updated);
    if (need_mark_source_idle)
        return handleEmitGlobalIdle();

    const bool is_bounded_subscription = !stream_settings.subscribe_for_updates;
    if (is_bounded_subscription)
        return handleBoundedReconfiguration(classification, subscription_updated);

    return handleReconfiguration(classification, subscription_updated);
}

void MergeTreeCommitOrderSource::work()
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeCommitOrderSource::work");

    chassert(!pending_round.has_value());

    if (subscription->isDisabled())
        return;

    const auto [safe_block_numbers, was_updated] = subscription->snapshot();
    const auto classification = classifyPartitions(read_state, safe_block_numbers, stream_settings);
    chassert(was_updated);

    read_state.updatePartitionSet(classification);
    read_state.startReadRound(classification, safe_block_numbers);

    pending_round = buildReadRoundPipeline(reading_context, read_state, safe_block_numbers);
}

std::tuple<int, uint32_t, Int64> MergeTreeCommitOrderSource::scheduleForEvent()
{
    return {subscription->fd(), EPOLLIN | EPOLLERR, read_state.calculateTimeToNextIdle(stream_settings)};
}

IProcessor::PipelineUpdate MergeTreeCommitOrderSource::updatePipeline()
{
    chassert(pending_round.has_value() || current_round.has_value());

    PipelineUpdate update;

    /// Tear down the previous read round sub-pipeline.
    if (current_round.has_value())
    {
        chassert(!inputs.empty());
        chassert(inputs.front().isConnected());
        chassert(inputs.front().isFinished());
        LOG_TEST(log, "Tear down previous read round sub-pipeline");

        auto & input = inputs.front();
        disconnect(input.getOutputPort(), input);

        update.to_remove = current_round->pipe.getProcessors();
        current_round.reset();
    }

    /// Attach the next read round sub-pipeline if one is ready.
    if (pending_round.has_value())
    {
        current_round = std::exchange(pending_round, std::nullopt);
        chassert(current_round->pipe.numOutputPorts() == 1);
        LOG_TEST(log, "Connecting next read round sub-pipeline");

        if (inputs.empty())
            inputs.emplace_back(*header, this);

        for (const auto & processor : current_round->pipe.getProcessors())
            processor->inheritQueryPlanStepFromParent(*this, getQueryPlanStepGroup());

        auto & input = inputs.front();
        connect(*current_round->pipe.getOutputPort(0), input);
        input.reopen();
        input.setNeeded();

        update.to_add = current_round->pipe.getProcessors();
    }

    return update;
}

void MergeTreeCommitOrderSource::onUpdatePorts()
{
    if (outputs.front().isFinished())
        subscription->disable();
}

void MergeTreeCommitOrderSource::onCancel() noexcept
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

#endif
