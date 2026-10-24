#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Storages/MergeTree/Streaming/MergeTreeCommitOrderSource.h>
#include <Storages/MergeTree/Streaming/PartitionsClassification.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/ReadRoundContext.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/StampPartitionCursors.h>
#include <Storages/MergeTree/Streaming/ReadingPlan/AlignStreams.h>

#include <Storages/MergeTree/MergeTreeData.h>

#include <Parsers/IAST.h>

#include <Interpreters/Context.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/printPipeline.h>

#include <IO/WriteBufferFromString.h>

#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Streaming/Markers.h>

#include <Core/UUID.h>
#include <Core/Block.h>
#include <Core/Streaming/Settings.h>
#include <Core/Streaming/CursorTree.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/Epoll.h>

#include <Core/Settings.h>

#include <base/defines.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 stream_max_rows_per_round;
    extern const SettingsUInt64 stream_max_bytes_per_round;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

std::string explainPipeline(const Pipe & pipe)
{
    WriteBufferFromOwnString buffer;
    printPipeline(pipe.getProcessors(), buffer);
    return buffer.str();
}

/// Lowers the per-partition upper bounds of a read round to about `max_rows` rows and `max_bytes` bytes, reading at least one block.
std::map<String, Int64> limitRoundBounds(
    const MergeTreeData & storage,
    const ReadState & state,
    std::map<String, Int64> safe_block_numbers,
    size_t first_partition,
    UInt64 max_rows,
    UInt64 max_bytes)
{
    std::vector<String> changed_partitions;
    for (const auto & [partition_id, safe_block_number] : safe_block_numbers)
        if (state.getPartitionCursor(partition_id).block_number <= safe_block_number)
            changed_partitions.push_back(partition_id);

    if (changed_partitions.empty())
        return safe_block_numbers;

    std::rotate(changed_partitions.begin(), changed_partitions.begin() + first_partition % changed_partitions.size(), changed_partitions.end());

    double rows_budget = max_rows ? static_cast<double>(max_rows) : std::numeric_limits<double>::infinity();
    double bytes_budget = max_bytes ? static_cast<double>(max_bytes) : std::numeric_limits<double>::infinity();
    bool budget_exhausted = false;
    bool any_part_taken = false;

    for (const auto & partition_id : changed_partitions)
    {
        const Int64 from = state.getPartitionCursor(partition_id).block_number;
        Int64 & bound = safe_block_numbers.at(partition_id);

        if (budget_exhausted)
        {
            bound = from - 1;
            continue;
        }

        auto parts = storage.getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, partition_id);
        std::ranges::sort(parts, {}, [](const auto & part) { return part->info.min_block; });

        const Int64 safe_block_number = bound;
        for (const auto & part : parts)
        {
            if (part->info.max_block < from || part->info.min_block > safe_block_number)
                continue;

            const Int64 low = std::max(part->info.min_block, from);
            const Int64 high = std::min(part->info.max_block, safe_block_number);
            const double blocks_in_part = static_cast<double>(part->info.max_block - part->info.min_block + 1);
            const double rows_per_block = static_cast<double>(part->rows_count) / blocks_in_part;
            const double bytes_per_block = static_cast<double>(part->getBytesUncompressedOnDisk()) / blocks_in_part;
            const double blocks = static_cast<double>(high - low + 1);

            if (rows_per_block * blocks <= rows_budget && bytes_per_block * blocks <= bytes_budget)
            {
                rows_budget -= rows_per_block * blocks;
                bytes_budget -= bytes_per_block * blocks;
                any_part_taken = true;
                continue;
            }

            /// The part crosses the limit: take as many of its blocks as fit, ending the round inside it.
            /// The limit that is crossed has a non-zero size per block, so the result is finite and below `blocks`.
            const auto fit = [](double budget, double per_block) { return per_block > 0 ? budget / per_block : std::numeric_limits<double>::infinity(); };
            const Int64 taken = static_cast<Int64>(std::min(fit(rows_budget, rows_per_block), fit(bytes_budget, bytes_per_block)));
            /// Read at least one block of data per round.
            bound = low + std::max<Int64>(taken, any_part_taken ? 0 : 1) - 1;
            budget_exhausted = true;
            break;
        }
    }

    return safe_block_numbers;
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
    , reading_context(makeReadRoundContext(storage_, query_info_, std::move(context_), std::move(user_requested_columns_), requested_num_streams_, max_block_size_, header))
    , max_rows_per_round(reading_context.context->getSettingsRef()[Setting::stream_max_rows_per_round])
    , max_bytes_per_round(reading_context.context->getSettingsRef()[Setting::stream_max_bytes_per_round])
    , log(getLogger(fmt::format("MergeTreeCommitOrderSource::{}", UUIDHelpers::generateV4())))
    , read_state(stream_settings)
{
    /// A partition left out of a round must not be reported idle, which would let the watermark pass its unread rows.
    if (stream_settings.watermark && (max_rows_per_round || max_bytes_per_round))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Settings `stream_max_rows_per_round` and `stream_max_bytes_per_round` are not supported for a stream with a watermark");
}

IProcessor::Status MergeTreeCommitOrderSource::handleRunningPipeline()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (input.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    auto chunk = input.pull(/*set_not_needed=*/true);

    if (!input.isFinished())
        input.setNeeded();

    if (auto global_watermark = chunk.getChunkInfos().get<WatermarkMarker>())
        read_state.updateGlobalWatermark(global_watermark->watermark);

    if (auto partition_cursor = chunk.getChunkInfos().extract<PartitionCursorInfo>())
        read_state.updatePartitionCursor(partition_cursor->partition_id, partition_cursor->last);

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

    if (finished_round.has_value())
        return Status::UpdatePipeline;

    return Status::Async;
}

IProcessor::Status MergeTreeCommitOrderSource::handleBoundedReconfiguration(const ClassifiedPartitions & partitions, bool subscription_updated)
{
    const auto result = handleReconfiguration(partitions, subscription_updated);

    // Finish after the first completed read round, or once the first enrichment shows nothing (more) to read.
    if (subscription_updated && (finished_rounds > 0 || result == Status::Async))
    {
        surfaceFinalCursor();
        outputs.front().finish();
        return Status::Finished;
    }

    return result;
}

void MergeTreeCommitOrderSource::startRound()
{
    current_round = std::exchange(pending_round, std::nullopt);
    read_state.startReadRound(current_round->partitions);
}

void MergeTreeCommitOrderSource::finishRound()
{
    read_state.finishReadRound(current_round->partitions, current_round->safe_block_numbers);
    finished_rounds += 1;

    LOG_TEST(log, "Finished read round #{}", finished_rounds);

    if (current_round->pipeline.has_value())
        finished_round = std::exchange(current_round, std::nullopt);
    else
        current_round.reset();
}

void MergeTreeCommitOrderSource::surfaceFinalCursor()
{
    auto cursor = reading_context.context->getStreamingCursor();
    if (!cursor)
        return;

    auto local = mergeTreeCursorToCursorTree(read_state.getPartitionCursors());
    std::lock_guard lock(cursor->mutex);
    mergeCursors(cursor->tree, local);
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

    const bool is_round_running = current_round.has_value();
    if (is_round_running)
    {
        if (auto sub_pipeline_status = handleRunningPipeline(); sub_pipeline_status != Status::Finished)
            return sub_pipeline_status;

        finishRound();
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
    chassert(!current_round.has_value());

    if (subscription->isDisabled())
        return;

    auto [safe_block_numbers, was_updated] = subscription->snapshot();
    if (max_rows_per_round || max_bytes_per_round)
        safe_block_numbers = limitRoundBounds(
            reading_context.storage, read_state, std::move(safe_block_numbers), static_cast<size_t>(finished_rounds), max_rows_per_round, max_bytes_per_round);

    auto classification = classifyPartitions(read_state, safe_block_numbers, stream_settings);

    read_state.updatePartitionSet(classification);
    pending_round = {
        .pipeline = buildReadRoundPipeline(reading_context, read_state, safe_block_numbers),
        .safe_block_numbers = std::move(safe_block_numbers),
        .partitions = std::move(classification),
    };

    if (!pending_round->pipeline.has_value())
    {
        startRound();
        finishRound();
    }
}

std::tuple<int, uint32_t, Int64> MergeTreeCommitOrderSource::scheduleForEvent()
{
    return {subscription->fd(), EPOLLIN | EPOLLERR, read_state.calculateTimeToNextIdle(stream_settings)};
}

IProcessor::PipelineUpdate MergeTreeCommitOrderSource::updatePipeline()
{
    chassert(finished_round.has_value() || pending_round.has_value());
    chassert(!current_round.has_value());

    PipelineUpdate update;

    /// Tear down the finished read round sub-pipeline.
    if (finished_round.has_value())
    {
        chassert(!inputs.empty());
        chassert(inputs.front().isConnected());
        chassert(inputs.front().isFinished());
        LOG_TEST(log, "Tear down finished read round sub-pipeline");

        auto & input = inputs.front();
        disconnect(input.getOutputPort(), input);

        update.to_remove = finished_round->pipeline->pipe.getProcessors();
        finished_round.reset();
    }

    /// Attach the pending read round sub-pipeline.
    if (pending_round.has_value())
    {
        startRound();

        auto & pipe = current_round->pipeline->pipe;
        chassert(pipe.numOutputPorts() == 1);
        LOG_TEST(log, "Connecting next read round sub-pipeline:\n{}", explainPipeline(pipe));

        if (inputs.empty())
            inputs.emplace_back(*header, this);

        for (const auto & processor : pipe.getProcessors())
            processor->inheritQueryPlanStepFromParent(*this, getQueryPlanStepGroup());

        auto & input = inputs.front();
        connect(*pipe.getOutputPort(0), input);
        input.reopen();
        input.setNeeded();

        update.to_add = pipe.getProcessors();
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
