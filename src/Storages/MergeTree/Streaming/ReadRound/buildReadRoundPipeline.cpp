#include <Storages/MergeTree/Streaming/ReadRound/buildReadRoundPipeline.h>
#include <Storages/MergeTree/Streaming/ReadRound/AlignStreams.h>
#include <Storages/MergeTree/Streaming/ReadRound/StampPartitionWatermarks.h>
#include <Storages/MergeTree/Streaming/ReadRound/StampPartitionCursors.h>
#include <Storages/MergeTree/Streaming/PartitionsClassification.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/StorageSnapshot.h>

#include <Parsers/IAST.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/Streaming/Utils.h>

#include <IO/WriteBufferFromString.h>

#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/printPipeline.h>

#include <Processors/QueryPlan/Streaming/CalculateWatermarksStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Streaming/Markers.h>

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Core/Streaming/StreamingVirtualColumns.h>

#include <Common/logger_useful.h>

#include <algorithm>
#include <memory>

namespace DB
{

namespace
{

/// Commit-order key + everything the watermark needs.
Names metadataStreamColumns(const StreamSettings & stream_settings, const StorageMetadataPtr & metadata, const ContextPtr & context)
{
    Names columns{PartitionIdColumn::name, BlockNumberColumn::name, BlockOffsetColumn::name};

    if (!std::ranges::contains(columns, stream_settings.watermark->column))
        columns.push_back(stream_settings.watermark->column);

    const auto source_columns = collectWatermarkSourceColumns(stream_settings.watermark->expression, metadata->getColumns().getAllPhysical(), context);
    for (const auto & source_column : source_columns)
        if (!std::ranges::contains(columns, source_column))
            columns.push_back(source_column);

    return columns;
}

/// User-requested columns + the commit-order key and the prewhere and row filter inputs.
Names dataStreamColumns(Names columns, const PrewhereInfoPtr & prewhere_info, const FilterDAGInfoPtr & row_level_filter)
{
    for (const auto & aux_name : {PartitionIdColumn::name, BlockNumberColumn::name, BlockOffsetColumn::name})
        if (!std::ranges::contains(columns, aux_name))
            columns.push_back(aux_name);

    if (prewhere_info)
    {
        const auto source_columns = prewhere_info->prewhere_actions.getRequiredColumnsNames();
        for (const auto & source_column : source_columns)
            if (!std::ranges::contains(columns, source_column))
                columns.push_back(source_column);
    }

    if (row_level_filter)
    {
        const auto source_columns = row_level_filter->actions.getRequiredColumnsNames();
        for (const auto & source_column : source_columns)
            if (!std::ranges::contains(columns, source_column))
                columns.push_back(source_column);
    }

    return columns;
}

/// Read the partition in commit order.
QueryPlanPtr buildPartitionCommitOrderReadPlan(
    const ReadRoundContext & reading_context,
    const ReadState & state,
    const String & partition_id,
    const Int64 & safe_block_number,
    const StorageSnapshotPtr & storage_snapshot,
    const Names & columns)
{
    const auto & stream_settings = reading_context.stream_settings;
    const auto & context = reading_context.context;

    auto plan = MergeTreeDataSelectExecutor(reading_context.storage).read(
        columns,
        storage_snapshot,
        reading_context.query_info,
        context,
        reading_context.max_block_size,
        reading_context.requested_num_streams,
        /*max_block_numbers_to_read=*/nullptr,
        /*enable_parallel_reading=*/false);

    if (!plan || !plan->getRootNode())
        return nullptr;

    /// Add cursor filter to read only the safe round slice.
    auto cursor_filter = buildPartitionFilter(partition_id, state.getPartitionCursor(partition_id), safe_block_number, *plan->getCurrentHeader(), context);
    plan->addStep(std::make_unique<FilterStep>(
        plan->getCurrentHeader(),
        std::move(cursor_filter.actions),
        cursor_filter.column_name,
        cursor_filter.do_remove_column));

    /// Commit-order sort (_block_number, _block_offset); skipped for unordered streams (ordering holds only between rounds).
    if (!stream_settings.unordered)
    {
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
    }

    return plan;
}

QueryPlanPtr addWatermarkCalculation(
    QueryPlanPtr data_plan,
    const ReadRoundContext & reading_context,
    const ReadState & state,
    const String & partition_id,
    const Int64 & safe_block_number,
    const StorageSnapshotPtr & storage_snapshot)
{
    const auto & stream_settings = reading_context.stream_settings;
    const auto & context = reading_context.context;

    const auto metadata_columns = metadataStreamColumns(stream_settings, storage_snapshot->metadata, context);
    auto metadata_plan = buildPartitionCommitOrderReadPlan(reading_context, state, partition_id, safe_block_number, storage_snapshot, metadata_columns);
    metadata_plan->addStep(std::make_unique<CalculateWatermarksStep>(metadata_plan->getCurrentHeader(), stream_settings.watermark, state.getPartitionWatermark(partition_id), context));

    auto align_step = std::make_unique<AlignStreamsStep>(metadata_plan->getCurrentHeader(), data_plan->getCurrentHeader());

    std::vector<QueryPlanPtr> plans;
    plans.push_back(std::move(metadata_plan));
    plans.push_back(std::move(data_plan));

    auto plan = std::make_unique<QueryPlan>();
    plan->unitePlans(std::move(align_step), std::move(plans));
    return plan;
}

Pipe buildPartitionReadingPipeline(
    const ReadRoundContext & reading_context,
    const ReadState & state,
    const String & partition_id,
    const Int64 & safe_block_number,
    const StorageSnapshotPtr & storage_snapshot,
    const QueryPlanOptimizationSettings & opt_settings,
    QueryPlanResourceHolder & resources)
{
    const auto & stream_settings = reading_context.stream_settings;
    const auto & context = reading_context.context;
    const auto & prewhere_info = reading_context.prewhere_info;
    const auto & row_level_filter = reading_context.row_level_filter;
    const auto & output_header = reading_context.output_header;

    const auto columns_to_read = dataStreamColumns(reading_context.user_requested_columns, prewhere_info, row_level_filter);
    auto plan = buildPartitionCommitOrderReadPlan(reading_context, state, partition_id, safe_block_number, storage_snapshot, columns_to_read);
    if (!plan)
        return {};

    /// Add filter built from the outer query analysis.
    if (prewhere_info)
    {
        plan->addStep(std::make_unique<FilterStep>(
            plan->getCurrentHeader(),
            prewhere_info->prewhere_actions.clone(),
            prewhere_info->prewhere_column_name,
            prewhere_info->remove_prewhere_column));
    }

    /// Add row policy filter built from the outer query analysis.
    if (row_level_filter)
    {
        plan->addStep(std::make_unique<FilterStep>(
            plan->getCurrentHeader(),
            row_level_filter->actions.clone(),
            row_level_filter->column_name,
            row_level_filter->do_remove_column));
    }

    /// The watermarks are computed on the unfiltered metadata stream and aligned with data stream.
    if (stream_settings.watermark)
    {
        plan = addWatermarkCalculation(std::move(plan), reading_context, state, partition_id, safe_block_number, storage_snapshot);
        plan->addStep(std::make_unique<StampPartitionWatermarksStep>(plan->getCurrentHeader(), partition_id));
    }

    /// Add cursor calculation step.
    plan->addStep(std::make_unique<StampPartitionCursorsStep>(plan->getCurrentHeader(), stream_settings.unordered));

    /// Add projection to required header.
    auto convert = ActionsDAG::makeConvertingActions(
        plan->getCurrentHeader()->getColumnsWithTypeAndName(),
        output_header->getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        context);
    plan->addStep(std::make_unique<ExpressionStep>(plan->getCurrentHeader(), std::move(convert)));

    /// Build pipeline.
    plan->optimize(opt_settings);
    auto builder = plan->buildQueryPipeline(opt_settings, BuildQueryPipelineSettings(context), /*do_optimize=*/false);
    return QueryPipelineBuilder::getPipe(std::move(*builder), resources);
}

Pipe makePlaceholderPipe(const SharedHeader & output_header, Chunk chunk)
{
    return Pipe(std::make_shared<SourceFromSingleChunk>(output_header, std::move(chunk)));
}

}

std::optional<ReadRoundPipeline> buildReadRoundPipeline(
    const ReadRoundContext & reading_context,
    const ReadState & state,
    const std::map<String, Int64> & safe_block_numbers)
{
    const auto & stream_settings = reading_context.stream_settings;
    const auto & context = reading_context.context;
    const auto & output_header = reading_context.output_header;
    const auto metadata = reading_context.storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/true);
    const auto storage_snapshot = reading_context.storage.getStorageSnapshot(metadata, context);
    const auto classification = classifyPartitions(state, safe_block_numbers, stream_settings);
    const QueryPlanOptimizationSettings opt_settings(context);

    ReadRoundPipeline result;
    Pipes pipes;

    for (const auto & partition_id : classification.changed_partitions)
    {
        auto pipe = buildPartitionReadingPipeline(
            reading_context,
            state,
            partition_id,
            safe_block_numbers.at(partition_id),
            storage_snapshot,
            opt_settings,
            result.resources);

        if (!pipe.empty())
            pipes.emplace_back(std::move(pipe));
    }

    if (stream_settings.watermark)
    {
        for (const auto & partition_id : classification.unchanged_partitions)
            pipes.emplace_back(makePlaceholderPipe(output_header, WatermarkMarker::create(*output_header, state.getPartitionWatermark(partition_id))));
    }

    if (pipes.empty())
        return std::nullopt;

    result.pipe = Pipe::unitePipes(std::move(pipes));

    if (stream_settings.watermark)
        result.pipe.calibrateWatermarks(1, state.getLastEmittedWatermark());
    else
        result.pipe.resize(1);

    return result;
}

}
