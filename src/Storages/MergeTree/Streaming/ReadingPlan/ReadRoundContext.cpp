#include <Storages/MergeTree/Streaming/ReadingPlan/ReadRoundContext.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/Streaming/Utils.h>

#include <Core/Streaming/StreamingVirtualColumns.h>

#include <algorithm>

namespace DB
{

namespace
{

ContextPtr makeStreamingContext(ContextPtr context_, const ProjectionDescription * projection)
{
    auto copy = Context::createCopy(context_);
    copy->makeQueryContext();
    copy->setQueryMetadataCache(nullptr);

    if (projection)
    {
        copy->setSetting("preferred_optimize_projection_name", projection->name);
        copy->setSetting("prefer_optimize_projection", true);
    }

    return copy;
}

SelectQueryInfo makeStreamingSelectQueryInfo(SelectQueryInfo info)
{
    info.table_expression_modifiers = std::nullopt;

    info.query_tree.reset();
    info.table_expression.reset();
    info.planner_context.reset();

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

const ProjectionDescription * chooseCommitOrderProjection(const StorageInMemoryMetadata & metadata, const Names & columns)
{
    for (const auto & projection : metadata.projections)
    {
        if (projection.type != ProjectionDescription::Type::Normal)
            continue;

        const auto sorting_key = projection.metadata->getSortingKeyColumns();
        if (sorting_key.size() < 2 || sorting_key[0] != BlockNumberColumn::name || sorting_key[1] != BlockOffsetColumn::name)
            continue;

        auto has_column = [&](const String & column) { return projection.sample_block.findColumnOrSubcolumnByName(column).has_value(); };
        if (std::ranges::all_of(columns, has_column))
            return &projection;
    }

    return nullptr;
}

}

Names extendWithAuxiliaryColumns(
    Names columns,
    const StreamSettings & stream_settings,
    const FilterDAGInfoPtr & row_level_filter,
    const StorageMetadataPtr & metadata,
    const ContextPtr & context)
{
    for (const auto & aux_name : {PartitionIdColumn::name, BlockNumberColumn::name, BlockOffsetColumn::name})
        if (!std::ranges::contains(columns, aux_name))
            columns.push_back(aux_name);

    if (stream_settings.watermark)
    {
        if (!std::ranges::contains(columns, stream_settings.watermark->column))
            columns.push_back(stream_settings.watermark->column);

        const auto source_columns = collectWatermarkSourceColumns(stream_settings.watermark->expression, metadata->getColumns().getAllPhysical(), context);
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

ReadRoundContext makeReadRoundContext(
    const MergeTreeData & storage,
    const SelectQueryInfo & query_info,
    ContextPtr context,
    Names user_requested_columns,
    size_t requested_num_streams,
    UInt64 max_block_size,
    SharedHeader output_header)
{
    const auto & stream_settings = *query_info.table_expression_modifiers->getStreamSettings();
    const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);

    auto row_level_filter = makeReadRoundRowLevelFilter(query_info.row_level_filter, stream_settings, storage, context);
    auto columns = filterStreamingVirtualColumns(std::move(user_requested_columns));
    const auto * projection = chooseCommitOrderProjection(*metadata, extendWithAuxiliaryColumns(columns, stream_settings, row_level_filter, metadata, context));

    return ReadRoundContext{
        .storage = storage,
        .query_info = makeStreamingSelectQueryInfo(query_info),
        .prewhere_info = makeReadRoundPrewhereInfo(query_info.prewhere_info, stream_settings, storage, context),
        .row_level_filter = std::move(row_level_filter),
        .stream_settings = stream_settings,
        .context = makeStreamingContext(std::move(context), projection),
        .user_requested_columns = std::move(columns),
        .requested_num_streams = requested_num_streams,
        .max_block_size = max_block_size,
        .output_header = std::move(output_header)};
}

}
