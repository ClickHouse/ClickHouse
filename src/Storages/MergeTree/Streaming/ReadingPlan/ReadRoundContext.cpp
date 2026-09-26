#include <Storages/MergeTree/Streaming/ReadingPlan/ReadRoundContext.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/Streaming/Utils.h>


#include <algorithm>
#include <optional>

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
        actions.tryRestoreColumn(stream_settings.watermark->time_attribute_column);

        const auto metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
        const auto source_columns = collectWatermarkSourceColumns(stream_settings.watermark->expression, metadata->getColumns().getAllPhysical(), context);
        for (const auto & source_column : source_columns)
            actions.tryRestoreColumn(source_column);
    }
}

FilterDAGInfo makeReadRoundFilter(const ActionsDAG & actions, const String & column_name, bool remove_column, const StreamSettings & stream_settings, const MergeTreeData & storage, const ContextPtr & context)
{
    FilterDAGInfo filter{actions.clone(), column_name, remove_column};
    restoreStreamingAuxiliaryColumns(filter.actions, stream_settings, storage, context);
    for (const auto & required_column : filter.actions.getRequiredColumnsNames())
        filter.actions.tryRestoreColumn(required_column);

    return filter;
}

std::optional<FilterDAGInfo> makeReadRoundRowLevelFilter(const FilterDAGInfoPtr & info, const StreamSettings & stream_settings, const MergeTreeData & storage, const ContextPtr & context)
{
    if (!info)
        return std::nullopt;

    return makeReadRoundFilter(info->actions, info->column_name, info->do_remove_column, stream_settings, storage, context);
}

std::optional<FilterDAGInfo> makeReadRoundPrewhereFilter(const PrewhereInfoPtr & info, const StreamSettings & stream_settings, const MergeTreeData & storage, const ContextPtr & context)
{
    if (!info)
        return std::nullopt;

    return makeReadRoundFilter(info->prewhere_actions, info->prewhere_column_name, info->remove_prewhere_column, stream_settings, storage, context);
}

Names makeColumnsToRead(Names columns, const std::optional<FilterDAGInfo> & row_level_filter, const std::optional<FilterDAGInfo> & prewhere_filter)
{
    for (const auto & aux_name : {PartitionIdColumn::name, BlockNumberColumn::name, BlockOffsetColumn::name})
        if (!std::ranges::contains(columns, aux_name))
            columns.push_back(aux_name);

    for (const auto * filter : {&row_level_filter, &prewhere_filter})
        if (filter->has_value())
            for (const auto & source_column : (*filter)->actions.getRequiredColumnsNames())
                if (!std::ranges::contains(columns, source_column))
                    columns.push_back(source_column);

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

        auto has_column = [&](const String & column) { return projection.sample_block.findColumnOrSubcolumnByName(column).has_value() || projection.metadata->virtuals.has(column); };
        if (std::ranges::all_of(columns, has_column))
            return &projection;
    }

    return nullptr;
}

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

    auto row_level_filter = makeReadRoundRowLevelFilter(query_info.row_level_filter, stream_settings, storage, context);
    auto prewhere_filter = makeReadRoundPrewhereFilter(query_info.prewhere_info, stream_settings, storage, context);
    auto columns_to_read = makeColumnsToRead(std::move(user_requested_columns), row_level_filter, prewhere_filter);

    const auto storage_metadata = storage.getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
    const auto streaming_metadata = extendMetadataWithStream(storage_metadata, stream_settings);
    const auto * projection = chooseCommitOrderProjection(*streaming_metadata, columns_to_read);

    return ReadRoundContext{
        .storage = storage,
        .query_info = makeStreamingSelectQueryInfo(query_info),
        .stream_settings = stream_settings,
        .row_level_filter = std::move(row_level_filter),
        .prewhere_filter = std::move(prewhere_filter),
        .context = makeStreamingContext(std::move(context), projection),
        .columns_to_read = std::move(columns_to_read),
        .requested_num_streams = requested_num_streams,
        .max_block_size = max_block_size,
        .output_header = std::move(output_header)};
}

}
