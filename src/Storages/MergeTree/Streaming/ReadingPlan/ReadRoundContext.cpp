#include <Storages/MergeTree/Streaming/ReadingPlan/ReadRoundContext.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/Streaming/Utils.h>

#include <Core/Streaming/StreamingVirtualColumns.h>

#include <algorithm>

namespace DB
{

namespace
{

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

    return columns;
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

    return ReadRoundContext{
        .storage = storage,
        .query_info = makeStreamingSelectQueryInfo(query_info),
        .prewhere_info = makeReadRoundPrewhereInfo(query_info.prewhere_info, stream_settings, storage, context),
        .row_level_filter = makeReadRoundRowLevelFilter(query_info.row_level_filter, stream_settings, storage, context),
        .stream_settings = stream_settings,
        .context = makeStreamingContext(std::move(context)),
        .user_requested_columns = filterStreamingVirtualColumns(std::move(user_requested_columns)),
        .requested_num_streams = requested_num_streams,
        .max_block_size = max_block_size,
        .output_header = std::move(output_header)};
}

}
