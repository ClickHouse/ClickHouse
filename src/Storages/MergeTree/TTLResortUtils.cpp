#include <Storages/MergeTree/TTLResortUtils.h>

#include <vector>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/TTLDescription.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool compile_sort_description;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 ttl_resort_max_bytes_before_external_sort;
}

namespace
{

/// Map each sorting-key dependency to its physical storage column (a dependency may be a
/// subcolumn, e.g. `t.a` for `ORDER BY t.a`, whose storage column is `t`), so it can be compared
/// with a `SET` target, which always names a physical column.
NameSet getSortKeyStorageDependencies(const StorageMetadataPtr & metadata_snapshot)
{
    const auto storage_columns = metadata_snapshot->getColumns().getAllPhysical().getNameSet();
    const auto virtual_columns
        = metadata_snapshot->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader).getNameSet();

    NameSet sort_key_dependencies;
    for (const auto & column : metadata_snapshot->getSortingKey().expression->getRequiredColumns())
    {
        if (storage_columns.contains(column) || virtual_columns.contains(column))
            sort_key_dependencies.insert(column);
        else if (auto column_in_storage = Nested::tryGetColumnNameInStorage(column, storage_columns))
            sort_key_dependencies.insert(*column_in_storage);
    }
    return sort_key_dependencies;
}

}

bool groupByTTLAssignsSortKeyColumn(const StorageMetadataPtr & metadata_snapshot, const NameSet & set_targets)
{
    if (!metadata_snapshot->hasSortingKey())
        return false;

    if (metadata_snapshot->getGroupByTTLs().empty() || set_targets.empty())
        return false;

    for (const auto & dependency : getSortKeyStorageDependencies(metadata_snapshot))
        if (set_targets.contains(dependency))
            return true;
    return false;
}

NameSet getFiringGroupByTTLSetTargets(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time,
    bool force_ttl)
{
    /// With several `GROUP BY` TTLs in one part an earlier `SET` can rewrite a column a later TTL
    /// groups by, so that TTL aggregates an input no longer ordered by its keys and produces wrong
    /// groups; re-sorting the result would hide that behind a correctly ordered part.
    if (metadata_snapshot->getGroupByTTLs().size() > 1)
        return {};

    NameSet targets;
    for (const auto & group_by_ttl : metadata_snapshot->getGroupByTTLs())
    {
        auto it = ttl_infos.group_by_ttl.find(group_by_ttl.result_column);
        /// Missing info or uninitialized `min` -> conservatively assume it may fire. A forced merge
        /// does not imply this TTL fired: it may evaluate a future TTL without rewriting any row.
        bool fires = force_ttl || it == ttl_infos.group_by_ttl.end() || it->second.min == 0 || it->second.min <= current_time;
        if (fires)
            for (const auto & set_part : group_by_ttl.set_parts)
                targets.insert(set_part.column_name);
    }
    return targets;
}

SortingStep::Settings buildTTLResortSortingSettings(const ContextPtr & context, const MergeTreeSettings & storage_settings)
{
    SortingStep::Settings sort_settings(context->getSettingsRef());

    /// `max_rows_to_sort` / `max_bytes_to_sort` bound a user query's result. `SortingStep` enforces
    /// them with a `LimitsCheckingTransform`, which under `sort_overflow_mode = 'break'` stops
    /// reading and lets the writer commit a truncated part, so this maintenance sort clears them.
    sort_settings.size_limits = {};

    /// Background merge and mutation contexts inherit the default `max_bytes_before_external_sort = 0`
    /// (neither `Context::makeQueryContextForMerge` nor `makeQueryContextForMutate` overrides it),
    /// and `MergeSortingTransform` spills only when that threshold is non-zero, so as taken from the
    /// context the sort could never externalize and would buffer the whole post-TTL part in memory.
    /// Bound it by the table-level setting instead. The temporary storage is only available when the
    /// global context provides it (a server always does; skip the override otherwise, since a non-zero
    /// threshold without temporary storage is an error at pipeline build time).
    const UInt64 max_bytes_before_external_sort = storage_settings[MergeTreeSetting::ttl_resort_max_bytes_before_external_sort];

    /// The table setting is the whole bound: `0` means do not spill, so an inherited query threshold
    /// must not put spilling back either.
    sort_settings.max_bytes_in_block_before_external_sort = 0;
    sort_settings.max_bytes_in_query_before_external_sort = 0;
    if (max_bytes_before_external_sort && context->getSharedTempDataOnDisk())
        sort_settings.max_bytes_in_block_before_external_sort = max_bytes_before_external_sort;

    return sort_settings;
}

static ActionsDAG buildRecomputeSortKeyExpressionDAG(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context)
{
    /// Recompute the sorting-key expression columns from the post-SET values, overwriting the
    /// now-stale ones already materialized in the stream before the TTL step.
    const auto & sorting_key_expression = metadata_snapshot->getSortingKey().expression;
    auto sorting_key_expression_dag = sorting_key_expression->getActionsDAG().clone();

    /// Drop the stale materialized sort-key expression columns first; otherwise re-applying the
    /// expression would leave duplicate columns of the same name in the block. Only the computed
    /// (non-storage) sort-key columns are dropped: the storage columns the expression reads from
    /// (e.g. `ts`, `k`, or a `Tuple` column `t` whose subcolumn `t.a` is in the sorting key) must
    /// stay so they can feed the recomputation.
    const auto storage_column_names = storage_columns.getNameSet();
    NameSet columns_to_recompute;
    for (const auto & name : sorting_key_expression_dag.getNames())
        if (!storage_column_names.contains(name))
            columns_to_recompute.insert(name);

    ActionsDAG drop_stale_dag(header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs kept_outputs;
    kept_outputs.reserve(drop_stale_dag.getOutputs().size());
    for (const auto * output : drop_stale_dag.getOutputs())
        if (!columns_to_recompute.contains(output->result_name))
            kept_outputs.push_back(output);
    drop_stale_dag.getOutputs() = std::move(kept_outputs);

    /// When the sorting key depends on a subcolumn (e.g. `ORDER BY t.a`), the stale `t.a`
    /// materialized before the TTL step is still in `header`. Hide the stale computed
    /// sort-key columns from the subcolumn extractor so it re-extracts them from the post-SET
    /// physical columns; otherwise it would treat the stale `t.a` as available, skip
    /// re-extraction, and the re-sort would key on the pre-SET value.
    Block header_for_extraction;
    for (const auto & column : header)
        if (!columns_to_recompute.contains(column.name))
            header_for_extraction.insert(column);

    auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(
        header_for_extraction, sorting_key_expression_dag.getRequiredColumnsNames(), context);

    return ActionsDAG::merge(
        std::move(drop_stale_dag),
        ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(sorting_key_expression_dag)));
}

void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context,
    const MergeTreeSettings & storage_settings)
{
    auto recalculate_sorting_key_dag
        = buildRecomputeSortKeyExpressionDAG(builder.getHeader(), metadata_snapshot, storage_columns, context);

    builder.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<ExpressionTransform>(
            header, std::make_shared<ExpressionActions>(recalculate_sorting_key_dag.clone()));
    });

    SortDescription sort_description;
    {
        Names sort_columns = metadata_snapshot->getSortingKeyColumns();
        std::vector<bool> reverse_flags = metadata_snapshot->getSortingKeyReverseFlags();
        sort_description.compile_sort_description = context->getSettingsRef()[Setting::compile_sort_description];
        sort_description.min_count_to_compile_sort_description
            = context->getSettingsRef()[Setting::min_count_to_compile_sort_description];
        sort_description.reserve(sort_columns.size());
        for (size_t i = 0; i < sort_columns.size(); ++i)
        {
            if (!reverse_flags.empty() && reverse_flags[i])
                sort_description.emplace_back(sort_columns[i], -1, 1);
            else
                sort_description.emplace_back(sort_columns[i], 1, 1);
        }
    }

    SortingStep sorting_step(
        builder.getSharedHeader(),
        sort_description,
        /*limit_=*/0,
        buildTTLResortSortingSettings(context, storage_settings));
    sorting_step.transformPipeline(builder, BuildQueryPipelineSettings(context));
}

}
