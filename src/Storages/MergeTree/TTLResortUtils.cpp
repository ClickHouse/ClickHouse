#include <Storages/MergeTree/TTLResortUtils.h>

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
#include <Storages/TTLDescription.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool compile_sort_description;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
}

bool groupByTTLAssignsSortKeyColumn(const StorageMetadataPtr & metadata_snapshot)
{
    if (!metadata_snapshot->hasSortingKey())
        return false;

    const auto group_by_ttls = metadata_snapshot->getGroupByTTLs();
    if (group_by_ttls.empty())
        return false;

    const auto storage_columns = metadata_snapshot->getColumns().getAllPhysical().getNameSet();
    const auto virtual_columns
        = metadata_snapshot->virtuals.getSampleBlock(VirtualsKind::All, VirtualsMaterializationPlace::Reader).getNameSet();

    /// Map each sorting-key dependency to its physical storage column (a dependency may be a
    /// subcolumn, e.g. `t.a` for `ORDER BY t.a`, whose storage column is `t`), so it can be
    /// compared with a `SET` target, which always names a physical column.
    NameSet sort_key_dependencies;
    for (const auto & column : metadata_snapshot->getSortingKey().expression->getRequiredColumns())
    {
        if (storage_columns.contains(column) || virtual_columns.contains(column))
            sort_key_dependencies.insert(column);
        else if (auto column_in_storage = Nested::tryGetColumnNameInStorage(column, storage_columns))
            sort_key_dependencies.insert(*column_in_storage);
    }

    for (const auto & ttl : group_by_ttls)
        for (const auto & set_part : ttl.set_parts)
            if (sort_key_dependencies.contains(set_part.column_name))
                return true;

    return false;
}

void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
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
    const auto & current_header = builder.getHeader();
    const auto storage_column_names = storage_columns.getNameSet();
    NameSet columns_to_recompute;
    for (const auto & name : sorting_key_expression_dag.getNames())
        if (!storage_column_names.contains(name))
            columns_to_recompute.insert(name);

    ActionsDAG drop_stale_dag(current_header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs kept_outputs;
    kept_outputs.reserve(drop_stale_dag.getOutputs().size());
    for (const auto * output : drop_stale_dag.getOutputs())
        if (!columns_to_recompute.contains(output->result_name))
            kept_outputs.push_back(output);
    drop_stale_dag.getOutputs() = std::move(kept_outputs);

    /// When the sorting key depends on a subcolumn (e.g. `ORDER BY t.a`), the stale `t.a`
    /// materialized before the TTL step is still in `current_header`. Hide the stale computed
    /// sort-key columns from the subcolumn extractor so it re-extracts them from the post-SET
    /// physical columns; otherwise it would treat the stale `t.a` as available, skip
    /// re-extraction, and the re-sort would key on the pre-SET value.
    Block header_for_extraction;
    for (const auto & column : current_header)
        if (!columns_to_recompute.contains(column.name))
            header_for_extraction.insert(column);

    auto extracting_subcolumns_dag = createSubcolumnsExtractionActions(
        header_for_extraction, sorting_key_expression_dag.getRequiredColumnsNames(), context);

    auto recalculate_sorting_key_dag = ActionsDAG::merge(
        std::move(drop_stale_dag),
        ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(sorting_key_expression_dag)));

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
        SortingStep::Settings(context->getSettingsRef()));
    sorting_step.transformPipeline(builder, BuildQueryPipelineSettings(context));
}

}
