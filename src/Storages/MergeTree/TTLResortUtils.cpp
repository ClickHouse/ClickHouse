#include <Storages/MergeTree/TTLResortUtils.h>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
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

namespace
{

/// The physical storage columns the `GROUP BY` TTLs `SET` (assignment targets are always physical).
NameSet getGroupByTTLSetTargets(const StorageMetadataPtr & metadata_snapshot)
{
    NameSet set_targets;
    for (const auto & ttl : metadata_snapshot->getGroupByTTLs())
        for (const auto & set_part : ttl.set_parts)
            set_targets.insert(set_part.column_name);
    return set_targets;
}

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

/// The source columns a MATERIALIZED column's default expression reads from, mapped to their
/// physical storage columns (the expression may reference a subcolumn). Analyzed the same way the
/// UPDATE mutation path does in `MutationsInterpreter::prepare`.
NameSet getMaterializedColumnSourceColumns(
    const ColumnDescription & column_desc,
    const NamesAndTypesList & all_columns,
    const NameSet & storage_columns,
    const ContextPtr & context)
{
    auto query = column_desc.default_desc.expression->clone();
    replaceSubcolumnsToGetSubcolumnFunctionInQuery(query, all_columns);
    auto syntax_result = TreeRewriter(context).analyze(query, all_columns);

    NameSet sources;
    for (const auto & source : syntax_result->requiredSourceColumns())
    {
        if (storage_columns.contains(source))
            sources.insert(source);
        else if (auto source_in_storage = Nested::tryGetColumnNameInStorage(source, storage_columns))
            sources.insert(*source_in_storage);
    }
    return sources;
}

}

NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    NamesAndTypesList affected;

    if (!metadata_snapshot->hasSortingKey() || metadata_snapshot->getGroupByTTLs().empty())
        return affected;

    const auto & columns_desc = metadata_snapshot->getColumns();
    const auto all_columns = columns_desc.getAllPhysical();
    const auto storage_columns = all_columns.getNameSet();
    const auto set_targets = getGroupByTTLSetTargets(metadata_snapshot);

    /// A sort-key dependency that is a MATERIALIZED column whose default expression reads a `SET`
    /// target is affected: the aggregation rewrites the source but leaves the stored value stale.
    for (const auto & dependency : getSortKeyStorageDependencies(metadata_snapshot))
    {
        if (!columns_desc.has(dependency))
            continue;

        const auto & column_desc = columns_desc.get(dependency);
        if (column_desc.default_desc.kind != ColumnDefaultKind::Materialized || !column_desc.default_desc.expression)
            continue;

        const auto sources = getMaterializedColumnSourceColumns(column_desc, all_columns, storage_columns, context);
        for (const auto & source : sources)
        {
            if (set_targets.contains(source))
            {
                affected.emplace_back(column_desc.name, column_desc.type);
                break;
            }
        }
    }

    return affected;
}

bool groupByTTLAssignsSortKeyColumn(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    if (!metadata_snapshot->hasSortingKey())
        return false;

    if (metadata_snapshot->getGroupByTTLs().empty())
        return false;

    const auto set_targets = getGroupByTTLSetTargets(metadata_snapshot);

    /// Direct case: a `SET` target is itself a sort-key dependency storage column.
    for (const auto & dependency : getSortKeyStorageDependencies(metadata_snapshot))
        if (set_targets.contains(dependency))
            return true;

    /// Materialized case: a `SET` target is a source of a MATERIALIZED sort-key column.
    return !getGroupByTTLSetAffectedMaterializedSortKeyColumns(metadata_snapshot, context).empty();
}

ActionsDAG buildRecomputeMaterializedColumnsDAG(
    const Block & header,
    const NamesAndTypesList & columns_to_recompute,
    const ColumnsDescription & columns_desc,
    const ContextPtr & context)
{
    NameSet recompute_names;
    for (const auto & column : columns_to_recompute)
        recompute_names.insert(column.name);

    /// Drop the stale values from the stream so `evaluateMissingDefaults` treats the columns as
    /// missing and recomputes them from their default expression (reading the post-`SET` sources
    /// that remain in the stream). Otherwise it would keep the stale value already present.
    ActionsDAG drop_stale_dag(header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs kept_outputs;
    kept_outputs.reserve(drop_stale_dag.getOutputs().size());
    for (const auto * output : drop_stale_dag.getOutputs())
        if (!recompute_names.contains(output->result_name))
            kept_outputs.push_back(output);
    drop_stale_dag.getOutputs() = std::move(kept_outputs);

    Block header_after_drop(drop_stale_dag.getResultColumns());

    /// Ask for every remaining stream column plus the recomputed ones, so the pass-through columns
    /// are preserved (`save_unneeded_columns`) and the materialized columns are re-evaluated.
    NamesAndTypesList required_columns;
    for (const auto & column : header_after_drop)
        required_columns.emplace_back(column.name, column.type);
    required_columns.insert(required_columns.end(), columns_to_recompute.begin(), columns_to_recompute.end());

    auto recompute_dag
        = evaluateMissingDefaults(header_after_drop, required_columns, columns_desc, context, /*save_unneeded_columns=*/true);
    if (!recompute_dag)
        return drop_stale_dag;

    return ActionsDAG::merge(std::move(drop_stale_dag), std::move(*recompute_dag));
}

void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context)
{
    /// If a MATERIALIZED sort-key column's source was rewritten by the `SET` (e.g.
    /// `d MATERIALIZED toDate(ts)`, `ORDER BY d`, `... SET ts = ...`), the stored `d` in the stream
    /// is stale. Recompute such columns from their default expression before recomputing the
    /// sorting-key expression and re-sorting, otherwise the re-sort would key on the stale value.
    auto materialized_sort_key_columns = getGroupByTTLSetAffectedMaterializedSortKeyColumns(metadata_snapshot, context);
    if (!materialized_sort_key_columns.empty())
    {
        auto recompute_dag = buildRecomputeMaterializedColumnsDAG(
            builder.getHeader(), materialized_sort_key_columns, metadata_snapshot->getColumns(), context);
        builder.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<ExpressionTransform>(
                header, std::make_shared<ExpressionActions>(recompute_dag.clone()));
        });
    }

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
