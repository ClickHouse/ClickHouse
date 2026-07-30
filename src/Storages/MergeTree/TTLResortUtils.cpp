#include <Storages/MergeTree/TTLResortUtils.h>

#include <optional>
#include <unordered_map>

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
/// UPDATE mutation path does in `MutationsInterpreter::prepare`. Returns nullopt when the default
/// expression reads an EPHEMERAL column: such a column cannot be recomputed here (ephemeral columns
/// are only available during INSERT, never read from disk during a merge/mutation), so it is
/// skipped instead of analyzed as recomputable.
std::optional<NameSet> getMaterializedColumnSourceColumns(
    const ColumnDescription & column_desc,
    const NamesAndTypesList & all_columns,
    const NameSet & storage_columns,
    const NameSet & ephemeral_columns,
    const ContextPtr & context)
{
    auto query = column_desc.default_desc.expression->clone();
    replaceSubcolumnsToGetSubcolumnFunctionInQuery(query, all_columns);
    auto syntax_result = TreeRewriter(context).analyze(query, all_columns);

    NameSet sources;
    for (const auto & source : syntax_result->requiredSourceColumns())
    {
        if (ephemeral_columns.contains(source))
            return std::nullopt;
        if (storage_columns.contains(source))
            sources.insert(source);
        else if (auto source_in_storage = Nested::tryGetColumnNameInStorage(source, storage_columns))
            sources.insert(*source_in_storage);
    }
    return sources;
}

/// The physical storage columns of every MATERIALIZED column, mapped to the physical storage
/// columns their default expression reads from. Used to walk materialized-dependency chains.
/// MATERIALIZED columns whose default expression reads an EPHEMERAL column are omitted: they cannot
/// be recomputed from on-disk data, because an ephemeral column is only available during INSERT.
/// Such a column may still read regular columns a `SET` rewrites, in which case its stored value
/// goes stale, matching the behaviour the ordinary mutation path already has for a mixed ephemeral
/// dependency (`04044_mutation_ephemeral_materialized`). The part stays ordered by the stored value,
/// so the primary index remains consistent with the data.
std::unordered_map<String, NameSet> getMaterializedColumnSourcesMap(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    const auto & columns_desc = metadata_snapshot->getColumns();
    const auto storage_columns = columns_desc.getAllPhysical().getNameSet();

    /// Include ephemeral columns in the analysis set so `TreeRewriter` can resolve MATERIALIZED
    /// expressions that reference them, mirroring `MutationsInterpreter::prepare`. Without this the
    /// analysis throws UNKNOWN_IDENTIFIER for a table such as `eph String EPHEMERAL, sk String
    /// MATERIALIZED reverse(eph)`.
    NamesAndTypesList all_columns_with_ephemeral = columns_desc.getAllPhysical();
    NameSet ephemeral_columns;
    for (const auto & column : columns_desc.getEphemeral())
    {
        ephemeral_columns.insert(column.name);
        all_columns_with_ephemeral.push_back(column);
    }

    std::unordered_map<String, NameSet> sources_map;
    for (const auto & column : columns_desc.getAllPhysical())
    {
        if (!columns_desc.has(column.name))
            continue;
        const auto & column_desc = columns_desc.get(column.name);
        if (column_desc.default_desc.kind != ColumnDefaultKind::Materialized || !column_desc.default_desc.expression)
            continue;
        auto sources = getMaterializedColumnSourceColumns(
            column_desc, all_columns_with_ephemeral, storage_columns, ephemeral_columns, context);
        if (sources)
            sources_map.emplace(column.name, std::move(*sources));
    }
    return sources_map;
}

/// Every MATERIALIZED column whose default expression (transitively) reads a column rewritten by
/// the `GROUP BY` TTL `SET`. A `SET` on a base column can invalidate a materialized column several
/// hops away (e.g. `x` fed to `y MATERIALIZED toDate(x)` fed to `z MATERIALIZED toYYYYMM(y)`), so
/// this is a fixpoint over the materialized-dependency graph, not a one-hop check.
NameSet getMaterializedColumnsAffectedBySet(
    const std::unordered_map<String, NameSet> & materialized_sources, const NameSet & set_targets)
{
    NameSet affected = set_targets;
    bool changed = true;
    while (changed)
    {
        changed = false;
        for (const auto & [name, sources] : materialized_sources)
        {
            if (affected.contains(name))
                continue;
            for (const auto & source : sources)
            {
                if (affected.contains(source))
                {
                    affected.insert(name);
                    changed = true;
                    break;
                }
            }
        }
    }

    /// Keep only the materialized columns; the seed `SET` targets are physical assigned columns.
    for (const auto & target : set_targets)
        affected.erase(target);
    return affected;
}

}

NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    NamesAndTypesList affected;

    if (!metadata_snapshot->hasSortingKey() || metadata_snapshot->getGroupByTTLs().empty())
        return affected;

    const auto & columns_desc = metadata_snapshot->getColumns();
    const auto set_targets = getGroupByTTLSetTargets(metadata_snapshot);
    const auto materialized_sources = getMaterializedColumnSourcesMap(metadata_snapshot, context);

    /// Every MATERIALIZED column (transitively) reading a `SET` target holds a stale stored value:
    /// the aggregation rewrites the source but nothing recomputes the materialized column.
    const auto affected_materialized = getMaterializedColumnsAffectedBySet(materialized_sources, set_targets);
    if (affected_materialized.empty())
        return affected;

    /// We only need to repair the ones the sorting key depends on, plus any affected MATERIALIZED
    /// columns those transitively read from (intermediate hops such as `y` in
    /// `y MATERIALIZED toDate(x)`, `z MATERIALIZED toYYYYMM(y)`, `ORDER BY z`). The intermediates
    /// must be recomputed too, otherwise recomputing the sort-key column would read their stale
    /// value. MATERIALIZED columns unrelated to the sorting key are left untouched (out of scope).
    NameSet to_recompute;
    NameSet frontier;
    for (const auto & dependency : getSortKeyStorageDependencies(metadata_snapshot))
        if (affected_materialized.contains(dependency))
            frontier.insert(dependency);

    while (!frontier.empty())
    {
        NameSet next;
        for (const auto & name : frontier)
        {
            if (!to_recompute.insert(name).second)
                continue;
            if (auto it = materialized_sources.find(name); it != materialized_sources.end())
                for (const auto & source : it->second)
                    if (affected_materialized.contains(source) && !to_recompute.contains(source))
                        next.insert(source);
        }
        frontier = std::move(next);
    }

    /// Return in physical column order so recomputation is deterministic.
    for (const auto & column : columns_desc.getAllPhysical())
        if (to_recompute.contains(column.name))
            affected.emplace_back(column.name, column.type);

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

    /// A default expression may read a subcolumn (e.g. `d MATERIALIZED toDate(tup.ts)` requires
    /// `tup.ts`), but the stream only carries the physical column `tup`. Prepend a subcolumn
    /// extraction DAG so `tup.ts` is available, exactly as `AddingDefaultsTransform` does before
    /// executing `evaluateMissingDefaults`; otherwise recomputation fails with
    /// NOT_FOUND_COLUMN_IN_BLOCK.
    auto extracting_subcolumns_dag
        = createSubcolumnsExtractionActions(header_after_drop, recompute_dag->getRequiredColumnsNames(), context);

    return ActionsDAG::merge(
        std::move(drop_stale_dag),
        ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(*recompute_dag)));
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
