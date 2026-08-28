#include <Storages/MergeTree/TTLResortUtils.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/replaceAliasColumnsInQuery.h>
#include <Interpreters/replaceSubcolumnsToGetSubcolumnFunctionInQuery.h>
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

/// Stored default and MATERIALIZED expressions must remain executable during a background merge
/// even when a feature used at table-creation time is disabled in the current context. Keep this
/// in sync with `IMergeTreeReader::createContextForDefaultExpressions`; chained materialized
/// subcolumns additionally require the analyzer for the expression DAG assembled here.
ContextPtr createContextForTTLDefaultExpressions(const ContextPtr & context)
{
    auto expressions_context = Context::createCopy(context);
    enableAllExperimentalSettings(expressions_context);
    expressions_context->setSetting("enable_analyzer", true);
    return expressions_context;
}

/// The physical storage columns the `GROUP BY` TTLs `SET` (assignment targets are always physical).
NameSet getGroupByTTLSetTargets(const StorageMetadataPtr & metadata_snapshot)
{
    NameSet set_targets;
    for (const auto & ttl : metadata_snapshot->getGroupByTTLs())
        for (const auto & set_part : ttl.set_parts)
            set_targets.insert(set_part.column_name);
    return set_targets;
}

/// The INPUT (leaf) column names of the sub-DAG rooted at the output node named `output_name`.
/// Used to obtain the source columns of ONE specific primary-key expression key (e.g. only
/// `toStartOfDay(ts)`), rather than of the whole primary-key expression. Returns nullopt when the
/// output node is absent (the caller then falls back to the whole-expression sources).
std::optional<NameSet> getExpressionOutputInputColumns(const ActionsDAG & dag, const String & output_name)
{
    const auto * root = dag.tryFindInOutputs(output_name);
    if (!root)
        return std::nullopt;

    NameSet inputs;
    std::vector<const ActionsDAG::Node *> stack{root};
    std::unordered_set<const ActionsDAG::Node *> seen;
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!seen.insert(node).second)
            continue;
        if (node->type == ActionsDAG::ActionType::INPUT)
            inputs.insert(node->result_name);
        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return inputs;
}

/// Map one `GROUP BY` key (a primary-key column NAME, which may be a computed expression such as
/// `toStartOfDay(ts)`, a subcolumn such as `t.a`, or a physical/MATERIALIZED column) to the physical
/// storage columns whose value it depends on. A physical column maps to itself; a MATERIALIZED
/// column additionally pulls in the (transitive) sources of its default expression; a computed key
/// pulls in the storage columns THAT KEY's primary-key expression node reads -- scoped to the
/// specific key node, so an unrelated `SET` on another sort-key column (e.g. `user_id` under
/// `ORDER BY (toStartOfDay(ts), user_id)` while grouping only by `toStartOfDay(ts)`) does not pull
/// that column into this key's dependencies.
NameSet getGroupByKeyStorageDependencies(
    const String & key,
    const StorageMetadataPtr & metadata_snapshot,
    const std::unordered_map<String, NameSet> & materialized_sources)
{
    const auto & columns_desc = metadata_snapshot->getColumns();
    const auto storage_names = columns_desc.getAllPhysical().getNameSet();

    /// Resolve a physical column to its own name plus, if MATERIALIZED, the transitive physical
    /// sources of its default expression.
    auto expand_physical = [&](const String & physical, NameSet & out)
    {
        NameSet frontier{physical};
        while (!frontier.empty())
        {
            NameSet next;
            for (const auto & name : frontier)
            {
                if (!out.insert(name).second)
                    continue;
                if (auto it = materialized_sources.find(name); it != materialized_sources.end())
                    for (const auto & source : it->second)
                        if (!out.contains(source))
                            next.insert(source);
            }
            frontier = std::move(next);
        }
    };

    auto expand_source = [&](const String & source, NameSet & out)
    {
        if (storage_names.contains(source))
            expand_physical(source, out);
        else if (auto source_in_storage = Nested::tryGetColumnNameInStorage(source, storage_names))
            expand_physical(*source_in_storage, out);
    };

    NameSet deps;
    if (storage_names.contains(key))
    {
        expand_physical(key, deps);
    }
    else if (auto key_in_storage = Nested::tryGetColumnNameInStorage(key, storage_names))
    {
        /// Subcolumn key such as `t.a` -> physical column `t`.
        expand_physical(*key_in_storage, deps);
    }
    else if (metadata_snapshot->hasPrimaryKey())
    {
        /// Computed key such as `toStartOfDay(ts)`: pull only the storage columns THIS key's
        /// primary-key expression node reads (mapped through storage for subcolumn reads). Scoping to
        /// the specific output node avoids flipping the fast path off for an unrelated `SET` on a
        /// sibling sort-key column.
        const auto & pk_dag = metadata_snapshot->getPrimaryKey().expression->getActionsDAG();
        if (auto key_sources = getExpressionOutputInputColumns(pk_dag, key))
        {
            for (const auto & source : *key_sources)
                expand_source(source, deps);
        }
        else
        {
            /// The key is not an output of the primary-key expression: fall back to the whole
            /// expression's sources (conservative -- correctness over the fast-path optimization).
            for (const auto & source : metadata_snapshot->getPrimaryKey().expression->getRequiredColumns())
                expand_source(source, deps);
        }
    }
    return deps;
}

/// The physical storage columns a `GROUP BY` TTL's expiry (and WHERE) expression reads. Used to
/// detect a chained interaction: a `TTLAggregationAlgorithm` decides which rows are expired from
/// these columns, so once an EARLIER firing `GROUP BY ... SET` rewrites one of them, this TTL's
/// precomputed `group_by_ttl.min` (computed over the UNMODIFIED part) is no longer a valid
/// "won't fire" proof -- the earlier `SET` can move this TTL from future to expired in the same
/// merge/mutation. A `SET` target is always a physical column, so subcolumn reads are mapped to
/// their parent for the comparison.
NameSet getGroupByTTLExpiryStorageColumns(const TTLDescription & ttl, const NameSet & storage_columns)
{
    NameSet result;
    auto add = [&](const NamesAndTypesList & columns)
    {
        for (const auto & column : columns)
        {
            if (storage_columns.contains(column.name))
                result.insert(column.name);
            else if (auto in_storage = Nested::tryGetColumnNameInStorage(column.name, storage_columns))
                result.insert(*in_storage);
        }
    };
    add(ttl.expression_columns);
    add(ttl.where_expression_columns);
    return result;
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

/// An ALIAS is computed on read and never stored, so its name cannot be resolved against the on-disk
/// columns a merge sees: it has to be replaced by the expression it stands for, cast to the alias
/// type, before the default expression is analyzed or evaluated here.
ASTPtr cloneDefaultWithAliasesReplaced(
    const ColumnDescription & column_desc, const ColumnsDescription & columns_desc, const ContextPtr & context)
{
    auto query = column_desc.default_desc.expression->clone();
    replaceAliasColumnsInQuery(query, columns_desc, {}, context);
    return query;
}

/// The source columns a MATERIALIZED column's default expression reads from, mapped to their
/// physical storage columns (the expression may reference a subcolumn). Analyzed the same way the
/// UPDATE mutation path does in `MutationsInterpreter::prepare`. Returns nullopt when the default
/// expression reads an EPHEMERAL column: such a column cannot be recomputed here (ephemeral columns
/// are only available during INSERT, never read from disk during a merge/mutation), so it is
/// skipped instead of analyzed as recomputable.
std::optional<NameSet> getMaterializedColumnSourceColumns(
    const ColumnDescription & column_desc,
    const ColumnsDescription & columns_desc,
    const NamesAndTypesList & all_columns,
    const NameSet & storage_columns,
    const NameSet & ephemeral_columns,
    const ContextPtr & context)
{
    auto query = cloneDefaultWithAliasesReplaced(column_desc, columns_desc, context);
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
    const auto expressions_context = createContextForTTLDefaultExpressions(context);
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
            column_desc, columns_desc, all_columns_with_ephemeral, storage_columns, ephemeral_columns, expressions_context);
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

Names getStaleEphemeralMaterializedColumnsAffectedBySet(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    return getStaleEphemeralMaterializedColumnsAffectedBySet(
        metadata_snapshot, context, getGroupByTTLSetTargets(metadata_snapshot));
}

Names getStaleEphemeralMaterializedColumnsAffectedBySet(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets)
{
    const auto expressions_context = createContextForTTLDefaultExpressions(context);
    Names stale;

    if (metadata_snapshot->getGroupByTTLs().empty() || set_targets.empty())
        return stale;

    const auto & columns_desc = metadata_snapshot->getColumns();
    const auto storage_columns = columns_desc.getAllPhysical().getNameSet();

    NamesAndTypesList all_columns_with_ephemeral = columns_desc.getAllPhysical();
    NameSet ephemeral_columns;
    for (const auto & column : columns_desc.getEphemeral())
    {
        ephemeral_columns.insert(column.name);
        all_columns_with_ephemeral.push_back(column);
    }

    /// A MATERIALIZED column whose default expression (transitively) reads an EPHEMERAL column cannot be
    /// recomputed here (ephemeral columns exist only during INSERT, never on disk). If such a column is
    /// ALSO (transitively) affected by a column rewritten by the `GROUP BY` TTL `SET`, its stored value
    /// goes stale and there is no way to refresh it -- mirror `MutationsInterpreter::prepare`'s handling
    /// for `UPDATE` and warn (fail loud) instead of silently treating it as unaffected.
    ///
    /// The dependency is transitive on BOTH axes, so a one-hop check misses real cases. For
    /// `m1 MATERIALIZED concat(toString(x), eph)`, `m2 MATERIALIZED lower(m1)`, `SET x = ...`:
    ///  - `m1` reads the ephemeral `eph` and the `SET` target `x` directly.
    ///  - `m2` reads neither directly, but it depends on `m1`, which is both unrecomputable (ephemeral)
    ///    and affected by the `SET` -- so `m2`'s stored value is stale AND unrecomputable too.
    /// Build the FULL materialized-dependency graph (unlike `getMaterializedColumnSourcesMap`, KEEP the
    /// ephemeral-reading columns as nodes so the chain is not cut at the first ephemeral hop), then take
    /// the two transitive closures and report their intersection.
    std::unordered_map<String, NameSet> full_sources;  /// materialized col -> its physical/materialized source columns
    NameSet reads_ephemeral_directly;                  /// materialized cols whose own expression reads an ephemeral column
    for (const auto & column : columns_desc.getAllPhysical())
    {
        if (!columns_desc.has(column.name))
            continue;
        const auto & column_desc = columns_desc.get(column.name);
        if (column_desc.default_desc.kind != ColumnDefaultKind::Materialized || !column_desc.default_desc.expression)
            continue;

        auto query = cloneDefaultWithAliasesReplaced(column_desc, columns_desc, expressions_context);
        replaceSubcolumnsToGetSubcolumnFunctionInQuery(query, all_columns_with_ephemeral);
        auto syntax_result = TreeRewriter(expressions_context).analyze(query, all_columns_with_ephemeral);

        NameSet sources;
        for (const auto & source : syntax_result->requiredSourceColumns())
        {
            if (ephemeral_columns.contains(source))
                reads_ephemeral_directly.insert(column.name);
            else if (storage_columns.contains(source))
                sources.insert(source);
            else if (auto source_in_storage = Nested::tryGetColumnNameInStorage(source, storage_columns))
                sources.insert(*source_in_storage);
        }
        full_sources.emplace(column.name, std::move(sources));
    }

    /// A materialized column is "unrecomputable" if it or any of its (transitive) materialized sources
    /// reads an ephemeral column.
    NameSet unrecomputable = reads_ephemeral_directly;
    bool changed = true;
    while (changed)
    {
        changed = false;
        for (const auto & [name, sources] : full_sources)
        {
            if (unrecomputable.contains(name))
                continue;
            for (const auto & source : sources)
                if (unrecomputable.contains(source))
                {
                    unrecomputable.insert(name);
                    changed = true;
                    break;
                }
        }
    }

    /// Materialized columns (transitively) reading a `SET` target hold a stale stored value. Reuse the
    /// same fixpoint the recompute path uses, but over the FULL graph so it also propagates through the
    /// ephemeral-reading nodes.
    const auto affected = getMaterializedColumnsAffectedBySet(full_sources, set_targets);

    for (const auto & column : columns_desc.getAllPhysical())
        if (affected.contains(column.name) && unrecomputable.contains(column.name))
            stale.push_back(column.name);
    return stale;
}

NamesAndTypesList getGroupByTTLSetAffectedMaterializedColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    return getGroupByTTLSetAffectedMaterializedColumns(
        metadata_snapshot, context, getGroupByTTLSetTargets(metadata_snapshot));
}

NamesAndTypesList getGroupByTTLSetAffectedMaterializedColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets)
{
    const auto expressions_context = createContextForTTLDefaultExpressions(context);
    NamesAndTypesList affected;

    if (metadata_snapshot->getGroupByTTLs().empty() || set_targets.empty())
        return affected;

    const auto & columns_desc = metadata_snapshot->getColumns();
    const auto materialized_sources = getMaterializedColumnSourcesMap(metadata_snapshot, context);

    /// Every MATERIALIZED column (transitively) reading a `SET` target holds a stale stored value:
    /// the aggregation rewrites the source but nothing recomputes the materialized column.
    const auto affected_materialized = getMaterializedColumnsAffectedBySet(materialized_sources, set_targets);

    /// Return in physical column order so recomputation is deterministic (chained defaults such as
    /// `y MATERIALIZED toDate(x)`, `z MATERIALIZED toYYYYMM(y)` are all present, and
    /// `evaluateMissingDefaults` resolves the dependency order between them).
    for (const auto & column : columns_desc.getAllPhysical())
        if (affected_materialized.contains(column.name))
        {
            auto default_ast = cloneDefaultWithAliasesReplaced(columns_desc.get(column.name), columns_desc, expressions_context);
            const auto syntax_result = TreeRewriter(expressions_context).analyze(default_ast, columns_desc.getAll());
            const auto default_actions = ExpressionAnalyzer{default_ast, syntax_result, expressions_context}.getActions(true);

            /// A `GROUP BY` TTL can aggregate some rows while passing other rows through unchanged.
            /// The post-TTL repair is a whole-stream expression, so it cannot safely recompute a
            /// non-deterministic MATERIALIZED default (such as `now()`) only for the rows the SET
            /// actually rewrote. Preserve its stored value instead, as we do for defaults that read
            /// EPHEMERAL columns, rather than writing a fresh unrelated value for untouched rows.
            if (default_actions->getActionsDAG().hasNonDeterministic())
                continue;

            affected.emplace_back(column.name, column.type);
        }

    return affected;
}

NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    return getGroupByTTLSetAffectedMaterializedSortKeyColumns(
        metadata_snapshot, context, getGroupByTTLSetTargets(metadata_snapshot));
}

NamesAndTypesList getGroupByTTLSetAffectedMaterializedSortKeyColumns(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets)
{
    NamesAndTypesList affected;

    if (!metadata_snapshot->hasSortingKey() || metadata_snapshot->getGroupByTTLs().empty() || set_targets.empty())
        return affected;

    const auto & columns_desc = metadata_snapshot->getColumns();
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

static ActionsDAG buildRecomputeSortKeyExpressionDAG(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context);

bool groupByKeysAffectedByEarlierSet(
    const Names & group_by_keys,
    const NameSet & earlier_set_targets,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context)
{
    if (earlier_set_targets.empty())
        return false;

    const auto materialized_sources = getMaterializedColumnSourcesMap(metadata_snapshot, context);
    for (const auto & key : group_by_keys)
    {
        auto deps = getGroupByKeyStorageDependencies(key, metadata_snapshot, materialized_sources);
        for (const auto & dep : deps)
            if (earlier_set_targets.contains(dep))
                return true;
    }
    return false;
}

bool groupByTTLExpiryAffectedByEarlierSet(
    const TTLDescription & group_by_ttl,
    const NameSet & earlier_set_targets,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context)
{
    if (earlier_set_targets.empty())
        return false;

    /// A `SET` target is always a physical column, but this TTL's expiry can read it INDIRECTLY through a
    /// MATERIALIZED column: e.g. `d MATERIALIZED toDate(ts2)`, expiry `d + 1d`, earlier `SET ts2 = ...`.
    /// The stored `d` is stale after the `SET`, so its expiry proof is void just as if the expiry read
    /// `ts2` directly. Compare the expiry storage columns against the earlier `SET` targets PLUS every
    /// MATERIALIZED column (transitively) derived from them, not only the direct targets.
    const auto materialized_sources = getMaterializedColumnSourcesMap(metadata_snapshot, context);
    NameSet affected = getMaterializedColumnsAffectedBySet(materialized_sources, earlier_set_targets);
    affected.insert(earlier_set_targets.begin(), earlier_set_targets.end());

    const auto storage_columns = metadata_snapshot->getColumns().getAllPhysical().getNameSet();
    for (const auto & expiry_column : getGroupByTTLExpiryStorageColumns(group_by_ttl, storage_columns))
        if (affected.contains(expiry_column))
            return true;
    return false;
}

bool groupByTTLSetExpressionsAffectedByEarlierSet(
    const TTLDescription & group_by_ttl,
    const NameSet & earlier_set_targets,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context)
{
    if (earlier_set_targets.empty())
        return false;

    const auto materialized_sources = getMaterializedColumnSourcesMap(metadata_snapshot, context);
    const auto affected_materialized = getMaterializedColumnsAffectedBySet(materialized_sources, earlier_set_targets);
    for (const auto & set_part : group_by_ttl.set_parts)
        for (const auto * input : set_part.expression->getActionsDAG().getInputs())
            if (affected_materialized.contains(input->result_name))
                return true;
    return false;
}

std::optional<ActionsDAG> buildRefreshGroupByKeysDAG(
    const Block & header,
    const StorageMetadataPtr & metadata_snapshot,
    const TTLDescription & group_by_ttl,
    const NameSet & earlier_set_targets,
    const ContextPtr & context)
{
    if (!metadata_snapshot->hasPrimaryKey())
        return std::nullopt;

    const auto & group_by_keys = group_by_ttl.group_by_keys;
    const auto & columns_desc = metadata_snapshot->getColumns();
    const auto storage_names = columns_desc.getAllPhysical().getNameSet();
    /// Only earlier TTLs that actually fired can have made this TTL's derived columns stale.
    /// Including every table-level SET target here may recompute an unrelated MATERIALIZED column
    /// from a later or future TTL while merely refreshing this TTL's key or expiry input.
    const auto & set_targets = earlier_set_targets;
    const auto materialized_sources = getMaterializedColumnSourcesMap(metadata_snapshot, context);
    const auto affected_materialized = getMaterializedColumnsAffectedBySet(materialized_sources, set_targets);

    /// Split this TTL's group_by keys (which are primary-key column NAMES) into the two derived
    /// forms whose in-stream value goes stale after an earlier `SET`:
    ///  - `expression_keys`: computed or subcolumn keys such as `toStartOfDay(ts)` or `t.a` -- not a
    ///    physical column, recomputed from the primary-key expression.
    ///  - a physical MATERIALIZED column used as a key (e.g. `ORDER BY d` with
    ///    `d MATERIALIZED toDate(ts)`, `SET ts = ...`) -- recomputed from its default expression.
    /// A plain physical key (including the `SET` target itself, which the earlier aggregation already
    /// rewrote in the block) needs no refresh.
    ///
    /// `needs_materialized_refresh` must also be set when a computed/subcolumn key merely READS an
    /// affected MATERIALIZED column (e.g. `ORDER BY toStartOfMonth(d)`, `d MATERIALIZED toDate(ts)`,
    /// `SET ts = ...`): recomputing `toStartOfMonth(d)` from the stale stored `d` would still group by
    /// the pre-`SET` month, so the affected MATERIALIZED sources must be refreshed FIRST, then the
    /// expression key rebuilt from the fresh values.
    NameSet expression_keys;
    bool needs_materialized_refresh = false;
    for (const auto & key : group_by_keys)
    {
        if (storage_names.contains(key))
        {
            if (affected_materialized.contains(key))
                needs_materialized_refresh = true;
        }
        else
        {
            expression_keys.insert(key);
            for (const auto & dep : getGroupByKeyStorageDependencies(key, metadata_snapshot, materialized_sources))
                if (affected_materialized.contains(dep))
                    needs_materialized_refresh = true;
        }
    }

    /// This TTL's expiry/`WHERE` expression may also read a MATERIALIZED column derived from a `SET`
    /// target (e.g. `d MATERIALIZED toDate(ts2)`, expiry `d + 1d`, earlier `SET ts2`). The in-stream `d`
    /// is stale, so `isTTLExpired` would read the pre-`SET` value and wrongly skip aggregation. Refresh
    /// the affected MATERIALIZED columns first so the expiry is evaluated on post-`SET` values.
    for (const auto & expiry_column : getGroupByTTLExpiryStorageColumns(group_by_ttl, storage_names))
        if (affected_materialized.contains(expiry_column))
            needs_materialized_refresh = true;

    /// A later `SET` expression can aggregate a MATERIALIZED column too. It is evaluated by
    /// `TTLAggregationAlgorithm` after this refresh, so rebuild every affected MATERIALIZED input
    /// before the aggregate sees a stale value from an earlier `SET`.
    for (const auto & set_part : group_by_ttl.set_parts)
        for (const auto * input : set_part.expression->getActionsDAG().getInputs())
            if (affected_materialized.contains(input->result_name))
                needs_materialized_refresh = true;

    /// This TTL's expiry/`WHERE` can also read a SUBCOLUMN of a rewritten physical parent, not only a
    /// MATERIALIZED column: e.g. `ORDER BY tup.ts` pre-extracts `tup.ts` into the stream, earlier
    /// `SET tup = ...` rewrites the parent `tup`, and `TTL2 tup.ts + 1d` reads that pre-extracted
    /// `tup.ts`. The subcolumn is neither a physical `SET` target nor a MATERIALIZED column, so the two
    /// refreshes above miss it; `executeExpressionAndGetColumn` then prefers the stale in-stream
    /// `tup.ts` (via `getColumnOrSubcolumnByName`) over re-extracting it from the post-`SET` `tup`, and
    /// the TTL evaluates expiry/`WHERE` on the pre-`SET` value (skips aggregation or fires on wrong
    /// rows). Collect such stale subcolumns so they are dropped and re-extracted fresh below. Parent
    /// rewritten by an earlier `SET` directly (a `SET` target) OR indirectly (an affected MATERIALIZED
    /// column, refreshed by the step above before this re-extraction runs). Subcolumn-granular: only a
    /// subcolumn actually read by this TTL's expiry/`WHERE` whose parent was rewritten is stale -- an
    /// unrelated pass-through subcolumn of the same parent must be kept (dropping it would make the
    /// later `TTLAggregationAlgorithm` throw NOT_FOUND_COLUMN_IN_BLOCK).
    NameSet stale_expiry_subcolumns;
    auto collect_stale_expiry_subcolumns = [&](const NamesAndTypesList & expiry_columns)
    {
        for (const auto & column : expiry_columns)
        {
            if (storage_names.contains(column.name))
                continue;
            auto parent = Nested::tryGetColumnNameInStorage(column.name, storage_names);
            if (!parent)
                continue;
            if (set_targets.contains(*parent) || affected_materialized.contains(*parent))
                if (header.has(column.name) && header.has(*parent))
                    stale_expiry_subcolumns.insert(column.name);
        }
    };
    collect_stale_expiry_subcolumns(group_by_ttl.expression_columns);
    collect_stale_expiry_subcolumns(group_by_ttl.where_expression_columns);

    if (expression_keys.empty() && !needs_materialized_refresh && stale_expiry_subcolumns.empty())
        return std::nullopt;

    std::optional<ActionsDAG> result;

    /// Refresh affected MATERIALIZED columns first (the full transitive set, so a materialized key
    /// computed from another materialized column is fed fresh inputs), then recompute the
    /// expression keys from them and the post-`SET` physical columns.
    if (needs_materialized_refresh)
    {
        auto affected_materialized_columns = getGroupByTTLSetAffectedMaterializedColumns(metadata_snapshot, context, set_targets);
        if (!affected_materialized_columns.empty())
            result = buildRecomputeMaterializedColumnsDAG(header, affected_materialized_columns, columns_desc, context);
    }

    if (!expression_keys.empty())
    {
        const Block & expr_input_header = result ? Block(result->getResultColumns()) : header;

        /// A `GROUP BY` key is a prefix of the primary key, which is a prefix of the sorting key, so
        /// a computed/subcolumn key such as `toStartOfDay(ts)` or `t.a` IS a sorting-key expression
        /// column. Reuse the sorting-key recompute: it recomputes every sorting-key expression column
        /// from the post-`SET` storage columns (extracting subcolumns as needed) while passing through
        /// every other stream column, so the refreshed key is available and no source column is lost.
        auto recompute_keys_dag = buildRecomputeSortKeyExpressionDAG(
            expr_input_header, metadata_snapshot, columns_desc.getAllPhysical(), context);

        if (result)
            result = ActionsDAG::merge(std::move(*result), std::move(recompute_keys_dag));
        else
            result = std::move(recompute_keys_dag);
    }

    /// Drop and re-extract the stale expiry/`WHERE` subcolumns so this TTL's expiry is evaluated on the
    /// post-`SET` parent. The stale copies were extracted before the TTL step (from the pre-`SET`
    /// parent), so drop them from the stream and re-extract fresh from the now-rewritten physical
    /// parent (which the earlier aggregation already rewrote in the block, or the materialized refresh
    /// above rebuilt). Done last so a subcolumn of a MATERIALIZED parent reads the refreshed value.
    if (!stale_expiry_subcolumns.empty())
    {
        const Block & drop_input_header = result ? Block(result->getResultColumns()) : header;

        /// Drop the stale subcolumns so they are missing from the stream; the re-extraction below then
        /// rebuilds them from the post-`SET` parent instead of reusing the pre-`SET` copy.
        ActionsDAG drop_stale_dag(drop_input_header.getColumnsWithTypeAndName());
        ActionsDAG::NodeRawConstPtrs kept_outputs;
        kept_outputs.reserve(drop_stale_dag.getOutputs().size());
        for (const auto * output : drop_stale_dag.getOutputs())
            if (!stale_expiry_subcolumns.contains(output->result_name))
                kept_outputs.push_back(output);
        drop_stale_dag.getOutputs() = std::move(kept_outputs);

        Block header_after_drop(drop_stale_dag.getResultColumns());
        Names required_subcolumns(stale_expiry_subcolumns.begin(), stale_expiry_subcolumns.end());
        auto extract_dag = createSubcolumnsExtractionActions(header_after_drop, required_subcolumns, context);

        auto refresh_subcolumns_dag = ActionsDAG::merge(std::move(drop_stale_dag), std::move(extract_dag));
        if (result)
            result = ActionsDAG::merge(std::move(*result), std::move(refresh_subcolumns_dag));
        else
            result = std::move(refresh_subcolumns_dag);
    }

    return result;
}

bool groupByTTLAssignsSortKeyColumn(const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context)
{
    return groupByTTLAssignsSortKeyColumn(
        metadata_snapshot, context, getGroupByTTLSetTargets(metadata_snapshot));
}

bool groupByTTLAssignsSortKeyColumn(
    const StorageMetadataPtr & metadata_snapshot, const ContextPtr & context, const NameSet & set_targets)
{
    if (!metadata_snapshot->hasSortingKey())
        return false;

    if (metadata_snapshot->getGroupByTTLs().empty() || set_targets.empty())
        return false;

    /// Direct case: a `SET` target is itself a sort-key dependency storage column.
    for (const auto & dependency : getSortKeyStorageDependencies(metadata_snapshot))
        if (set_targets.contains(dependency))
            return true;

    /// Materialized case: a `SET` target is a source of a MATERIALIZED sort-key column.
    return !getGroupByTTLSetAffectedMaterializedSortKeyColumns(metadata_snapshot, context, set_targets).empty();
}

NameSet getFiringGroupByTTLSetTargets(
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time,
    const ContextPtr & context)
{
    /// Forward pass in TTL order (the same order `TTLTransform` runs the algorithms). A later TTL's
    /// precomputed `min` proves "won't fire" only for the UNMODIFIED part; once an earlier firing
    /// `GROUP BY ... SET` rewrites a column this TTL's expiry expression reads, that proof is void and
    /// the TTL may now fire in this merge. Treat such a later TTL as firing (conservative) so its `SET`
    /// targets are included and the merge repairs below run for the columns it actually rewrites.
    NameSet targets;
    for (const auto & group_by_ttl : metadata_snapshot->getGroupByTTLs())
    {
        auto it = ttl_infos.group_by_ttl.find(group_by_ttl.result_column);
        /// Missing info or uninitialized `min` -> conservatively assume it may fire. A forced merge
        /// does not imply this TTL fired: it may evaluate a future TTL without rewriting any row.
        bool fires = it == ttl_infos.group_by_ttl.end() || it->second.min == 0 || it->second.min <= current_time;
        if (!fires)
            fires = groupByTTLExpiryAffectedByEarlierSet(group_by_ttl, targets, metadata_snapshot, context);
        if (fires)
            for (const auto & set_part : group_by_ttl.set_parts)
                targets.insert(set_part.column_name);
    }
    return targets;
}

ActionsDAG buildRecomputeMaterializedColumnsDAG(
    const Block & header,
    const NamesAndTypesList & columns_to_recompute,
    const ColumnsDescription & columns_desc,
    const ContextPtr & context)
{
    const auto expressions_context = createContextForTTLDefaultExpressions(context);
    NameSet recompute_names;
    for (const auto & column : columns_to_recompute)
        recompute_names.insert(column.name);

    /// A MATERIALIZED default expression may read a subcolumn (e.g. `d MATERIALIZED toDate(tup.ts)`
    /// requires `tup.ts`). That subcolumn can already be materialized in the stream: for
    /// `ORDER BY tup.ts`, `add_primary_key_expression` extracts `tup.ts` before the TTL step. After
    /// an earlier `SET tup = ...` that pre-extracted `tup.ts` is STALE -- the physical parent `tup`
    /// was rewritten but the derived subcolumn still holds its pre-`SET` value.
    /// `createSubcolumnsExtractionActions` returns early when the required subcolumn is already in the
    /// stream, so it would reuse the stale copy and recompute the MATERIALIZED column from the
    /// pre-`SET` subcolumn. Such a stale subcolumn must be dropped so it is rebuilt fresh from the
    /// post-`SET` physical column below.
    ///
    /// Drop ONLY the subcolumns that feed the recompute -- not every re-extractable subcolumn in the
    /// stream. An unrelated pass-through subcolumn (e.g. a sort-key column `t.a` with
    /// `ORDER BY (d, t.a)` while only `d` is recomputed) is preserved by `save_unneeded_columns`, but
    /// this DAG never restores it, so dropping it would make the later `TTLAggregationAlgorithm` throw
    /// NOT_FOUND_COLUMN_IN_BLOCK.
    ///
    /// A re-extractable subcolumn is stale (and must be re-extracted from the post-`SET` physical
    /// parent) only when the recompute actually READS that specific subcolumn from a rewritten
    /// physical parent: e.g. `d MATERIALIZED toDate(tup.ts)` reads `tup.ts`, whose parent `tup` was
    /// rewritten, so the pre-extracted `tup.ts` is stale. A DIFFERENT subcolumn of the same parent
    /// that the recompute does NOT read (e.g. a pass-through sort-key column `t.a` with
    /// `ORDER BY (d, t.a)`, `d MATERIALIZED toDate(t.b)`) is unrelated and must be kept -- dropping it
    /// by physical-parent name would make the later `TTLAggregationAlgorithm` throw
    /// NOT_FOUND_COLUMN_IN_BLOCK, since this DAG never restores it. Track the SPECIFIC stale
    /// subcolumns the recompute needs (subcolumn-granular), not the physical parent.
    const auto storage_names = columns_desc.getAllPhysical().getNameSet();

    /// The exact subcolumns each recomputed MATERIALIZED column's default expression reads (analyzed
    /// the same way the mutation path does). Only such a subcolumn -- present in the stream and
    /// re-extractable from its post-`SET` physical parent -- can be a stale input to re-extract.
    NamesAndTypesList all_columns_with_ephemeral = columns_desc.getAllPhysical();
    NameSet ephemeral_columns;
    for (const auto & column : columns_desc.getEphemeral())
    {
        ephemeral_columns.insert(column.name);
        all_columns_with_ephemeral.push_back(column);
    }
    NameSet recompute_read_subcolumns;
    for (const auto & name : recompute_names)
    {
        if (!columns_desc.has(name))
            continue;
        const auto & column_desc = columns_desc.get(name);
        if (column_desc.default_desc.kind != ColumnDefaultKind::Materialized || !column_desc.default_desc.expression)
            continue;

        /// Analyze the default expression as written (do NOT rewrite subcolumn reads to
        /// `getSubcolumn(parent, ...)` first): a rewritten `tup.ts` collapses to its physical parent
        /// `tup` in `requiredSourceColumns()`, hiding the subcolumn we must drop. Analyzed as-is, a
        /// subcolumn read such as `tup.ts` in `d MATERIALIZED toDate(tup.ts)` is reported by its
        /// subcolumn name, so it is recognised and dropped below.
        auto query = cloneDefaultWithAliasesReplaced(column_desc, columns_desc, expressions_context);
        auto syntax_result = TreeRewriter(expressions_context).analyze(query, all_columns_with_ephemeral);
        for (const auto & source : syntax_result->requiredSourceColumns())
            /// A subcolumn source is one that is not itself a physical column but maps to one.
            if (!storage_names.contains(source))
                if (Nested::tryGetColumnNameInStorage(source, storage_names))
                    recompute_read_subcolumns.insert(source);
    }

    NameSet drop_names = recompute_names;
    for (const auto & column : header)
        if (recompute_read_subcolumns.contains(column.name))
            if (auto parent = Nested::tryGetColumnNameInStorage(column.name, storage_names); parent && header.has(*parent))
                drop_names.insert(column.name);

    /// Drop the stale values from the stream so `evaluateMissingDefaults` treats the columns as
    /// missing and recomputes them from their default expression (reading the post-`SET` sources
    /// that remain in the stream). Otherwise it would keep the stale value already present.
    ActionsDAG drop_stale_dag(header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs kept_outputs;
    kept_outputs.reserve(drop_stale_dag.getOutputs().size());
    for (const auto * output : drop_stale_dag.getOutputs())
        if (!drop_names.contains(output->result_name))
            kept_outputs.push_back(output);
    drop_stale_dag.getOutputs() = std::move(kept_outputs);

    Block header_after_drop(drop_stale_dag.getResultColumns());

    /// Ask for every remaining stream column plus the recomputed ones, so the pass-through columns
    /// are preserved (`save_unneeded_columns`) and the materialized columns are re-evaluated.
    NamesAndTypesList required_columns;
    for (const auto & column : header_after_drop)
        required_columns.emplace_back(column.name, column.type);
    required_columns.insert(required_columns.end(), columns_to_recompute.begin(), columns_to_recompute.end());

    /// A recomputed default may read a subcolumn of another recomputed column (`z MATERIALIZED
    /// toYYYYMM(y.d)` over MATERIALIZED `y`): both are dropped above, so `y.d` is resolvable only
    /// against the sibling `y` built in the same expression list, which the old analyzer cannot do.
    auto recompute_dag = evaluateMissingDefaults(
        header_after_drop, required_columns, columns_desc, expressions_context, /*save_unneeded_columns=*/true);
    if (!recompute_dag)
        return drop_stale_dag;

    /// Prepend a subcolumn extraction DAG so a required subcolumn (`tup.ts`) is available, exactly as
    /// `AddingDefaultsTransform` does before executing `evaluateMissingDefaults`; otherwise
    /// recomputation fails with NOT_FOUND_COLUMN_IN_BLOCK. The stale copies were dropped above, so a
    /// re-extractable subcolumn is now missing from `header_after_drop` and is rebuilt fresh from its
    /// post-`SET` physical parent.
    auto extracting_subcolumns_dag
        = createSubcolumnsExtractionActions(header_after_drop, recompute_dag->getRequiredColumnsNames(), expressions_context);

    auto result = ActionsDAG::merge(
        std::move(drop_stale_dag),
        ActionsDAG::merge(std::move(extracting_subcolumns_dag), std::move(*recompute_dag)));

    /// A constant-folded default expression yields a ColumnConst, which the part writer cannot
    /// serialize.
    result.addMaterializingOutputActions(/*materialize_sparse=*/false);
    return result;
}

SortingStep::Settings buildTTLResortSortingSettings(const ContextPtr & context, const MergeTreeSettings & storage_settings)
{
    SortingStep::Settings sort_settings(context->getSettingsRef());

    /// Background merge and mutation contexts inherit the default `max_bytes_before_external_sort = 0`
    /// (neither `Context::makeQueryContextForMerge` nor `makeQueryContextForMutate` overrides it),
    /// and `MergeSortingTransform` spills only when that threshold is non-zero, so as taken from the
    /// context the sort could never externalize and would buffer the whole post-TTL part in memory.
    /// Bound it by the table-level setting instead. The temporary storage is only available when the
    /// global context provides it (a server always does; skip the override otherwise, since a non-zero
    /// threshold without temporary storage is an error at pipeline build time).
    const UInt64 max_bytes_before_external_sort = storage_settings[MergeTreeSetting::ttl_resort_max_bytes_before_external_sort];
    if (max_bytes_before_external_sort && context->getSharedTempDataOnDisk())
    {
        sort_settings.max_bytes_in_block_before_external_sort = max_bytes_before_external_sort;
        /// The query-memory gate (derived from `max_bytes_ratio_before_external_sort`, half of the
        /// available server memory by default) would delay the spill until this merge or mutation
        /// alone uses that much memory. Disable it so the threshold above is the actual bound.
        sort_settings.max_bytes_in_query_before_external_sort = 0;
    }

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

bool recomputeAffectedMaterializedColumns(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context)
{
    return recomputeAffectedMaterializedColumns(
        builder, metadata_snapshot, context, getGroupByTTLSetTargets(metadata_snapshot));
}

bool recomputeAffectedMaterializedColumns(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const NameSet & set_targets)
{
    /// If a MATERIALIZED column's source was rewritten by the `SET` (e.g. `d MATERIALIZED toDate(ts)`,
    /// `... SET ts = ...`), the stored `d` in the stream is stale. Recompute EVERY affected
    /// MATERIALIZED column so the written part (and any rebuilt skip index / projection reading them)
    /// is not stale. Independent of the sort-key re-sort: it must run even when no sort-key column is
    /// assigned. Returns true when a recompute step was added.
    auto affected_materialized_columns = getGroupByTTLSetAffectedMaterializedColumns(metadata_snapshot, context, set_targets);
    if (affected_materialized_columns.empty())
        return false;

    auto recompute_dag = buildRecomputeMaterializedColumnsDAG(
        builder.getHeader(), affected_materialized_columns, metadata_snapshot->getColumns(), context);
    builder.addSimpleTransform([&](const SharedHeader & header)
    {
        return std::make_shared<ExpressionTransform>(
            header, std::make_shared<ExpressionActions>(recompute_dag.clone()));
    });
    return true;
}

void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context,
    const MergeTreeSettings & storage_settings)
{
    resortPipelineAfterTTLGroupBySet(
        builder, metadata_snapshot, storage_columns, context, storage_settings, getGroupByTTLSetTargets(metadata_snapshot));
}

void resortPipelineAfterTTLGroupBySet(
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    const NamesAndTypesList & storage_columns,
    const ContextPtr & context,
    const MergeTreeSettings & storage_settings,
    const NameSet & set_targets)
{
    /// A MATERIALIZED sort-key column whose source the `SET` rewrote (e.g. `d MATERIALIZED toDate(ts)`,
    /// `ORDER BY d`, `... SET ts = ...`) is stale in the stream; recompute the affected MATERIALIZED
    /// columns before recomputing the sorting-key expression and re-sorting, otherwise the re-sort
    /// would key on the stale value. `recomputeAffectedMaterializedColumns` covers every affected
    /// MATERIALIZED column (including the sort-key subset), so nothing else is needed here.
    recomputeAffectedMaterializedColumns(builder, metadata_snapshot, context, set_targets);

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
