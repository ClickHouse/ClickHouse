#include <Processors/Sources/RecursiveCTESource.h>

#include <Storages/IStorage.h>
#include <Storages/StorageMemory.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>

#include <Core/Settings.h>

#include <optional>
#include <set>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_recursive_cte_evaluation_depth;
    extern const SettingsUInt64 recursive_cte_max_in_filter_cardinality;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

std::vector<TableNode *> collectTableNodesWithTemporaryTableName(const std::string & temporary_table_name, IQueryTreeNode * root)
{
    std::vector<TableNode *> result;

    std::vector<IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        auto * table_node = subtree_node->as<TableNode>();
        if (table_node && table_node->getTemporaryTableName() == temporary_table_name)
            result.push_back(table_node);

        for (auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return result;
}

/// Information about a join key between a CTE table and a real table.
struct JoinKeyInfo
{
    String real_table_column_name;
    String cte_column_name;
    StorageID real_table_storage_id;
    /// Alias of the real table expression, if any. When present, the generated
    /// filter is keyed by alias so each occurrence of the same physical table
    /// in the join tree receives only the filter derived from its own join keys.
    String real_table_alias;
    /// Index of the recursive `UNION` branch this key was extracted from
    /// (0 for non-union recursive queries). `additional_table_filters` is keyed
    /// by alias/table name with no branch dimension, so when the same key is
    /// seen in multiple branches the generated filter must be dropped — see
    /// the `branches` check in `buildAdditionalTableFiltersForRecursiveStep`.
    size_t branch_index = 0;
};

/// Check if a query tree node pointer matches one of the CTE table nodes.
bool isCTETableNode(const IQueryTreeNode * node, const std::vector<TableNode *> & recursive_table_nodes)
{
    for (const auto * table_node : recursive_table_nodes)
        if (node == table_node)
            return true;
    return false;
}

/// Extract equi-join key pairs from an ON join expression.
/// Handles single `equals` and `AND`-combined conditions.
void extractEquiJoinKeys(
    const QueryTreeNodePtr & expression,
    const std::vector<TableNode *> & recursive_table_nodes,
    size_t branch_index,
    std::vector<JoinKeyInfo> & result)
{
    const auto * function_node = expression->as<FunctionNode>();
    if (!function_node)
        return;

    if (function_node->getFunctionName() == "and")
    {
        for (const auto & arg : function_node->getArguments().getNodes())
            extractEquiJoinKeys(arg, recursive_table_nodes, branch_index, result);
        return;
    }

    if (function_node->getFunctionName() != "equals")
        return;

    const auto & args = function_node->getArguments().getNodes();
    if (args.size() != 2)
        return;

    const auto * left_column = args[0]->as<ColumnNode>();
    const auto * right_column = args[1]->as<ColumnNode>();
    if (!left_column || !right_column)
        return;

    auto left_source = left_column->getColumnSourceOrNull();
    auto right_source = right_column->getColumnSourceOrNull();
    if (!left_source || !right_source)
        return;

    bool left_is_cte = isCTETableNode(left_source.get(), recursive_table_nodes);
    bool right_is_cte = isCTETableNode(right_source.get(), recursive_table_nodes);

    /// Both columns from the same side (both CTE or both real) — skip.
    if (left_is_cte == right_is_cte)
        return;

    const auto * cte_column = left_is_cte ? left_column : right_column;
    const auto * real_column = left_is_cte ? right_column : left_column;
    const auto & real_source = left_is_cte ? right_source : left_source;

    const auto * real_table_node = real_source->as<TableNode>();
    if (!real_table_node)
        return;

    /// Use the original alias (the one the user wrote) — the analyzer rewrites
    /// aliases to internal `__tableN` names, but `PlannerJoinTree::buildAdditionalFiltersIfNeeded`
    /// matches `additional_table_filters` keys against `getOriginalAlias()`.
    result.push_back({
        real_column->getColumnName(),
        cte_column->getColumnName(),
        real_table_node->getStorageID(),
        real_table_node->getOriginalAlias(),
        branch_index,
    });
}

/// Extract equi-join keys from the join tree of a single QueryNode.
void findJoinKeysInQueryNode(
    const QueryNode & query_node,
    const std::vector<TableNode *> & recursive_table_nodes,
    size_t branch_index,
    std::vector<JoinKeyInfo> & result)
{
    std::vector<IQueryTreeNode *> nodes_to_visit;
    nodes_to_visit.push_back(query_node.getJoinTree().get());

    while (!nodes_to_visit.empty())
    {
        auto * node = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        auto * join_node = node->as<JoinNode>();
        if (!join_node)
            continue;

        if (join_node->getKind() == JoinKind::Inner
            && join_node->hasJoinExpression()
            && join_node->isOnJoinExpression())
        {
            extractEquiJoinKeys(join_node->getJoinExpression(), recursive_table_nodes, branch_index, result);
        }

        nodes_to_visit.push_back(join_node->getLeftTableExpression().get());
        nodes_to_visit.push_back(join_node->getRightTableExpression().get());
    }
}

/// Walk the join tree of the recursive query to find equi-join keys
/// between the CTE table and real tables. Only handles INNER JOINs with ON
/// expressions. When the recursive query has more than two branches, its root
/// is a UnionNode and each branch must be inspected independently. Each
/// extracted key is tagged with the `UNION`-branch index it came from so the
/// filter builder can detect cross-branch collisions.
std::vector<JoinKeyInfo> findJoinKeysWithCTETable(
    const QueryTreeNodePtr & recursive_query,
    const std::vector<TableNode *> & recursive_table_nodes)
{
    std::vector<JoinKeyInfo> result;

    if (const auto * query_node = recursive_query->as<QueryNode>())
    {
        findJoinKeysInQueryNode(*query_node, recursive_table_nodes, 0, result);
    }
    else if (const auto * union_node = recursive_query->as<UnionNode>())
    {
        size_t branch_index = 0;
        for (const auto & subquery : union_node->getQueries().getNodes())
        {
            if (const auto * sub_query_node = subquery->as<QueryNode>())
                findJoinKeysInQueryNode(*sub_query_node, recursive_table_nodes, branch_index, result);
            ++branch_index;
        }
    }

    return result;
}

/// Read deduplicated values of a column from a StorageMemory-backed temporary table.
/// Returns nullopt if the number of distinct values exceeds max_cardinality
/// (caller falls back to user-specified filters in that case).
std::optional<std::vector<Field>> readColumnValuesFromMemoryStorage(
    const StoragePtr & storage,
    const String & column_name,
    const ContextPtr & context,
    size_t max_cardinality)
{
    auto * memory_storage = typeid_cast<StorageMemory *>(storage.get());
    if (!memory_storage)
        return std::vector<Field>{};

    auto metadata = memory_storage->getInMemoryMetadataPtr(context, false);
    auto snapshot = memory_storage->getStorageSnapshot(metadata, context);
    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*snapshot->data);

    if (!snapshot_data.blocks)
        return std::vector<Field>{};

    std::set<Field> unique_values;

    for (const auto & block : *snapshot_data.blocks)
    {
        if (!block.has(column_name))
            continue;

        const auto & column = block.getByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i)
        {
            Field value;
            column->get(i, value);
            unique_values.insert(std::move(value));

            if (unique_values.size() > max_cardinality)
                return std::nullopt;
        }
    }

    return std::vector<Field>(unique_values.begin(), unique_values.end());
}

/// Build a SQL filter expression like: `column_name` IN (val1, val2, ...)
String buildInFilterExpression(const String & column_name, const std::vector<Field> & values)
{
    if (values.empty())
        return {};

    String result = backQuoteIfNeed(column_name) + " IN (";
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (i > 0)
            result += ", ";
        result += applyVisitor(FieldVisitorToString(), values[i]);
    }
    result += ")";
    return result;
}

/// Identity of a real table occurrence in the join tree: its alias (preferred
/// when present) or its full table name. Keying generated filters by this value
/// ensures that different occurrences of the same physical table (e.g., two
/// aliases of `edges` in a self-join) receive only the filter derived from
/// their own join keys — rather than a single combined filter that would
/// over-constrain every occurrence.
struct TableExpressionKey
{
    String value;
    bool is_alias = false;
};

TableExpressionKey makeTableExpressionKey(const JoinKeyInfo & key_info)
{
    if (!key_info.real_table_alias.empty())
        return {key_info.real_table_alias, true};
    return {key_info.real_table_storage_id.getFullNameNotQuoted(), false};
}

/// Build the `additional_table_filters` Map for a recursive CTE step.
/// Reads join key values from the working (CTE) table and creates IN filters
/// for the corresponding real tables.
/// Merges with user-specified `additional_table_filters` to avoid overwriting them.
Map buildAdditionalTableFiltersForRecursiveStep(
    const QueryTreeNodePtr & recursive_query,
    std::optional<std::vector<JoinKeyInfo>> & cached_join_keys,
    const std::vector<TableNode *> & recursive_table_nodes,
    const StoragePtr & working_table_storage,
    const ContextPtr & context,
    const Map & original_additional_table_filters,
    size_t max_in_filter_cardinality,
    size_t max_filter_size)
{
    if (max_in_filter_cardinality == 0)
        return original_additional_table_filters;

    if (!cached_join_keys.has_value())
        cached_join_keys = findJoinKeysWithCTETable(recursive_query, recursive_table_nodes);

    if (cached_join_keys->empty())
        return original_additional_table_filters;

    /// Group filter expressions by table-expression identity (alias when
    /// present, otherwise full table name). Remember the StorageID so we can
    /// still match user filters written as short name / full name.
    struct GroupedFilter
    {
        std::vector<String> parts;
        StorageID storage_id = StorageID::createEmpty();
        /// Recursive `UNION` branches that contributed join keys for this group.
        /// `additional_table_filters` is global across branches, so when more
        /// than one branch contributes — even with the same `StorageID` — we
        /// cannot express branch-local predicates and must drop the filter to
        /// avoid over-constraining results.
        std::set<size_t> branches;
        /// Set when the same key (e.g. alias) is used for join keys belonging
        /// to different physical tables — typically when the recursive query
        /// is a UNION whose branches reuse the same alias for different
        /// tables. `additional_table_filters` matches by alias alone, so the
        /// planner cannot disambiguate the two; emitting a combined filter
        /// would over-constrain (or reference missing columns on) one of
        /// them. We drop the CTE-derived filter for such keys; user filters
        /// for the same key are still preserved below.
        bool ambiguous = false;
    };
    std::map<String, GroupedFilter> filters_by_key; /// keyed by TableExpressionKey::value
    std::set<String> alias_keys;

    for (const auto & key_info : *cached_join_keys)
    {
        auto key = makeTableExpressionKey(key_info);
        auto & entry = filters_by_key[key.value];

        if (!entry.parts.empty() && entry.storage_id != key_info.real_table_storage_id)
            entry.ambiguous = true;

        if (entry.ambiguous)
            continue;

        auto values = readColumnValuesFromMemoryStorage(
            working_table_storage, key_info.cte_column_name, context, max_in_filter_cardinality);

        /// nullopt means cardinality exceeded the limit — skip the optimization entirely
        /// for this step and fall back to unfiltered scans.
        if (!values.has_value())
            return original_additional_table_filters;

        if (values->empty())
            continue;

        String filter_expr = buildInFilterExpression(key_info.real_table_column_name, *values);
        if (filter_expr.empty())
            continue;

        if (entry.parts.empty())
            entry.storage_id = key_info.real_table_storage_id;
        entry.parts.push_back(std::move(filter_expr));
        entry.branches.insert(key_info.branch_index);
        if (key.is_alias)
            alias_keys.insert(key.value);
    }

    /// Find the user-specified filter for a given generated filter group.
    /// Mirrors the matching logic in `PlannerJoinTree::buildAdditionalFiltersIfNeeded`
    /// so that user filters referencing the table by alias, short name, or full name
    /// are merged (rather than shadowed) by CTE-generated filters.
    auto find_user_filter = [&](const String & group_key, const StorageID & storage_id)
        -> std::pair<size_t, String>
    {
        const auto & current_database = context->getCurrentDatabase();
        const bool group_key_is_alias = alias_keys.contains(group_key);

        for (size_t idx = 0; idx < original_additional_table_filters.size(); ++idx)
        {
            const auto & tuple = original_additional_table_filters[idx].safeGet<Tuple>();
            const auto & user_key = tuple.at(0).safeGet<String>();

            const bool matches_alias = group_key_is_alias && user_key == group_key;
            const bool matches_full_name = user_key == storage_id.getFullNameNotQuoted();
            const bool matches_short_name = user_key == storage_id.getTableName()
                && current_database == storage_id.getDatabaseName();

            if (matches_alias || matches_full_name || matches_short_name)
                return {idx, tuple.at(1).safeGet<String>()};
        }

        return {SIZE_MAX, {}};
    };

    Map filters_map;
    std::set<size_t> merged_user_filter_indices;

    for (auto & [group_key, entry] : filters_by_key)
    {
        if (entry.ambiguous || entry.parts.empty())
            continue;

        /// Cross-branch occurrence of the same key cannot be expressed via
        /// `additional_table_filters` (which is global across branches), so
        /// drop the CTE-derived filter — user filters for the same key are
        /// still preserved below.
        if (entry.branches.size() > 1)
            continue;

        String combined_filter;
        for (size_t i = 0; i < entry.parts.size(); ++i)
        {
            if (i > 0)
                combined_filter += " AND ";
            combined_filter += entry.parts[i];
        }

        /// `PlannerJoinTree::buildAdditionalFiltersIfNeeded` re-parses each
        /// filter expression with `parseQuery` under the user's
        /// `max_query_size`. With long join key strings or many distinct
        /// values, the serialized filter can exceed that limit and break
        /// recursive evaluation. Drop the CTE-derived filter when its size
        /// exceeds the budget; the user filter for the same group, if any,
        /// is still preserved by the loop below.
        ///
        /// `max_query_size == 0` is the parser's "unlimited" sentinel
        /// (see `Lexer::nextToken`), so skip the size guard in that case.
        if (max_filter_size != 0 && combined_filter.size() > max_filter_size)
            continue;

        /// Combine with user-specified filter for the same table, if any.
        auto [user_idx, user_filter] = find_user_filter(group_key, entry.storage_id);
        if (user_idx != SIZE_MAX)
        {
            String merged_filter = "(" + combined_filter + ") AND (" + user_filter + ")";

            /// The merged predicate is also re-parsed under `max_query_size`. If the
            /// CTE-derived filter and the user filter together exceed the budget,
            /// drop the CTE-derived part and let the user filter pass through
            /// unchanged via the loop below. This preserves the pre-optimization
            /// behavior for queries that previously worked with just the user filter.
            /// As above, `max_query_size == 0` means "unlimited".
            if (max_filter_size != 0 && merged_filter.size() > max_filter_size)
                continue;

            combined_filter = std::move(merged_filter);
            merged_user_filter_indices.insert(user_idx);
        }

        Tuple tuple;
        tuple.push_back(Field(group_key));
        tuple.push_back(Field(std::move(combined_filter)));
        filters_map.push_back(Field(std::move(tuple)));
    }

    /// Preserve user-specified filters for tables not involved in the CTE join.
    for (size_t i = 0; i < original_additional_table_filters.size(); ++i)
    {
        if (!merged_user_filter_indices.contains(i))
            filters_map.push_back(original_additional_table_filters[i]);
    }

    return filters_map;
}

}

class RecursiveCTEChunkGenerator
{
public:
    RecursiveCTEChunkGenerator(SharedHeader header_, QueryTreeNodePtr recursive_cte_union_node_)
        : header(std::move(header_))
        , recursive_cte_union_node(std::move(recursive_cte_union_node_))
    {
        auto & recursive_cte_union_node_typed = recursive_cte_union_node->as<UnionNode &>();
        chassert(recursive_cte_union_node_typed.hasRecursiveCTETable());

        auto & recursive_cte_table = recursive_cte_union_node_typed.getRecursiveCTETable();

        const auto & cte_name = recursive_cte_union_node_typed.getCTEName();
        recursive_table_nodes = collectTableNodesWithTemporaryTableName(cte_name, recursive_cte_union_node.get());
        if (recursive_table_nodes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION query {} is not recursive", recursive_cte_union_node->formatASTForErrorMessage());

        size_t recursive_cte_union_node_queries_size = recursive_cte_union_node_typed.getQueries().getNodes().size();
        chassert(recursive_cte_union_node_queries_size > 1);

        non_recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[0];
        recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[1];

        if (recursive_cte_union_node_queries_size > 2)
        {
            auto working_union_query = std::make_shared<UnionNode>(recursive_cte_union_node_typed.getMutableContext(),
                recursive_cte_union_node_typed.getUnionMode());
            auto & working_union_query_subqueries = working_union_query->getQueries().getNodes();

            for (size_t i = 1; i < recursive_cte_union_node_queries_size; ++i)
                working_union_query_subqueries.push_back(recursive_cte_union_node_typed.getQueries().getNodes()[i]);

            recursive_query = std::move(working_union_query);
        }

        recursive_query_context = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getMutableContext() :
            recursive_query->as<UnionNode &>().getMutableContext();

        /// Save the original additional_table_filters so we can merge CTE-derived filters
        /// with user-specified ones on each recursive step, instead of overwriting them.
        original_additional_table_filters = recursive_query_context->getSettingsRef()[Setting::additional_table_filters].value;

        /// Collect all QueryNodes/UnionNodes inside the recursive query, together with their
        /// original `mutable_context`. On each recursive step we re-point their `mutable_context`
        /// at a per-step copy so the planner sees per-step settings (the planner reads settings
        /// from the query node's mutable context, not from the interpreter's context). For each
        /// branch of a UNION, the analyzer creates a `QueryNode` with its own context (a copy of
        /// the parent), so we must visit and update every nested node — not only those that
        /// happen to share the outer recursive query's context.
        std::vector<IQueryTreeNode *> nodes_to_visit;
        nodes_to_visit.push_back(recursive_query.get());
        while (!nodes_to_visit.empty())
        {
            auto * node = nodes_to_visit.back();
            nodes_to_visit.pop_back();

            if (auto * qn = node->as<QueryNode>())
                recursive_query_nodes_and_original_contexts.push_back({node, qn->getMutableContext()});
            else if (auto * un = node->as<UnionNode>())
                recursive_query_nodes_and_original_contexts.push_back({node, un->getMutableContext()});

            for (auto & child : node->getChildren())
            {
                if (child)
                    nodes_to_visit.push_back(child.get());
            }
        }

        const auto & recursive_query_projection_columns = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getProjectionColumns() :
            recursive_query->as<UnionNode &>().computeProjectionColumns();

        if (recursive_cte_table->columns.size() != recursive_query_projection_columns.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Recursive CTE subquery {}. Expected projection columns to have same size in recursive and non recursive subquery.",
            recursive_cte_union_node->formatASTForErrorMessage());

        working_temporary_table_holder = recursive_cte_table->holder;
        working_temporary_table_storage = recursive_cte_table->storage;

        intermediate_temporary_table_holder = std::make_shared<TemporaryTableHolder>(
            recursive_query_context,
            ColumnsDescription{NamesAndTypesList{recursive_cte_table->columns.begin(), recursive_cte_table->columns.end()}},
            ConstraintsDescription{},
            nullptr /*query*/,
            true /*create_for_global_subquery*/);
        intermediate_temporary_table_storage = intermediate_temporary_table_holder->getTable();
    }

    Chunk generate()
    {
        Chunk current_chunk;

        while (!finished)
        {
            if (!executor.has_value())
                buildStepExecutor();

            while (current_chunk.getNumRows() == 0 && executor->pull(current_chunk))
            {
            }

            read_rows_during_recursive_step += current_chunk.getNumRows();

            if (current_chunk.getNumRows() > 0)
                break;

            executor.reset();

            if (read_rows_during_recursive_step == 0)
            {
                finished = true;
                truncateTemporaryTable(intermediate_temporary_table_storage);
                continue;
            }

            read_rows_during_recursive_step = 0;

            for (auto & recursive_table_node : recursive_table_nodes)
                recursive_table_node->updateStorage(intermediate_temporary_table_storage, recursive_query_context);

            truncateTemporaryTable(working_temporary_table_storage);

            std::swap(intermediate_temporary_table_holder, working_temporary_table_holder);
            std::swap(intermediate_temporary_table_storage, working_temporary_table_storage);
        }

        return current_chunk;
    }

private:
    void buildStepExecutor()
    {
        const auto & recursive_subquery_settings = recursive_query_context->getSettingsRef();

        if (recursive_step > recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth])
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum recursive CTE evaluation depth ({}) exceeded, during evaluation of {}. Consider raising "
                "max_recursive_cte_evaluation_depth setting.",
                recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth].value,
                recursive_cte_union_node->formatASTForErrorMessage());

        auto & query_to_execute = recursive_step > 0 ? recursive_query : non_recursive_query;
        ++recursive_step;

        SelectQueryOptions select_query_options;
        select_query_options.merge_tree_enable_remove_parts_from_snapshot_optimization = false;

        const auto & recursive_table_name = recursive_cte_union_node->as<UnionNode &>().getCTEName();
        recursive_query_context->addOrUpdateExternalTable(recursive_table_name, working_temporary_table_holder);

        /// For recursive steps, inject additional_table_filters to push join key values
        /// into MergeTree's key condition, enabling index usage.
        /// recursive_step was already incremented above, so >1 means we're executing the recursive query.
        /// The interpreter receives a per-step copy of the context so that the per-step setting
        /// mutations below do not race with concurrent reads of the shared Settings object
        /// from other recursive CTEs (e.g. nested ones) that share `recursive_query_context`.
        ///
        /// We also re-point every QueryNode/UnionNode inside the recursive query at a per-step
        /// copy of its own original context: `Planner::buildPlannerContext` reads its settings
        /// directly from the query node's mutable context (not from the interpreter's context),
        /// and for `UNION ALL` branches the planner spawns a child `Planner` for each branch
        /// using that branch's own context, so the per-step settings must be visible there too.
        /// Restoring the original context after the step is unnecessary because the next step
        /// rebuilds fresh copies from the originals captured at construction time.
        auto interpreter_context = recursive_step > 1 ? Context::createCopy(recursive_query_context) : recursive_query_context;
        if (recursive_step > 1)
        {
            const auto max_in_filter_cardinality
                = recursive_subquery_settings[Setting::recursive_cte_max_in_filter_cardinality].value;
            const auto max_filter_size
                = recursive_subquery_settings[Setting::max_query_size].value;

            auto filters = buildAdditionalTableFiltersForRecursiveStep(
                recursive_query, cached_join_keys, recursive_table_nodes,
                working_temporary_table_storage, interpreter_context,
                original_additional_table_filters,
                max_in_filter_cardinality,
                max_filter_size);

            /// Disable parallel replicas for recursive CTE step queries. When parallel replicas
            /// is enabled, JOINs are rewritten to GLOBAL JOINs and the right-side subquery is
            /// materialized into a cached external table keyed by tree hash. Since the recursive
            /// CTE temporary table has the same tree structure across steps (only the data changes),
            /// the hash stays identical and stale cached data is reused, producing wrong results.
            /// We do this only for recursive iterations (not the seed) so that the seed query —
            /// which does not reference the CTE table — keeps the user's parallel replicas
            /// configuration. Setting it on every recursive iteration is idempotent.
            auto apply_step_overrides = [&](ContextMutablePtr & ctx)
            {
                ctx->setSetting("allow_experimental_parallel_reading_from_replicas", Field(UInt64(0)));
                ctx->setSetting("additional_table_filters", Field(filters));
            };

            apply_step_overrides(interpreter_context);

            /// Re-point each tracked QueryNode/UnionNode `mutable_context` at a per-step copy of
            /// its own original context, deduplicating so nodes that originally shared a context
            /// continue to share one after the rewrite.
            std::map<Context *, ContextMutablePtr> step_context_for_original;
            for (const auto & [node, original_context] : recursive_query_nodes_and_original_contexts)
            {
                ContextMutablePtr step_context;
                if (original_context == recursive_query_context)
                {
                    step_context = interpreter_context;
                }
                else
                {
                    auto it = step_context_for_original.find(original_context.get());
                    if (it != step_context_for_original.end())
                    {
                        step_context = it->second;
                    }
                    else
                    {
                        step_context = Context::createCopy(original_context);
                        apply_step_overrides(step_context);
                        step_context_for_original.emplace(original_context.get(), step_context);
                    }
                }

                if (auto * qn = node->as<QueryNode>())
                    qn->getMutableContext() = step_context;
                else if (auto * un = node->as<UnionNode>())
                    un->getMutableContext() = step_context;
            }
        }

        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_to_execute, interpreter_context, select_query_options);
        auto pipeline_builder = interpreter->buildQueryPipeline();

        pipeline_builder.addSimpleTransform([&](const SharedHeader & in_header)
        {
            return std::make_shared<MaterializingTransform>(in_header);
        });

        auto convert_to_temporary_tables_header_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline_builder.getHeader().getColumnsWithTypeAndName(),
            header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            interpreter->getContext());
        auto convert_to_temporary_tables_header_actions = std::make_shared<ExpressionActions>(std::move(convert_to_temporary_tables_header_actions_dag));
        pipeline_builder.addSimpleTransform([&](const SharedHeader & input_header)
        {
            return std::make_shared<ExpressionTransform>(input_header, convert_to_temporary_tables_header_actions);
        });

        /// TODO: Support squashing transform

        auto intermediate_temporary_table_storage_sink = intermediate_temporary_table_storage->write(
            {},
            intermediate_temporary_table_storage->getInMemoryMetadataPtr(recursive_query_context, false),
            recursive_query_context,
            false /*async_insert*/);

        pipeline_builder.addChain(Chain(std::move(intermediate_temporary_table_storage_sink)));

        pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
        pipeline.setProgressCallback(recursive_query_context->getProgressCallback());
        pipeline.setProcessListElement(recursive_query_context->getProcessListElement());

        executor.emplace(pipeline);
    }

    void truncateTemporaryTable(StoragePtr & temporary_table)
    {
        /// TODO: Support proper locking
        TableExclusiveLockHolder table_exclusive_lock;
        temporary_table->truncate({},
            temporary_table->getInMemoryMetadataPtr(recursive_query_context, false),
            recursive_query_context,
            table_exclusive_lock);
    }

    SharedHeader header;
    QueryTreeNodePtr recursive_cte_union_node;
    std::vector<TableNode *> recursive_table_nodes;

    QueryTreeNodePtr non_recursive_query;
    QueryTreeNodePtr recursive_query;
    ContextMutablePtr recursive_query_context;

    TemporaryTableHolderPtr working_temporary_table_holder;
    StoragePtr working_temporary_table_storage;

    TemporaryTableHolderPtr intermediate_temporary_table_holder;
    StoragePtr intermediate_temporary_table_storage;

    QueryPipeline pipeline;
    std::optional<PullingAsyncPipelineExecutor> executor;

    std::optional<std::vector<JoinKeyInfo>> cached_join_keys;
    Map original_additional_table_filters;

    struct RecursiveQueryNodeAndOriginalContext
    {
        IQueryTreeNode * node;
        ContextMutablePtr original_context;
    };
    std::vector<RecursiveQueryNodeAndOriginalContext> recursive_query_nodes_and_original_contexts;

    size_t recursive_step = 0;
    size_t read_rows_during_recursive_step = 0;
    bool finished = false;
};

RecursiveCTESource::RecursiveCTESource(SharedHeader header, QueryTreeNodePtr recursive_cte_union_node_)
    : ISource(header)
    , generator(std::make_unique<RecursiveCTEChunkGenerator>(std::move(header), std::move(recursive_cte_union_node_)))
{}

RecursiveCTESource::~RecursiveCTESource() = default;

Chunk RecursiveCTESource::generate()
{
    return generator->generate();
}

}
