#include <Analyzer/Passes/SubcolumnPushdownPass.h>

#include <unordered_map>
#include <unordered_set>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Storages/IStorage.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_push_subcolumns_into_subqueries;
}

namespace
{

/// Information about a getSubcolumn call that can be optimized
struct SubcolumnAccess
{
    QueryTreeNodePtr * node_to_replace;       /// Pointer to the node location to replace
    ColumnNode * column_node;                 /// The column being accessed (first arg of getSubcolumn)
    String base_column_name;                  /// Name of the base column (e.g., "tup")
    String subcolumn_name;                    /// Name of the subcolumn (e.g., "a")
    String full_subcolumn_name;               /// Full name (e.g., "tup.a")
    DataTypePtr subcolumn_type;               /// Type of the subcolumn
    size_t projection_index;                  /// Index of base column in the source query's projection
};

/// A column source is either a `TableNode` (`FROM t`) or a `TableFunctionNode` (`FROM file(...)`).
/// Returns nullptr for anything else, and for an unresolved table function, which carries no storage.
StorageSnapshotPtr getStorageSnapshotForColumnSource(const QueryTreeNodePtr & column_source)
{
    if (const auto * table_node = column_source->as<TableNode>())
        return table_node->getStorageSnapshot();
    if (const auto * table_function_node = column_source->as<TableFunctionNode>(); table_function_node && table_function_node->isResolved())
        return table_function_node->getStorageSnapshot();
    return nullptr;
}

/// Whether the storage behind `proj_column` can serve its subcolumn directly. This is the same gate
/// `FunctionToSubcolumnsPass` applies before reading a subcolumn instead of the whole column: some storages
/// (system tables, `file`, `url`, ...) report `supportsOptimizationToSubcolumns` = false, virtual columns and
/// columns the storage does not know under that exact type cannot be read as subcolumns either.
bool storageCanReadSubcolumn(const ColumnNode & proj_column, const QueryTreeNodePtr & proj_source, const ContextPtr & context)
{
    auto storage_snapshot = getStorageSnapshotForColumnSource(proj_source);
    if (!storage_snapshot)
        return false;

    const auto & storage = storage_snapshot->storage;
    if (!storage.supportsOptimizationToSubcolumns())
        return false;

    /// The storage is replaced with the view source only after the passes, see `FunctionToSubcolumnsPass`.
    auto view_source = context->getViewSource();
    if (view_source && view_source->getStorageID().getFullNameNotQuoted() == storage.getStorageID().getFullNameNotQuoted())
        return false;

    const auto & column = proj_column.getColumn();
    if (storage_snapshot->metadata->isVirtualColumn(column.name))
        return false;

    auto column_in_table = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), column.name);
    return column_in_table && column_in_table->type->equals(*column.type);
}

/// Check if a projection column can be optimized and return the new projection node.
/// Returns nullptr if optimization is not possible.
QueryTreeNodePtr tryCreateSubcolumnProjectionNode(
    const ColumnNode * proj_column,
    const String & subcolumn_name,
    DataTypePtr subcolumn_type,
    ContextPtr context)
{
    auto proj_source = proj_column->getColumnSourceOrNull();

    /// The projection column must have a valid source to determine how to create the subcolumn node
    if (!proj_source)
        return nullptr;

    auto proj_source_type = proj_source->getNodeType();

    /// Case 1: Projection column comes directly from a table or table function.
    /// We can create a direct ColumnNode for the subcolumn (e.g., tup.a) if the storage supports it.
    if (proj_source_type == QueryTreeNodeType::TABLE || proj_source_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        /// Some storage engines don't support subcolumn optimization (e.g., system tables, `file`).
        /// Skip optimization for those to avoid errors.
        if (proj_column->hasExpression() || !storageCanReadSubcolumn(*proj_column, proj_source, context))
            return nullptr;

        /// The name has to be built from the name of the column in the table, which is not necessarily
        /// the name the subquery exposes it under (`SELECT v AS u FROM t` has to read `v.a`, not `u.a`).
        NameAndTypePair subcolumn_name_and_type{proj_column->getColumnName() + "." + subcolumn_name, subcolumn_type};
        return std::make_shared<ColumnNode>(subcolumn_name_and_type, proj_source);
    }
    /// Case 2: Projection column comes from a nested subquery or union.
    /// We need to create a getSubcolumn function call, which is pushed further down when the pass is run
    /// on the rewritten source query, see `SubcolumnPushdownPass::run`.
    else if (proj_source_type == QueryTreeNodeType::QUERY || proj_source_type == QueryTreeNodeType::UNION)
    {
        auto get_subcolumn_func = std::make_shared<FunctionNode>("getSubcolumn");
        auto & func_args = get_subcolumn_func->getArguments().getNodes();
        func_args.push_back(proj_column->clone());
        func_args.push_back(std::make_shared<ConstantNode>(subcolumn_name));

        auto func = FunctionFactory::instance().get("getSubcolumn", context);
        get_subcolumn_func->resolveAsFunction(func->build(get_subcolumn_func->getArgumentColumns()));
        return get_subcolumn_func;
    }

    return nullptr;
}

/// Check that rewriting the projection of `source_query` cannot change the rows it produces.
/// `DISTINCT`, `GROUP BY`, `LIMIT BY` and window functions all key off the projected expressions,
/// so replacing a column with one of its subcolumns there would silently change the result
/// (`SELECT DISTINCT tup` and `SELECT DISTINCT tup.a` are not the same query), or leave the
/// subcolumn outside of the grouping keys, which is not a valid query at all.
/// `INTERPOLATE` targets are projection columns as well, so dropping one from the projection
/// would leave a dangling target (`Missing column 'tup' as an INTERPOLATE expression target`).
bool isSourceQuerySafeToRewrite(const QueryNode & source_query)
{
    return !source_query.isDistinct()
        && !source_query.hasGroupBy()
        && !source_query.isGroupByAll()
        && !source_query.isGroupByWithTotals()
        && !source_query.isGroupByWithRollup()
        && !source_query.isGroupByWithCube()
        && !source_query.isGroupByWithGroupingSets()
        && !source_query.hasLimitBy()
        && !source_query.isLimitByAll()
        && !source_query.hasWindow()
        && !source_query.hasQualify()
        && !source_query.hasInterpolate();
}

using NodeSet = std::unordered_set<const IQueryTreeNode *>;

void collectSubcolumnAccessesExposedToAggregation(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys, NodeSet & exposed)
{
    if (!node)
        return;

    /// An expression that is a grouping key is fine as it is, and so is everything below it.
    for (const auto & key : group_by_keys)
    {
        if (node->isEqual(*key))
            return;
    }

    if (const auto * function_node = node->as<FunctionNode>())
    {
        /// The arguments of an aggregate function are evaluated before the aggregation.
        if (function_node->isAggregateFunction())
            return;

        if (function_node->getFunctionName() == "getSubcolumn")
        {
            exposed.insert(node.get());
            return;
        }
    }

    /// A nested query is its own scope.
    auto node_type = node->getNodeType();
    if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
        return;

    for (const auto & child : node->getChildren())
        collectSubcolumnAccessesExposedToAggregation(child, group_by_keys, exposed);
}

/// In an aggregating query, an expression that is evaluated after the aggregation (the projection, `HAVING`,
/// `ORDER BY`, window definitions, ...) must be a grouping key, a function of grouping keys, or an aggregate.
/// `tup.a` is a function of the key `tup`, but the plain column `tup.a` that the rewrite would turn it into is
/// not, so such a query would fail to plan (`Not found column tup.a in block`). Collect the `getSubcolumn`
/// nodes of `query` that are in such a position and are neither a grouping key themselves nor an argument of
/// an aggregate function: those must be left alone.
NodeSet collectSubcolumnAccessesExposedToAggregation(const QueryNode & query)
{
    NodeSet exposed;

    if (!query.hasGroupBy() && !query.isGroupByAll())
        return exposed;

    /// With `GROUPING SETS` the keys are nested one level deeper.
    QueryTreeNodes group_by_keys;
    for (const auto & key : query.getGroupBy().getNodes())
    {
        if (const auto * key_list = key->as<ListNode>(); key_list && query.isGroupByWithGroupingSets())
            group_by_keys.insert(group_by_keys.end(), key_list->getNodes().begin(), key_list->getNodes().end());
        else
            group_by_keys.push_back(key);
    }

    for (const auto & section : {query.getProjectionNode(), query.getHaving(), query.getWindowNode(), query.getQualify(),
                                 query.getOrderByNode(), query.getInterpolate(), query.getLimitByNode()})
        collectSubcolumnAccessesExposedToAggregation(section, group_by_keys, exposed);

    return exposed;
}

/// The same for every query in the tree. Only a source that is the join tree of the query the pass runs on is
/// rewritten, but a column of that source can also be read from a nested query (a correlated reference), so the
/// aggregation of every scope has to be taken into account.
void collectSubcolumnAccessesExposedToAggregationInAllScopes(const QueryTreeNodePtr & node, NodeSet & exposed)
{
    if (!node)
        return;

    if (const auto * query_node = node->as<QueryNode>())
        exposed.merge(collectSubcolumnAccessesExposedToAggregation(*query_node));

    for (const auto & child : node->getChildren())
        collectSubcolumnAccessesExposedToAggregationInAllScopes(child, exposed);
}

/// Collect all getSubcolumn calls that can be optimized, plus all columns referencing each source.
/// Groups by source node so we can clone each source only once and update all references.
class CollectSubcolumnAccessesVisitor : public InDepthQueryTreeVisitor<CollectSubcolumnAccessesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<CollectSubcolumnAccessesVisitor>;

    CollectSubcolumnAccessesVisitor(ContextPtr context_, NodeSet nodes_to_skip_)
        : context(std::move(context_))
        , nodes_to_skip(std::move(nodes_to_skip_))
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        /// Track all columns that reference QueryNode sources (needed to update sources after cloning)
        if (auto * column_node = node->as<ColumnNode>())
        {
            auto column_source = column_node->getColumnSourceOrNull();
            if (column_source && column_source->as<QueryNode>())
                all_columns_by_source[column_source.get()].push_back(column_node);
        }

        auto * function_node = node->as<FunctionNode>();

        /// We only optimize getSubcolumn function calls (e.g., getSubcolumn(tup, 'a') from tup.a access)
        if (!function_node || function_node->getFunctionName() != "getSubcolumn")
            return;

        auto & args = function_node->getArguments().getNodes();

        /// getSubcolumn must have exactly 2 arguments: the column and the subcolumn name
        if (args.size() != 2)
            return;

        /// This access is evaluated after an aggregation and is not a grouping key, see above.
        if (nodes_to_skip.contains(node.get()))
            return;

        auto * column_node = args[0]->as<ColumnNode>();
        auto * subcolumn_name_node = args[1]->as<ConstantNode>();

        /// First arg must be a column, second must be a constant (the subcolumn name)
        if (!column_node || !subcolumn_name_node)
            return;

        /// The subcolumn name must be a string constant
        if (subcolumn_name_node->getValue().getType() != Field::Types::String)
            return;

        auto column_source = column_node->getColumnSourceOrNull();

        /// The column must have a valid source to push the optimization into
        if (!column_source)
            return;

        auto * query_source = column_source->as<QueryNode>();

        /// We can only push down into QueryNode sources (subqueries), not tables directly.
        /// Tables are already handled by FunctionToSubcolumnsPass.
        if (!query_source)
            return;

        /// Rewriting the projection must not change which rows the source query produces.
        if (!isSourceQuerySafeToRewrite(*query_source))
            return;

        const String & base_column_name = column_node->getColumnName();
        const String subcolumn_name = subcolumn_name_node->getValue().safeGet<String>();
        const String full_subcolumn_name = base_column_name + "." + subcolumn_name;

        auto subcolumn_type = column_node->getResultType()->tryGetSubcolumnType(subcolumn_name);

        /// The column type must support this subcolumn (e.g., Tuple has .a, Map has .keys/.values)
        if (!subcolumn_type)
            return;

        const auto & source_projection_columns = query_source->getProjectionColumns();

        /// Adding a projection column under a name the source query already exposes would shadow it.
        for (const auto & source_projection_column : source_projection_columns)
        {
            if (source_projection_column.name == full_subcolumn_name)
                return;
        }

        /// Find the matching projection column in the source query. The match has to be done on the
        /// name the source query exposes the column under, which is what the outer query refers to -
        /// it is not necessarily the name of the underlying column (`SELECT v AS u FROM t`).
        auto & projection_nodes = query_source->getProjection().getNodes();
        for (size_t i = 0; i < projection_nodes.size() && i < source_projection_columns.size(); ++i)
        {
            if (source_projection_columns[i].name != base_column_name)
                continue;

            auto * proj_column = projection_nodes[i]->as<ColumnNode>();

            /// Only a plain column reference can be turned into a subcolumn reference
            if (!proj_column)
                return;

            /// Verify this can actually be optimized before recording
            auto new_proj_node = tryCreateSubcolumnProjectionNode(proj_column, subcolumn_name, subcolumn_type, context);

            /// Skip if we can't create the optimized projection node
            if (!new_proj_node)
                return;

            /// Record this access grouped by source for batch processing
            subcolumn_accesses_by_source[column_source.get()].push_back({
                .node_to_replace = &node,
                .column_node = column_node,
                .base_column_name = base_column_name,
                .subcolumn_name = subcolumn_name,
                .full_subcolumn_name = full_subcolumn_name,
                .subcolumn_type = subcolumn_type,
                .projection_index = i
            });
            return;
        }
    }

    std::unordered_map<IQueryTreeNode *, std::vector<SubcolumnAccess>> & getSubcolumnAccessesBySource()
    {
        return subcolumn_accesses_by_source;
    }

    std::unordered_map<IQueryTreeNode *, std::vector<ColumnNode *>> & getAllColumnsBySource()
    {
        return all_columns_by_source;
    }

private:
    ContextPtr context;
    NodeSet nodes_to_skip;
    std::unordered_map<IQueryTreeNode *, std::vector<SubcolumnAccess>> subcolumn_accesses_by_source;
    std::unordered_map<IQueryTreeNode *, std::vector<ColumnNode *>> all_columns_by_source;
};

/// Try to clone the target QueryNode if it IS the join_tree root.
/// We must clone to avoid modifying shared nodes (e.g., CTEs referenced multiple times).
/// Returns nullptr if cloning is not possible or not needed.
TableExpressionNodePtr tryCloneTopLevelQueryNode(QueryTreeNodePtr & join_tree, const TableExpressionNodePtr & target)
{
    /// join_tree must exist
    if (!join_tree)
        return nullptr;

    /// We can only clone if target is exactly the join_tree root.
    /// Nested sources inside JOINs cannot be safely cloned without breaking ON clause references.
    if (join_tree.get() != target.get())
        return nullptr;

    /// Don't clone JoinNode - modifying its children would invalidate column references in ON clause
    if (join_tree->as<JoinNode>())
        return nullptr;

    auto clone = static_pointer_cast<ITableExpressionNode>(join_tree->clone());
    join_tree = clone;
    return clone;
}

}

void SubcolumnPushdownPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto * root_query_node = query_tree_node->as<QueryNode>();

    /// This pass only works on query nodes (SELECT statements)
    if (!root_query_node)
        return;

    if (!context->getSettingsRef()[Setting::optimize_push_subcolumns_into_subqueries])
        return;

    /// Collect all subcolumn accesses and all columns grouped by source, except those evaluated after an aggregation.
    NodeSet nodes_to_skip;
    collectSubcolumnAccessesExposedToAggregationInAllScopes(query_tree_node, nodes_to_skip);
    CollectSubcolumnAccessesVisitor collector(context, std::move(nodes_to_skip));
    collector.visit(query_tree_node);

    auto & subcolumn_accesses_by_source = collector.getSubcolumnAccessesBySource();
    auto & all_columns_by_source = collector.getAllColumnsBySource();

    /// Nothing to optimize if no subcolumn accesses were found
    if (subcolumn_accesses_by_source.empty())
        return;

    /// For each source with subcolumn accesses, clone once and add all needed subcolumns
    for (auto & [original_source_ptr, accesses] : subcolumn_accesses_by_source)
    {
        /// Skip empty access lists (shouldn't happen, but defensive check)
        if (accesses.empty())
            continue;

        /// Get the source from any access (they all reference the same source)
        auto column_source = accesses[0].column_node->getColumnSourceOrNull();

        /// Source must still be valid (weak pointer might have expired)
        if (!column_source)
            continue;

        /// Clone the source to avoid modifying shared nodes
        auto & join_tree = root_query_node->getJoinTreeNode();
        auto cloned_source = tryCloneTopLevelQueryNode(join_tree, column_source);

        /// Skip if cloning failed (e.g., source is inside a JOIN)
        if (!cloned_source)
            continue;

        /// Update all columns that reference the original source to point to the cloned source.
        /// This is necessary because columns not involved in subcolumn access (e.g., `id` in
        /// `SELECT id, data.a FROM view`) would otherwise be left with orphaned sources.
        auto it = all_columns_by_source.find(original_source_ptr);
        if (it != all_columns_by_source.end())
        {
            for (auto * col : it->second)
                col->setColumnSource(cloned_source);
        }

        auto * cloned_query_source = cloned_source->as<QueryNode>();

        /// Cloned source must be a QueryNode to modify its projection
        if (!cloned_query_source)
            continue;

        /// Get unique subcolumns to add (deduplicate by full_subcolumn_name).
        /// Multiple accesses to the same subcolumn (e.g., tup.a used twice) share one projection column.
        std::unordered_map<String, SubcolumnAccess *> unique_subcolumns;
        for (auto & access : accesses)
            unique_subcolumns.try_emplace(access.full_subcolumn_name, &access);

        /// Count how many times each base column of this source is referenced anywhere in the query.
        /// A base column may only be dropped from the projection once every one of its references has
        /// been rewritten into a subcolumn reference - otherwise the outer query would still ask for a
        /// column that the source no longer produces (`SELECT tup, tup.a FROM (SELECT tup FROM t)`).
        std::unordered_map<String, size_t> total_references_by_base_column;
        if (it != all_columns_by_source.end())
        {
            for (const auto * col : it->second)
                ++total_references_by_base_column[col->getColumnName()];
        }

        /// Candidate base columns to drop, mapped to their index in the source projection.
        std::unordered_map<String, size_t> removal_candidates;

        /// Add new subcolumn projections
        auto & cloned_projection_nodes = cloned_query_source->getProjection().getNodes();
        auto projection_columns = cloned_query_source->getProjectionColumns();

        /// Map from full_subcolumn_name to its new projection index
        std::unordered_map<String, size_t> subcolumn_to_new_index;

        for (auto & [full_subcolumn_name, access_ptr] : unique_subcolumns)
        {
            auto & access = *access_ptr;

            /// Projection index must be valid (defensive check against data corruption)
            if (access.projection_index >= cloned_projection_nodes.size())
                continue;

            auto * proj_column = cloned_projection_nodes[access.projection_index]->as<ColumnNode>();

            /// Projection node must be a ColumnNode to extract subcolumn from
            if (!proj_column)
                continue;

            /// Create the subcolumn projection node
            auto new_proj_node = tryCreateSubcolumnProjectionNode(
                proj_column, access.subcolumn_name, access.subcolumn_type, context);

            /// Skip if we couldn't create the projection node
            if (!new_proj_node)
                continue;

            /// Add to projection (at the end)
            size_t new_index = cloned_projection_nodes.size();
            cloned_projection_nodes.push_back(new_proj_node);
            projection_columns.push_back(NameAndTypePair{access.full_subcolumn_name, access.subcolumn_type});

            subcolumn_to_new_index[full_subcolumn_name] = new_index;
            removal_candidates.emplace(access.base_column_name, access.projection_index);
        }

        /// Replace all getSubcolumn calls with direct column references to the new projection columns
        std::unordered_map<String, size_t> rewritten_references_by_base_column;
        for (auto & access : accesses)
        {
            auto subcolumn_it = subcolumn_to_new_index.find(access.full_subcolumn_name);

            /// If we couldn't add this subcolumn to projection, just update the source reference
            if (subcolumn_it == subcolumn_to_new_index.end())
            {
                access.column_node->setColumnSource(cloned_source);
                continue;
            }

            /// Replace the getSubcolumn call with a direct column reference
            NameAndTypePair new_column_name_and_type{access.full_subcolumn_name, access.subcolumn_type};
            *access.node_to_replace = std::make_shared<ColumnNode>(new_column_name_and_type, cloned_source);
            ++rewritten_references_by_base_column[access.base_column_name];
        }

        /// Remove the base columns that are now completely unused, i.e. every reference to them was
        /// rewritten above. A base column that is still read directly stays in the projection.
        std::vector<size_t> indices_to_remove;
        for (const auto & [base_column_name, projection_index] : removal_candidates)
        {
            if (total_references_by_base_column[base_column_name] == rewritten_references_by_base_column[base_column_name])
                indices_to_remove.push_back(projection_index);
        }

        /// We need to remove from highest index to lowest to avoid invalidating indices.
        std::sort(indices_to_remove.begin(), indices_to_remove.end(), std::greater<>());

        for (size_t idx : indices_to_remove)
        {
            /// Defensive check to avoid out-of-bounds access
            if (idx < cloned_projection_nodes.size())
            {
                cloned_projection_nodes.erase(cloned_projection_nodes.begin() + idx);
                projection_columns.erase(projection_columns.begin() + idx);
            }
        }

        /// The source query may carry a column alias list (`FROM (...) t(r, x)`). Those aliases were already
        /// applied by name to the projection columns when the source was resolved, and `resolveProjectionColumns`
        /// re-applies them positionally, so with the projection grown or shrunk above it would either throw
        /// (`Number of aliases does not match number of projection columns`) or rename the wrong columns.
        /// The names in `projection_columns` are the final ones, so the alias list is no longer needed.
        cloned_query_source->setProjectionAliasesToOverride({});
        cloned_query_source->resolveProjectionColumns(std::move(projection_columns));

        /// If the source reads the column from a subquery itself, the projection got a `getSubcolumn` call
        /// (see `tryCreateSubcolumnProjectionNode`) that can be pushed one level further down:
        /// `SELECT tup.a FROM (SELECT tup FROM (SELECT tup FROM t))` has to read only `tup.a` from `t`.
        /// Each level clones its own source, so this terminates at the depth of the nested subqueries.
        QueryTreeNodePtr cloned_source_node = cloned_source;
        run(cloned_source_node, context);
    }
}

}
