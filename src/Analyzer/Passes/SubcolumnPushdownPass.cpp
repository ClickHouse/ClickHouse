#include <Analyzer/Passes/SubcolumnPushdownPass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Functions/FunctionFactory.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace
{

/// Try to clone the target QueryNode if it IS the join_tree root (not nested inside).
/// Returns the clone if successful, nullptr otherwise.
///
/// We intentionally do NOT recurse into JoinNodes because cloning a child of a
/// JoinNode would invalidate column references in the join condition (ON clause).
/// The ON clause contains columns that reference the original table expressions,
/// and cloning one side would leave those columns with invalid (deallocated) sources.
QueryTreeNodePtr tryCloneTopLevelQueryNode(QueryTreeNodePtr & join_tree, const QueryTreeNodePtr & target)
{
    if (!join_tree)
        return nullptr;

    /// Only clone if join_tree IS the target (a simple FROM clause, not a JOIN)
    if (join_tree.get() != target.get())
        return nullptr;

    /// Don't clone JoinNodes - their children have cross-references we can't safely update
    if (join_tree->as<JoinNode>())
        return nullptr;

    auto clone = join_tree->clone();
    join_tree = clone;
    return clone;
}

/// Check if the optimization can be performed for a given projection column.
/// Returns true if optimization is possible and sets out_new_proj_node to the replacement node.
bool canOptimizeProjectionColumn(
    const ColumnNode * proj_column,
    const String & full_subcolumn_name,
    DataTypePtr subcolumn_type,
    ContextPtr context,
    QueryTreeNodePtr & out_new_proj_node)
{
    auto proj_source = proj_column->getColumnSourceOrNull();
    /// The projection column must have a valid source (table, subquery, etc.)
    if (!proj_source)
        return false;

    auto proj_source_type = proj_source->getNodeType();

    /// Case 1: Projection column comes directly from a table or table function.
    /// We can replace it with a direct subcolumn reference (e.g., tup.a instead of tup).
    if (proj_source_type == QueryTreeNodeType::TABLE || proj_source_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        auto * table_node = proj_source->as<TableNode>();
        if (table_node)
        {
            /// Some storages (like system tables) don't support subcolumn optimization.
            /// Skip optimization to avoid errors at read time.
            if (!table_node->getStorage()->supportsOptimizationToSubcolumns())
                return false;
        }

        /// Create a direct subcolumn reference. The storage will read only this subcolumn
        /// at execution time (e.g., for Tuple/JSON columns).
        NameAndTypePair subcolumn_name_and_type{full_subcolumn_name, subcolumn_type};
        out_new_proj_node = std::make_shared<ColumnNode>(subcolumn_name_and_type, proj_source);
        return true;
    }
    /// Case 2: Projection column comes from a nested subquery or union.
    /// We create a getSubcolumn call that will be pushed down in the next iteration.
    else if (proj_source_type == QueryTreeNodeType::QUERY || proj_source_type == QueryTreeNodeType::UNION)
    {
        const String subcolumn_name = full_subcolumn_name.substr(proj_column->getColumnName().size() + 1);
        auto get_subcolumn_func = std::make_shared<FunctionNode>("getSubcolumn");
        auto & func_args = get_subcolumn_func->getArguments().getNodes();
        func_args.push_back(proj_column->clone());
        func_args.push_back(std::make_shared<ConstantNode>(subcolumn_name));

        auto func = FunctionFactory::instance().get("getSubcolumn", context);
        get_subcolumn_func->resolveAsFunction(func->build(get_subcolumn_func->getArgumentColumns()));
        out_new_proj_node = get_subcolumn_func;
        return true;
    }

    /// Other source types (e.g., ARRAY_JOIN) are not supported
    return false;
}

class SubcolumnPushdownVisitor : public InDepthQueryTreeVisitor<SubcolumnPushdownVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<SubcolumnPushdownVisitor>;

    explicit SubcolumnPushdownVisitor(ContextPtr context_, QueryTreeNodePtr root_query_)
        : context(std::move(context_))
        , root_query(std::move(root_query_))
    {}

    bool madeChanges() const { return made_changes; }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        /// We only optimize getSubcolumn(column, 'subcolumn_name') calls.
        /// These are created by the analyzer for expressions like tup.a or event.name.
        if (!function_node || function_node->getFunctionName() != "getSubcolumn")
            return;

        auto & args = function_node->getArguments().getNodes();
        /// getSubcolumn must have exactly 2 arguments: the column and the subcolumn name
        if (args.size() != 2)
            return;

        auto * column_node = args[0]->as<ColumnNode>();
        auto * subcolumn_name_node = args[1]->as<ConstantNode>();

        /// First argument must be a column, second must be a constant string
        if (!column_node || !subcolumn_name_node)
            return;

        /// The subcolumn name must be a string (e.g., "a" for tup.a)
        if (subcolumn_name_node->getValue().getType() != Field::Types::String)
            return;

        auto column_source = column_node->getColumnSource();
        auto * query_source = column_source->as<QueryNode>();
        /// We only push down into QueryNode sources (subqueries, CTEs, VIEWs).
        /// For direct table access, the optimization is already done by FunctionToSubcolumnsPass.
        if (!query_source)
            return;

        const String & base_column_name = column_node->getColumnName();
        const String subcolumn_name = subcolumn_name_node->getValue().safeGet<String>();
        const String full_subcolumn_name = base_column_name + "." + subcolumn_name;

        auto subcolumn_type = column_node->getResultType()->tryGetSubcolumnType(subcolumn_name);
        /// The column type must support this subcolumn (e.g., Tuple must have this element)
        if (!subcolumn_type)
            return;

        /// Find the matching projection column in the source query's SELECT list
        auto & projection_nodes = query_source->getProjection().getNodes();

        for (size_t i = 0; i < projection_nodes.size(); ++i)
        {
            auto * proj_column = projection_nodes[i]->as<ColumnNode>();
            /// Skip non-column projections and columns with different names
            if (!proj_column || proj_column->getColumnName() != base_column_name)
                continue;

            /// First, check if we can optimize this projection column (without modifying anything).
            /// This is a "dry run" to avoid partial modifications if optimization fails later.
            QueryTreeNodePtr new_proj_node;
            if (!canOptimizeProjectionColumn(proj_column, full_subcolumn_name, subcolumn_type, context, new_proj_node))
                return;

            /// We need to clone the source QueryNode before modifying it to avoid
            /// modifying shared state. This is necessary for both CTEs and inline
            /// subqueries because the query tree might be used in multiple contexts
            /// (e.g., EXPLAIN, caching, etc.)
            auto * root_query_node = root_query->as<QueryNode>();
            if (!root_query_node)
                return;

            auto & join_tree = root_query_node->getJoinTree();
            auto clone = tryCloneTopLevelQueryNode(join_tree, column_source);
            /// The source must be found at the top level of JoinTree for cloning to work.
            /// If it's nested inside a JOIN, we skip optimization (cloning would break
            /// column references in the join condition).
            if (!clone)
                return;

            QueryTreeNodePtr source_to_modify = clone;
            query_source = source_to_modify->as<QueryNode>();

            /// After cloning, we need to re-get the projection column from the clone
            auto & cloned_projection_nodes = query_source->getProjection().getNodes();
            proj_column = cloned_projection_nodes[i]->as<ColumnNode>();
            /// The cloned projection must still be a ColumnNode
            if (!proj_column)
                return;

            /// Re-check optimization on the cloned source (its internal pointers may differ)
            if (!canOptimizeProjectionColumn(proj_column, full_subcolumn_name, subcolumn_type, context, new_proj_node))
                return;

            /// Now we can safely modify the query source
            auto & final_projection_nodes = query_source->getProjection().getNodes();

            /// Safety check: projection nodes and columns must be in sync
            auto projection_columns = query_source->getProjectionColumns();
            if (i >= final_projection_nodes.size() || i >= projection_columns.size())
                return;

            /// Replace the projection column (e.g., tup -> tup.a)
            final_projection_nodes[i] = new_proj_node;

            /// Update the projection columns metadata to reflect the new column name and type
            projection_columns[i] = NameAndTypePair{full_subcolumn_name, subcolumn_type};
            query_source->resolveProjectionColumns(std::move(projection_columns));

            /// Replace the getSubcolumn(column, 'subcolumn') call with a direct column reference
            /// that reads from the modified source
            NameAndTypePair new_column_name_and_type{full_subcolumn_name, subcolumn_type};
            node = std::make_shared<ColumnNode>(new_column_name_and_type, source_to_modify);

            made_changes = true;
            return;
        }
    }

private:
    ContextPtr context;
    QueryTreeNodePtr root_query;
    bool made_changes = false;
};

}

void SubcolumnPushdownPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    /// Run the pass iteratively until no more changes are made.
    /// Multiple iterations are needed for nested subqueries: each iteration pushes
    /// the subcolumn access one level deeper. For example:
    ///   SELECT tup.a FROM (SELECT * FROM (SELECT * FROM table))
    /// requires 2 iterations to push tup.a down to the innermost query.
    const size_t max_iterations = 10;
    for (size_t i = 0; i < max_iterations; ++i)
    {
        SubcolumnPushdownVisitor visitor(context, query_tree_node);
        visitor.visit(query_tree_node);
        /// Stop when no more optimizations can be made
        if (!visitor.madeChanges())
            break;
    }
}

}
