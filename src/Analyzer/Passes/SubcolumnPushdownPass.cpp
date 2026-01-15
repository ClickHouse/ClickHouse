#include <Analyzer/Passes/SubcolumnPushdownPass.h>

#include <unordered_map>
#include <unordered_set>

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

/// Check if a projection column can be optimized and return the new projection node
QueryTreeNodePtr tryCreateSubcolumnProjectionNode(
    const ColumnNode * proj_column,
    const String & full_subcolumn_name,
    DataTypePtr subcolumn_type,
    ContextPtr context)
{
    auto proj_source = proj_column->getColumnSourceOrNull();
    if (!proj_source)
        return nullptr;

    auto proj_source_type = proj_source->getNodeType();

    /// Case 1: Projection column comes directly from a table or table function
    if (proj_source_type == QueryTreeNodeType::TABLE || proj_source_type == QueryTreeNodeType::TABLE_FUNCTION)
    {
        auto * table_node = proj_source->as<TableNode>();
        if (table_node && !table_node->getStorage()->supportsOptimizationToSubcolumns())
            return nullptr;

        NameAndTypePair subcolumn_name_and_type{full_subcolumn_name, subcolumn_type};
        return std::make_shared<ColumnNode>(subcolumn_name_and_type, proj_source);
    }
    /// Case 2: Projection column comes from a nested subquery or union
    else if (proj_source_type == QueryTreeNodeType::QUERY || proj_source_type == QueryTreeNodeType::UNION)
    {
        const String subcolumn_part = full_subcolumn_name.substr(proj_column->getColumnName().size() + 1);
        auto get_subcolumn_func = std::make_shared<FunctionNode>("getSubcolumn");
        auto & func_args = get_subcolumn_func->getArguments().getNodes();
        func_args.push_back(proj_column->clone());
        func_args.push_back(std::make_shared<ConstantNode>(subcolumn_part));

        auto func = FunctionFactory::instance().get("getSubcolumn", context);
        get_subcolumn_func->resolveAsFunction(func->build(get_subcolumn_func->getArgumentColumns()));
        return get_subcolumn_func;
    }

    return nullptr;
}

/// First pass: collect all getSubcolumn calls that can be optimized
class CollectSubcolumnAccessesVisitor : public InDepthQueryTreeVisitor<CollectSubcolumnAccessesVisitor>
{
public:
    using Base = InDepthQueryTreeVisitor<CollectSubcolumnAccessesVisitor>;

    explicit CollectSubcolumnAccessesVisitor(ContextPtr context_)
        : context(std::move(context_))
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "getSubcolumn")
            return;

        auto & args = function_node->getArguments().getNodes();
        if (args.size() != 2)
            return;

        auto * column_node = args[0]->as<ColumnNode>();
        auto * subcolumn_name_node = args[1]->as<ConstantNode>();

        if (!column_node || !subcolumn_name_node)
            return;

        if (subcolumn_name_node->getValue().getType() != Field::Types::String)
            return;

        auto column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return;

        auto * query_source = column_source->as<QueryNode>();
        if (!query_source)
            return;

        const String & base_column_name = column_node->getColumnName();
        const String subcolumn_name = subcolumn_name_node->getValue().safeGet<String>();
        const String full_subcolumn_name = base_column_name + "." + subcolumn_name;

        auto subcolumn_type = column_node->getResultType()->tryGetSubcolumnType(subcolumn_name);
        if (!subcolumn_type)
            return;

        /// Find the matching projection column
        auto & projection_nodes = query_source->getProjection().getNodes();
        for (size_t i = 0; i < projection_nodes.size(); ++i)
        {
            auto * proj_column = projection_nodes[i]->as<ColumnNode>();
            if (!proj_column || proj_column->getColumnName() != base_column_name)
                continue;

            /// Verify this can be optimized
            auto new_proj_node = tryCreateSubcolumnProjectionNode(proj_column, full_subcolumn_name, subcolumn_type, context);
            if (!new_proj_node)
                return;

            /// Record this access grouped by source
            accesses_by_source[column_source.get()].push_back({
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

    std::unordered_map<IQueryTreeNode *, std::vector<SubcolumnAccess>> & getAccessesBySource()
    {
        return accesses_by_source;
    }

private:
    ContextPtr context;
    std::unordered_map<IQueryTreeNode *, std::vector<SubcolumnAccess>> accesses_by_source;
};

/// Try to clone the target QueryNode if it IS the join_tree root
QueryTreeNodePtr tryCloneTopLevelQueryNode(QueryTreeNodePtr & join_tree, const QueryTreeNodePtr & target)
{
    if (!join_tree)
        return nullptr;

    if (join_tree.get() != target.get())
        return nullptr;

    if (join_tree->as<JoinNode>())
        return nullptr;

    auto clone = join_tree->clone();
    join_tree = clone;
    return clone;
}

}

void SubcolumnPushdownPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    auto * root_query_node = query_tree_node->as<QueryNode>();
    if (!root_query_node)
        return;

    /// First pass: collect all subcolumn accesses grouped by source
    CollectSubcolumnAccessesVisitor collector(context);
    collector.visit(query_tree_node);

    auto & accesses_by_source = collector.getAccessesBySource();
    if (accesses_by_source.empty())
        return;

    /// Second pass: for each source, clone once and add all needed subcolumns
    for (auto & [original_source_ptr, accesses] : accesses_by_source)
    {
        if (accesses.empty())
            continue;

        /// Get the source from any access (they all reference the same source)
        auto column_source = accesses[0].column_node->getColumnSourceOrNull();
        if (!column_source)
            continue;

        /// Clone the source
        auto & join_tree = root_query_node->getJoinTree();
        auto cloned_source = tryCloneTopLevelQueryNode(join_tree, column_source);
        if (!cloned_source)
            continue;

        auto * cloned_query_source = cloned_source->as<QueryNode>();
        if (!cloned_query_source)
            continue;

        /// Get unique subcolumns to add (deduplicate by full_subcolumn_name)
        /// Multiple accesses to the same subcolumn (e.g., tup.a used twice) share one projection column
        std::unordered_map<String, SubcolumnAccess *> unique_subcolumns;
        for (auto & access : accesses)
            unique_subcolumns.try_emplace(access.full_subcolumn_name, &access);

        /// Add new subcolumn projections
        auto & cloned_projection_nodes = cloned_query_source->getProjection().getNodes();
        auto projection_columns = cloned_query_source->getProjectionColumns();

        /// Map from full_subcolumn_name to its new projection index
        std::unordered_map<String, size_t> subcolumn_to_new_index;

        for (auto & [full_subcolumn_name, access_ptr] : unique_subcolumns)
        {
            auto & access = *access_ptr;

            if (access.projection_index >= cloned_projection_nodes.size())
                continue;

            auto * proj_column = cloned_projection_nodes[access.projection_index]->as<ColumnNode>();
            if (!proj_column)
                continue;

            /// Create the subcolumn projection node
            auto new_proj_node = tryCreateSubcolumnProjectionNode(
                proj_column, access.full_subcolumn_name, access.subcolumn_type, context);
            if (!new_proj_node)
                continue;

            /// Add to projection (at the end)
            size_t new_index = cloned_projection_nodes.size();
            cloned_projection_nodes.push_back(new_proj_node);
            projection_columns.push_back(NameAndTypePair{access.full_subcolumn_name, access.subcolumn_type});

            subcolumn_to_new_index[full_subcolumn_name] = new_index;
        }

        cloned_query_source->resolveProjectionColumns(std::move(projection_columns));

        /// Replace all getSubcolumn calls with direct column references to the new projection columns
        for (auto & access : accesses)
        {
            auto it = subcolumn_to_new_index.find(access.full_subcolumn_name);
            if (it == subcolumn_to_new_index.end())
            {
                /// Couldn't add this subcolumn - just update the source reference
                access.column_node->setColumnSource(cloned_source);
                continue;
            }

            /// Replace the getSubcolumn call with a direct column reference
            NameAndTypePair new_column_name_and_type{access.full_subcolumn_name, access.subcolumn_type};
            *access.node_to_replace = std::make_shared<ColumnNode>(new_column_name_and_type, cloned_source);
        }
    }

    /// Note: The original base columns (e.g., "tup") are now unused in the cloned projection.
    /// RemoveUnusedProjectionColumnsPass will clean them up if they're not referenced elsewhere.
}

}
