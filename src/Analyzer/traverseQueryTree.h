#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/TableFunctionNode.h>

namespace DB
{

struct NoOp
{
    void operator()(const QueryTreeNodePtr &) const {}
};

/// Traverse query tree in depth-first manner, applying `func` to each node.
/// `should_visit_predicate` is called for each child node to determine whether to visit it or not.
/// If the node is a TableFunctionNode, its arguments that are not resolved are skipped during traversal.
///
/// Note: This function implements non-recursive traversal to avoid stack overflow on deep trees.
/// If you need recursive traversal or you need to use query context, consider using InDepthQueryTreeVisitorWithContext.
/// This function is suitable for simple use cases where you just need to apply a function to each node.
///
/// @param node The root node of the query tree to traverse.
/// @param should_visit_predicate A callable that takes (parent_node, child_node) and returns a bool indicating whether to visit the child node.
/// @param func_enter A callable that takes (current_node) to be applied to each visited node upon entering subtree.
/// @param func_leave A callable that takes (current_node) to be applied to each visited node upon leaving subtree.
template <typename ShouldVisitPredicate, typename FuncEnter, typename FuncLeave = NoOp>
void traverseQueryTree(const QueryTreeNodePtr & node, ShouldVisitPredicate should_visit_predicate, FuncEnter func_enter, FuncLeave func_leave = {})
{
    struct Frame
    {
        QueryTreeNodePtr node;
        bool children_processed = false;
    };
    std::vector<Frame> nodes_to_process = { { .node = node } };

    while (!nodes_to_process.empty())
    {
        auto [current_node, children_processed] = nodes_to_process.back();

        if (children_processed)
        {
            func_leave(current_node);
            nodes_to_process.pop_back();
            continue;
        }
        else
        {
            func_enter(current_node);
            nodes_to_process.back().children_processed = true;
        }

        if (auto * table_function_node = current_node->template as<TableFunctionNode>())
        {
            for (const auto & child : current_node->getChildren())
            {
                if (!child)
                    continue;

                if (child == table_function_node->getArgumentsNode())
                {
                    const auto & unresolved_indexes = table_function_node->getUnresolvedArgumentIndexes();
                    const auto & arguments_nodes = table_function_node->getArguments().getNodes();

                    for (size_t index = 0; index < arguments_nodes.size(); ++index)
                    {
                        const auto & argument_node = arguments_nodes[index];
                        if (std::find(unresolved_indexes.begin(), unresolved_indexes.end(), index) == unresolved_indexes.end())
                        {
                            nodes_to_process.push_back({ .node = argument_node });
                        }
                    }
                }
                else
                {
                    if (should_visit_predicate(current_node, child))
                        nodes_to_process.push_back({ .node = child });
                }
            }
        }
        else
        {
            for (const auto & child : current_node->getChildren())
            {
                if (!child)
                    continue;

                if (should_visit_predicate(current_node, child))
                    nodes_to_process.push_back({ .node = child });
            }
        }
    }
}

struct ExceptSubqueries
{
    bool operator()(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node) const
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }
};

struct Everything
{
    bool operator()(const QueryTreeNodePtr &, const QueryTreeNodePtr &) const
    {
        return true;
    }
};

}
