#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/TableFunctionNode.h>

namespace DB
{

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
/// @param func A callable that takes (current_node) to be applied to each visited node.
template <typename ShouldVisitPredicate, typename Func>
void traverseQueryTree(const QueryTreeNodePtr & node, ShouldVisitPredicate should_visit_predicate, Func func)
{
    QueryTreeNodes nodes_to_process = { node };

    while (!nodes_to_process.empty())
    {
        auto current_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        func(current_node);

        if (auto * table_function_node = current_node->as<TableFunctionNode>())
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
                            nodes_to_process.push_back(argument_node);
                        }
                    }
                }
                else
                {
                    if (should_visit_predicate(current_node, child))
                        nodes_to_process.push_back(child);
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
                    nodes_to_process.push_back(child);
            }
        }
    }
}

}
