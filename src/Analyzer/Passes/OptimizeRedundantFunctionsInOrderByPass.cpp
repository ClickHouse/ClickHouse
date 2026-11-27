#include <Analyzer/Passes/OptimizeRedundantFunctionsInOrderByPass.h>

#include <Functions/IFunction.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Core/SortDescription.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_redundant_functions_in_order_by;
}

namespace
{

class OptimizeRedundantFunctionsInOrderByVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeRedundantFunctionsInOrderByVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeRedundantFunctionsInOrderByVisitor>;
    using Base::Base;

    static bool needChildVisit(QueryTreeNodePtr & node, QueryTreeNodePtr & /*parent*/)
    {
        if (node->as<FunctionNode>())
            return false;
        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_redundant_functions_in_order_by])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasOrderBy())
            return;

        /// A function of a key can be another key, which has a default value in subtotals.
        bool keep_functions = query->isGroupByWithRollup() || query->isGroupByWithCube() || query->isGroupByWithGroupingSets();

        auto & order_by = query->getOrderBy();
        for (auto & elem : order_by.getNodes())
        {
            auto * order_by_elem = elem->as<SortNode>();
            if (order_by_elem->withFill())
                return;
        }

        QueryTreeNodes new_order_by_nodes;
        new_order_by_nodes.reserve(order_by.getNodes().size());

        for (auto & elem : order_by.getNodes())
        {
            auto & sort_node = elem->as<SortNode &>();
            auto & order_by_expr = sort_node.getExpression();
            switch (order_by_expr->getNodeType())
            {
                case QueryTreeNodeType::FUNCTION:
                {
                    /// Not a function of the previous column keys: the key is needed, and it is not changed.
                    if (!isRedundantExpression(order_by_expr, existing_keys))
                        break;
                    if (!keep_functions && isRedundantExpression(order_by_expr, exact_keys))
                        continue;
                    /// The key is a function of the previous column keys, but some of them do not compare by value, or
                    /// the query has subtotals. A positional argument is a deep copy of the projection expression. It keeps
                    /// the aliases that the projection loses, so a remote server can find different definitions of them.
                    removeAliasesRecursive(order_by_expr);
                    break;
                }
                case QueryTreeNodeType::COLUMN:
                {
                    existing_keys.insert(order_by_expr);
                    /// Different values can compare equal: with a collation, or `-0.0` and `0.0` and the `NaN`
                    /// payloads. Functions of them can tell them apart, so such a key does not make them redundant.
                    if (!sort_node.getCollator() && !comparisonCanMergeDistinctValues(*order_by_expr->getResultType()))
                        exact_keys.insert(order_by_expr);
                    break;
                }
                default:
                    break;
            }

            new_order_by_nodes.push_back(elem);
        }
        existing_keys.clear();
        exact_keys.clear();

        if (new_order_by_nodes.size() < order_by.getNodes().size())
            order_by.getNodes() = std::move(new_order_by_nodes);
    }

private:
    /// All the previous column keys.
    QueryTreeNodePtrWithHashSet existing_keys;
    /// The previous column keys that compare by value. Only they make a function of them redundant.
    QueryTreeNodePtrWithHashSet exact_keys;

    static bool isRedundantExpression(QueryTreeNodePtr function, const QueryTreeNodePtrWithHashSet & keys)
    {
        QueryTreeNodes nodes_to_process{ function };
        while (!nodes_to_process.empty())
        {
            auto node = nodes_to_process.back();
            nodes_to_process.pop_back();

            // TODO: handle constants here
            switch (node->getNodeType())
            {
                case QueryTreeNodeType::FUNCTION:
                {
                    auto * function_node = node->as<FunctionNode>();
                    const auto & function_arguments = function_node->getArguments().getNodes();
                    if (function_arguments.empty())
                        return false;
                    const auto & function_base = function_node->getFunction();
                    if (!function_base || !function_base->isDeterministicInScopeOfQuery())
                        return false;

                    // Process arguments in order
                    for (auto it = function_arguments.rbegin(); it != function_arguments.rend(); ++it)
                        nodes_to_process.push_back(*it);
                    break;
                }
                case QueryTreeNodeType::COLUMN:
                {
                    if (!keys.contains(node))
                        return false;
                    break;
                }
                default:
                    return false;
            }
        }
        return true;
    }
};

}

void OptimizeRedundantFunctionsInOrderByPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeRedundantFunctionsInOrderByVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
