#include <Analyzer/Passes/OptimizeRedundantFunctionsInOrderByPass.h>

#include <Functions/IFunction.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Core/Settings.h>

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
            auto & order_by_expr = elem->as<SortNode>()->getExpression();
            switch (order_by_expr->getNodeType())
            {
                case QueryTreeNodeType::FUNCTION:
                {
                    if (isRedundantExpression(order_by_expr))
                        continue;
                    break;
                }
                case QueryTreeNodeType::COLUMN:
                {
                    existing_keys.insert(order_by_expr);
                    break;
                }
                default:
                    break;
            }

            new_order_by_nodes.push_back(elem);
        }
        existing_keys.clear();

        if (new_order_by_nodes.size() < order_by.getNodes().size())
            order_by.getNodes() = std::move(new_order_by_nodes);
    }

private:
    QueryTreeNodePtrWithHashSet existing_keys;

    bool isRedundantExpression(QueryTreeNodePtr function)
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
                    if (!existing_keys.contains(node))
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
