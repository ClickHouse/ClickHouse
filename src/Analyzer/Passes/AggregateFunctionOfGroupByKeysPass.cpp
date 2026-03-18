#include <Analyzer/Passes/AggregateFunctionOfGroupByKeysPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_aggregators_of_group_by_keys;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Try to eliminate min/max/any/anyLast.
class EliminateFunctionVisitor : public InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>;
    using Base::Base;

    using GroupByKeysStack = std::vector<QueryTreeNodePtrWithHashSet>;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_aggregators_of_group_by_keys])
            return;

        /// Collect group by keys.
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        if (!query_node->hasGroupBy())
        {
            group_by_keys_stack.push_back({});
        }
        else if (query_node->isGroupByWithTotals() || query_node->isGroupByWithCube() || query_node->isGroupByWithRollup())
        {
            /// Keep aggregator if group by is with totals/cube/rollup.
            group_by_keys_stack.push_back({});
        }
        else
        {
            QueryTreeNodePtrWithHashSet group_by_keys;
            for (auto & group_key : query_node->getGroupBy().getNodes())
            {
                /// For grouping sets case collect only keys that are presented in every set.
                if (auto * list = group_key->as<ListNode>())
                {
                    QueryTreeNodePtrWithHashSet common_keys_set;
                    for (auto & group_elem : list->getNodes())
                    {
                        if (group_by_keys.contains(group_elem))
                            common_keys_set.insert(group_elem);
                    }
                    group_by_keys = std::move(common_keys_set);
                }
                else
                {
                    group_by_keys.insert(group_key);
                }
            }
            group_by_keys_stack.push_back(std::move(group_by_keys));
        }
    }

    /// Now we visit all nodes in QueryNode, we should remove group_by_keys from stack.
    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_aggregators_of_group_by_keys])
            return;

        if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            if (aggregationCanBeEliminated(node, group_by_keys_stack.back()))
                node = node->as<FunctionNode>()->getArguments().getNodes()[0];
        }
        else if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            group_by_keys_stack.pop_back();
        }
    }

    static bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child)
    {
        /// Skip ArrayJoin.
        return !child->as<ArrayJoinNode>();
    }

private:

    struct NodeWithInfo
    {
        QueryTreeNodePtr node;
        bool parents_are_only_deterministic = false;
    };

    bool aggregationCanBeEliminated(QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & group_by_keys)
    {
        if (group_by_keys.empty())
            return false;

        auto * function = node->as<FunctionNode>();
        if (!function || !function->isAggregateFunction())
            return false;

        if (!(function->getFunctionName() == "min"
                || function->getFunctionName() == "max"
                || function->getFunctionName() == "any"
                || function->getFunctionName() == "anyLast"))
            return false;

        std::vector<NodeWithInfo> candidates;
        auto & function_arguments = function->getArguments().getNodes();
        if (function_arguments.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a single argument of function '{}' but received {}", function->getFunctionName(), function_arguments.size());

        if (!function->getResultType()->equals(*function_arguments[0]->getResultType()))
            return false;

        candidates.push_back({ function_arguments[0], true });

        /// Using DFS we traverse function tree and try to find if it uses other keys as function arguments.
        while (!candidates.empty())
        {
            auto [candidate, parents_are_only_deterministic] = candidates.back();
            candidates.pop_back();

            bool found = group_by_keys.contains(candidate);

            switch (candidate->getNodeType())
            {
                case QueryTreeNodeType::FUNCTION:
                {
                    auto * func = candidate->as<FunctionNode>();
                    auto & arguments = func->getArguments().getNodes();
                    if (arguments.empty())
                        return false;

                    if (!found)
                    {
                        bool is_deterministic_function = parents_are_only_deterministic &&
                            func->getFunctionOrThrow()->isDeterministicInScopeOfQuery();
                        for (auto it = arguments.rbegin(); it != arguments.rend(); ++it)
                            candidates.push_back({ *it, is_deterministic_function });
                    }
                    break;
                }
                case QueryTreeNodeType::COLUMN:
                    if (!found)
                        return false;
                    break;
                case QueryTreeNodeType::CONSTANT:
                    if (!parents_are_only_deterministic)
                        return false;
                    break;
                default:
                    return false;
            }
        }

        return true;
    }

    GroupByKeysStack group_by_keys_stack;
};

}

void AggregateFunctionOfGroupByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    EliminateFunctionVisitor eliminator(context);
    eliminator.visit(query_tree_node);
}

};
