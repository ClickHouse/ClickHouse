#include "AggregateFunctionOfGroupByKeysPass.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/ArrayJoinNode.h>

namespace DB
{

namespace
{

/// Check whether we should keep aggregator.
class KeepEliminateFunctionVisitor : public ConstInDepthQueryTreeVisitor<KeepEliminateFunctionVisitor>
{
public:
    using Base = ConstInDepthQueryTreeVisitor<KeepEliminateFunctionVisitor>;
    using Base::Base;

    explicit KeepEliminateFunctionVisitor(const QueryTreeNodes & group_by_keys_, bool & keep_aggregator_)
        : group_by_keys(group_by_keys_), keep_aggregator(keep_aggregator_)
    {
    }

    static bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType & child [[maybe_unused]])
    {
        return parent->as<ListNode>();
    }

    void visitFunction(const FunctionNode * function_node)
    {
        if (function_node->getArguments().getNodes().empty())
        {
            keep_aggregator = true;
            return;
        }
        auto it = std::find_if(
            group_by_keys.begin(),
            group_by_keys.end(),
            [function_node](const QueryTreeNodePtr & group_by_ele) { return function_node->isEqual(*group_by_ele); });

        if (it == group_by_keys.end())
        {
            KeepEliminateFunctionVisitor visitor(group_by_keys, keep_aggregator);
            visitor.visit(function_node->getArgumentsNode());
        }
    }

    void visitColumn(const ColumnNode * column)
    {
        /// if variable of a function is not in GROUP BY keys, this function should not be deleted
        auto it = std::find_if(
            group_by_keys.begin(),
            group_by_keys.end(),
            [column](const QueryTreeNodePtr & group_by_ele) { return column->isEqual(*group_by_ele); });

        if (it == group_by_keys.end())
            keep_aggregator = true;
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (keep_aggregator)
            return;

        if (node->as<ListNode>())
            return;

        if (auto * function_node = node->as<FunctionNode>())
        {
            visitFunction(function_node);
        }
        else if (auto * column = node->as<ColumnNode>())
        {
            visitColumn(column);
        }
    }

private :
    const QueryTreeNodes & group_by_keys;
    bool & keep_aggregator;
};

/// Try to eliminate min/max/any/anyLast.
class EliminateFunctionVisitor : public InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>;
    using Base::Base;

    using GroupByKeysStack = std::vector<QueryTreeNodes>;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_aggregators_of_group_by_keys)
            return;

        /// (1) collect group by keys
        if (auto * query_node = node->as<QueryNode>())
        {
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
                QueryTreeNodes group_by_keys;
                for (auto & group_key : query_node->getGroupBy().getNodes())
                {
                    /// for grouping sets case
                    if (auto * list = group_key->as<ListNode>())
                    {
                        for (auto & group_elem : list->getNodes())
                            group_by_keys.push_back(group_elem);
                    }
                    else
                    {
                        group_by_keys.push_back(group_key);
                    }
                }
                group_by_keys_stack.push_back(std::move(group_by_keys));
            }
        }
        /// (2) Try to eliminate any/min/max
        else if (auto * function_node = node->as<FunctionNode>())
        {
            if (!function_node
                || !(function_node->getFunctionName() == "min" || function_node->getFunctionName() == "max"
                     || function_node->getFunctionName() == "any" || function_node->getFunctionName() == "anyLast"))
                return;

            if (!function_node->getArguments().getNodes().empty())
            {
                bool keep_aggregator = false;

                KeepEliminateFunctionVisitor visitor(group_by_keys_stack.back(), keep_aggregator);
                visitor.visit(function_node->getArgumentsNode());

                /// Place argument of an aggregate function instead of function
                if (!keep_aggregator)
                    node = function_node->getArguments().getNodes()[0];
            }
        }

    }

    /// Now we visit all nodes in QueryNode, we should remove group_by_keys from stack.
    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_aggregators_of_group_by_keys)
            return;

        if (auto * query_node = node->as<QueryNode>())
            group_by_keys_stack.pop_back();
    }

    static bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child)
    {
        /// Skip ArrayJoin.
        return !child->as<ArrayJoinNode>();
    }

private:
    GroupByKeysStack group_by_keys_stack;

};

}

void AggregateFunctionOfGroupByKeysPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    EliminateFunctionVisitor eliminator(context);
    eliminator.visit(query_tree_node);
}

};
