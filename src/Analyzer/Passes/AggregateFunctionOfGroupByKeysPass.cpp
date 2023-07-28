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

/// Try to eliminate min/max/any/anyLast which will not decent into subqueries.
class EliminateFunctionVisitor : public InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<EliminateFunctionVisitor>;
    using Base::Base;

    explicit EliminateFunctionVisitor(const QueryTreeNodes & group_by_keys_, ContextPtr context) : Base(context), group_by_keys(group_by_keys_) { }

    void enterImpl(QueryTreeNodePtr & node)
    {
        /// Check if function is min/max/any/anyLast
        auto * function_node = node->as<FunctionNode>();
        if (!function_node
            || !(function_node->getFunctionName() == "min" || function_node->getFunctionName() == "max"
                 || function_node->getFunctionName() == "any" || function_node->getFunctionName() == "anyLast"))
            return;

        if (!function_node->getArguments().getNodes().empty())
        {
            bool keep_aggregator = false;

            KeepEliminateFunctionVisitor visitor(group_by_keys, keep_aggregator);
            visitor.visit(function_node->getArgumentsNode());

            /// Place argument of an aggregate function instead of function
            if (!keep_aggregator)
                node = function_node->getArguments().getNodes()[0];
        }
    }

    static bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child)
    {
        /// Don't descent into table functions and subqueries and special case for ArrayJoin.
        return !child->as<QueryNode>() && !child->as<TableNode>() && !child->as<ArrayJoinNode>();
    }

private:
    const QueryTreeNodes & group_by_keys;
};

/// Collect QueryNode and its group by keys.
class CollectQueryAndGroupByKeysVisitor : public InDepthQueryTreeVisitorWithContext<CollectQueryAndGroupByKeysVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CollectQueryAndGroupByKeysVisitor>;
    using Base::Base;

    using Data = std::unordered_map<QueryTreeNodePtr, QueryTreeNodes>;
    Data data;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_aggregators_of_group_by_keys)
            return;

        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        if (!query_node->hasGroupBy())
            return;

        if (query_node->isGroupByWithTotals() || query_node->isGroupByWithCube() || query_node->isGroupByWithRollup())
            return;

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
        data.insert({node, std::move(group_by_keys)});
    }
};

}

void AggregateFunctionOfGroupByKeysPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    CollectQueryAndGroupByKeysVisitor collector(context);
    collector.visit(query_tree_node);

    for (auto & [query_node, group_by_keys] : collector.data)
    {
        EliminateFunctionVisitor eliminator(group_by_keys, query_node->as<QueryNode>()->getContext());
        auto mutable_query_node = query_node;
        eliminator.visit(mutable_query_node);
    }
}

};
