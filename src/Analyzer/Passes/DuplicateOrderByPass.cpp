#include "DuplicateOrderByPass.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/JoinNode.h>

namespace DB
{

namespace
{

/// Remove order by clause from subquery node
void removeOrderby(QueryNode * subquery_node)
{
    if (!subquery_node->hasOrderBy())
        return;

    /// If we have limits then the ORDER BY is non-removable.
    if (subquery_node->hasLimit() || subquery_node->hasLimitBy()
        || subquery_node->hasLimitByLimit() || subquery_node->hasLimitByOffset())
        return;

    /// If ORDER BY contains filling (in addition to sorting) it is non-removable.
    for (const auto & child : subquery_node->getOrderBy().getNodes())
    {
        if (auto * sort_node = child->as<SortNode>())
        {
            if (sort_node->hasFillStep() || sort_node->hasFillTo() || sort_node->hasFillFrom())
                return;
        }
    }

    /// remove order by
    subquery_node->getOrderBy().getNodes().clear();
}

/// remove order by from join tree
void removeFromJoinTree(QueryTreeNodePtr& join_tree)
{
    if (auto * subquery_node = join_tree->as<QueryNode>())
    {
        removeOrderby(subquery_node);
    }
    else if (auto * join_node = join_tree->as<JoinNode>())
    {
        removeFromJoinTree(join_node->getLeftTableExpression());
        removeFromJoinTree(join_node->getRightTableExpression());
    }
};

/// Whether nodes contain stateful functions
class StatefulFunctionsVisitor : public ConstInDepthQueryTreeVisitor<StatefulFunctionsVisitor>
{
public:
    using Base = ConstInDepthQueryTreeVisitor<StatefulFunctionsVisitor>;
    using Base::Base;

    bool contains_stateful_function = false;

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (auto * to_check_func = node->as<FunctionNode>())
        {
            if (to_check_func->getAggregateFunction())
            {
                auto agg_func_properties = AggregateFunctionFactory::instance().tryGetProperties(to_check_func->getFunctionName());
                if (agg_func_properties && agg_func_properties->is_order_dependent)
                    contains_stateful_function = true;
            }
            else if (to_check_func->getFunction()->isStateful())
            {
                contains_stateful_function = true;
            }
        }
    }
};

bool containsStatefulFunc(const QueryTreeNodes & nodes)
{
    for (const auto & node : nodes)
    {
        StatefulFunctionsVisitor visitor;
        visitor.visit(node);
        if (visitor.contains_stateful_function)
            return true;
    }
    return false;
}

class DuplicateOrderByVisitor : public InDepthQueryTreeVisitorWithContext<DuplicateOrderByVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DuplicateOrderByVisitor>;
    using Base::Base;

    static bool shouldTraverseTopToBottom() const
    {
        return false;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_duplicate_order_by_and_distinct)
            return;

        auto * query_node = node->as<QueryNode>();

        if (!query_node)
            return;

        if (!query_node->hasOrderBy() && !query_node->hasGroupBy())
            return;

        if (containsStatefulFunc(query_node->getProjection().getNodes()))
            return;

        if (query_node->hasGroupBy())
        {
            if (containsStatefulFunc(query_node->getGroupBy().getNodes()))
                return;
        }

        /// checking done, remove order by
        removeFromJoinTree(query_node->getJoinTree());
    }
};

}

void DuplicateOrderByPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    DuplicateOrderByVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
