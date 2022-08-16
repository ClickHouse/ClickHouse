#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB::QueryPlanOptimizations
{

size_t tryMergeExpressions(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
{
    if (parent_node->children.size() != 1)
        return false;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * parent_expr = typeid_cast<ExpressionStep *>(parent.get());
    auto * parent_filter = typeid_cast<FilterStep *>(parent.get());
    auto * child_expr = typeid_cast<ExpressionStep *>(child.get());

    if (parent_expr && child_expr)
    {
        const auto & child_actions = child_expr->getExpression();
        const auto & parent_actions = parent_expr->getExpression();

        /// We cannot combine actions with arrayJoin and stateful function because we not always can reorder them.
        /// Example: select rowNumberInBlock() from (select arrayJoin([1, 2]))
        /// Such a query will return two zeroes if we combine actions together.
        if (child_actions->hasArrayJoin() && parent_actions->hasStatefulFunctions())
            return 0;

        auto merged = ActionsDAG::merge(std::move(*child_actions->clone()), std::move(*parent_actions->clone()));

        auto expr = std::make_unique<ExpressionStep>(child_expr->getInputStreams().front(), merged);
        expr->setStepDescription("(" + parent_expr->getStepDescription() + " + " + child_expr->getStepDescription() + ")");

        parent_node->step = std::move(expr);
        parent_node->children.swap(child_node->children);
        return 1;
    }
    else if (parent_filter && child_expr)
    {
        const auto & child_actions = child_expr->getExpression();
        const auto & parent_actions = parent_filter->getExpression();

        if (child_actions->hasArrayJoin() && parent_actions->hasStatefulFunctions())
            return 0;

        auto merged = ActionsDAG::merge(std::move(*child_actions->clone()), std::move(*parent_actions->clone()));

        auto filter = std::make_unique<FilterStep>(child_expr->getInputStreams().front(),
                                                   merged,
                                                   parent_filter->getFilterColumnName(),
                                                   parent_filter->removesFilterColumn());
        filter->setStepDescription("(" + parent_filter->getStepDescription() + " + " + child_expr->getStepDescription() + ")");

        parent_node->step = std::move(filter);
        parent_node->children.swap(child_node->children);
        return 1;
    }

    return 0;
}

}
