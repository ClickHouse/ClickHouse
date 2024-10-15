#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB::QueryPlanOptimizations
{

static void removeFromOutputs(ActionsDAG & dag, const ActionsDAG::Node & node)
{
    auto & outputs = dag.getOutputs();
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (&node == outputs[i])
        {
            outputs.erase(outputs.begin() + i);
            return;
        }
    }
}

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
        auto & child_actions = child_expr->getExpression();
        auto & parent_actions = parent_expr->getExpression();

        /// We cannot combine actions with arrayJoin and stateful function because we not always can reorder them.
        /// Example: select rowNumberInBlock() from (select arrayJoin([1, 2]))
        /// Such a query will return two zeroes if we combine actions together.
        if (child_actions.hasArrayJoin() && parent_actions.hasStatefulFunctions())
            return 0;

        auto merged = ActionsDAG::merge(std::move(child_actions), std::move(parent_actions));

        auto expr = std::make_unique<ExpressionStep>(child_expr->getInputHeaders().front(), std::move(merged));
        expr->setStepDescription("(" + parent_expr->getStepDescription() + " + " + child_expr->getStepDescription() + ")");

        parent_node->step = std::move(expr);
        parent_node->children.swap(child_node->children);
        return 1;
    }
    if (parent_filter && child_expr)
    {
        auto & child_actions = child_expr->getExpression();
        auto & parent_actions = parent_filter->getExpression();

        if (child_actions.hasArrayJoin() && parent_actions.hasStatefulFunctions())
            return 0;

        auto merged = ActionsDAG::merge(std::move(child_actions), std::move(parent_actions));

        auto filter = std::make_unique<FilterStep>(
            child_expr->getInputHeaders().front(),
            std::move(merged),
            parent_filter->getFilterColumnName(),
            parent_filter->removesFilterColumn());
        filter->setStepDescription("(" + parent_filter->getStepDescription() + " + " + child_expr->getStepDescription() + ")");

        parent_node->step = std::move(filter);
        parent_node->children.swap(child_node->children);
        return 1;
    }

    return 0;
}
size_t tryMergeFilters(QueryPlan::Node * parent_node, QueryPlan::Nodes &)
{
    if (parent_node->children.size() != 1)
        return false;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;

    auto * parent_filter = typeid_cast<FilterStep *>(parent.get());
    auto * child_filter = typeid_cast<FilterStep *>(child.get());

    if (parent_filter && child_filter)
    {
        auto & child_actions = child_filter->getExpression();
        auto & parent_actions = parent_filter->getExpression();

        if (child_actions.hasArrayJoin())
            return 0;

        const auto & child_filter_node = child_actions.findInOutputs(child_filter->getFilterColumnName());
        if (child_filter->removesFilterColumn())
            removeFromOutputs(child_actions, child_filter_node);

        child_actions.mergeInplace(std::move(parent_actions));

        const auto & parent_filter_node = child_actions.findInOutputs(parent_filter->getFilterColumnName());
        if (parent_filter->removesFilterColumn())
            removeFromOutputs(child_actions, parent_filter_node);

        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        const auto & condition = child_actions.addFunction(func_builder_and, {&child_filter_node, &parent_filter_node}, {});
        auto & outputs = child_actions.getOutputs();
        outputs.insert(outputs.begin(), &condition);

        child_actions.removeUnusedActions(false);

        auto filter = std::make_unique<FilterStep>(child_filter->getInputHeaders().front(),
                                                   std::move(child_actions),
                                                   condition.result_name,
                                                   true);
        filter->setStepDescription("(" + parent_filter->getStepDescription() + " + " + child_filter->getStepDescription() + ")");

        parent_node->step = std::move(filter);
        parent_node->children.swap(child_node->children);
        return 1;
    }

    return 0;
}

}
