#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>

namespace DB::QueryPlanOptimizations
{

/// Split FilterStep into chain `ExpressionStep -> FilterStep`, where FilterStep contains minimal number of nodes.
size_t trySplitFilter(QueryPlan::Node * node, QueryPlan::Nodes & nodes)
{
    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (!filter_step)
        return 0;

    const auto & expr = filter_step->getExpression();

    /// Do not split if there are function like runningDifference.
    if (expr->hasStatefulFunctions())
        return 0;

    auto split = expr->splitActionsForFilter(filter_step->getFilterColumnName());

    if (split.second->trivial())
        return 0;

    bool remove_filter = false;
    if (filter_step->removesFilterColumn())
        remove_filter = split.second->removeUnusedResult(filter_step->getFilterColumnName());

    auto description = filter_step->getStepDescription();

    auto & filter_node = nodes.emplace_back();
    node->children.swap(filter_node.children);
    node->children.push_back(&filter_node);

    filter_node.step = std::make_unique<FilterStep>(
            filter_node.children.at(0)->step->getOutputStream(),
            std::move(split.first),
            filter_step->getFilterColumnName(),
            remove_filter);

    node->step = std::make_unique<ExpressionStep>(filter_node.step->getOutputStream(), std::move(split.second));

    filter_node.step->setStepDescription("(" + description + ")[split]");
    node->step->setStepDescription(description);

    return 2;
}

}
