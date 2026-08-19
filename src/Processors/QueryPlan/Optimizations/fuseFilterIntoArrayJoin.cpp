#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

static bool dagHasArrayJoin(const ActionsDAG & dag)
{
    for (const auto & node : dag.getNodes())
        if (node.type == ActionsDAG::ActionType::ARRAY_JOIN)
            return true;
    return false;
}

/// Move a filter's element-only conjuncts into the ArrayJoinStep below it, so they run in element
/// space before expansion. Runs after filterPushDown, which already pushed the non-element conjuncts down
size_t tryFuseFilterIntoArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings &)
{
    auto & parent = parent_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());
    if (!filter || parent_node->children.size() != 1)
        return 0;

    auto * child_node = parent_node->children.front();
    auto * array_join = typeid_cast<ArrayJoinStep *>(child_node->step.get());
    if (!array_join || array_join->hasElementFilter())
        return 0;

    auto & expression = filter->getExpression();
    /// v1: never hoist a nested arrayJoin into element space
    if (dagHasArrayJoin(expression))
        return 0;

    const auto & joined_columns = array_join->getColumns();
    Names available_inputs(joined_columns.begin(), joined_columns.end());
    NameSet joined_set(joined_columns.begin(), joined_columns.end());

    /// The element filter runs on just the joined columns, so its inputs must be exactly those
    ColumnsWithTypeAndName all_inputs;
    for (const auto & column : filter->getInputHeaders().front()->getColumnsWithTypeAndName())
        if (joined_set.contains(column.name))
            all_inputs.push_back(column);
    if (all_inputs.empty())
        return 0;

    auto split = expression.splitActionsForFilterPushDown(
        filter->getFilterColumnName(),
        filter->removesFilterColumn(),
        available_inputs,
        all_inputs,
        /*allow_non_deterministic_functions=*/false);
    if (!split)
        return 0;

    String element_filter_column = split->dag.getOutputs()[split->filter_pos]->result_name;
    array_join->setElementFilter(std::move(split->dag), element_filter_column, split->remove_filter);

    /// ARRAY JOIN output columns are unchanged - the fused filter only drops rows
    const auto & array_join_output = child_node->step->getOutputHeader();
    const auto * filter_node = expression.tryFindInOutputs(filter->getFilterColumnName());
    if (!filter_node || split->is_filter_const_after_push_down)
    {
        /// Every conjunct was fused - the filter above is now a no-op
        auto expression_step = std::make_unique<ExpressionStep>(array_join_output, std::move(expression));
        expression_step->setStepDescription(*filter);
        parent = std::move(expression_step);
    }
    else
    {
        filter->updateInputHeader(array_join_output);
    }

    return 2;
}

}
