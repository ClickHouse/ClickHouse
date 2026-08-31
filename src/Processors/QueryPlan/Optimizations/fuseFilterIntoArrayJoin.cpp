#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

/// Move a filter's element-only conjuncts into the ArrayJoinStep below it, so they run in element
/// space before expansion. Runs after filterPushDown, which already pushed the non-element conjuncts down
size_t tryFuseFilterIntoArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings & settings)
{
    /// fused filter can't be read by older workers, and it's perf-only - skip if the plan may be shipped out
    if (settings.make_distributed_plan || settings.serialize_query_plan)
        return 0;

    auto & parent = parent_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());
    if (!filter || parent_node->children.size() != 1)
        return 0;

    auto * child_node = parent_node->children.front();
    auto * array_join = typeid_cast<ArrayJoinStep *>(child_node->step.get());
    /// skip LEFT: optimizeReadInOrder keeps LIMIT through it, but a fused filter drops rows
    if (!array_join || array_join->isLeft() || array_join->hasElementFilter())
        return 0;

    auto & expression = filter->getExpression();
    /// v1: never hoist a nested arrayJoin into element space
    if (expression.hasArrayJoin())
        return 0;

    const auto & joined_columns = array_join->getColumns();
    NameSet joined_set(joined_columns.begin(), joined_columns.end());

    /// The element filter runs on just the joined columns, so its inputs must be exactly those
    ColumnsWithTypeAndName all_inputs;
    for (const auto & column : filter->getInputHeaders().front()->getColumnsWithTypeAndName())
        if (joined_set.contains(column.name))
            all_inputs.push_back(column);
    if (all_inputs.empty())
        return 0;

    /// Only fuse when the WHOLE filter moves into the ARRAY JOIN. If any conjunct must stay above, lifting
    /// an element conjunct out of the AND changes short-circuit evaluation - a throwing element predicate
    /// (intDiv(1, elem)) could run where a sibling above would have skipped it, and ARRAY JOIN does not
    /// support short-circuit. Split on a clone so bailing out leaves the real filter untouched.
    auto residual = expression.clone();
    auto split = residual.splitActionsForFilterPushDown(
        filter->getFilterColumnName(),
        filter->removesFilterColumn(),
        joined_columns,
        all_inputs,
        /*allow_non_deterministic_functions=*/false);
    if (!split)
        return 0;

    /// Fully fused iff nothing is left to filter above: the residual filter column was removed, or it
    /// collapsed to a constant. Otherwise a real sibling conjunct remains and we must not fuse.
    const bool fully_fused = !residual.tryFindInOutputs(filter->getFilterColumnName()) || split->is_filter_const_after_push_down;
    if (!fully_fused)
        return 0;

    /// With short-circuit off, a FilterStep still masks a throwing atom by splitting the AND into sequential
    /// filters. The fused path runs the whole predicate at once, so don't fuse a multi-atom AND in this mode.
    const auto * filter_node = split->dag.getOutputs()[split->filter_pos];
    if (settings.short_circuit_function_evaluation_disabled
        && filter_node->type == ActionsDAG::ActionType::FUNCTION
        && filter_node->function_base && filter_node->function_base->getName() == "and")
        return 0;

    String element_filter_column = filter_node->result_name;
    array_join->setElementFilter(std::move(split->dag), element_filter_column, split->remove_filter);

    /// The whole filter is now the element filter; replace it with its residual (an all-columns pass-through).
    const auto & array_join_output = child_node->step->getOutputHeader();
    auto expression_step = std::make_unique<ExpressionStep>(array_join_output, std::move(residual));
    expression_step->setStepDescription(*filter);
    parent = std::move(expression_step);

    return 2;
}

}
