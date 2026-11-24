#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ActionsDAGTraversal.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/IFunction.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

/// Move a filter on the ARRAY JOINed elements into the ArrayJoinStep below it, so it runs in element
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
    if (!array_join || array_join->hasElementFilter())
        return 0;

    auto & expression = filter->getExpression();
    /// v1: never hoist a nested arrayJoin into element space
    if (expression.hasArrayJoin())
        return 0;

    const auto & joined_columns = array_join->getColumns();

    if (!expression.splitActionsBeforeArrayJoin(joined_columns).first.trivial())
        return 0;

    NameSet joined_set(joined_columns.begin(), joined_columns.end());

    const auto required
        = ActionsDAG::cloneSubDAG({&expression.findInOutputs(filter->getFilterColumnName())}, false).getRequiredColumnsNames();
    NameSet required_set(required.begin(), required.end());
    ColumnsWithTypeAndName all_inputs;
    for (const auto & column : filter->getInputHeaders().front()->getColumnsWithTypeAndName())
        if (required_set.contains(column.name))
            all_inputs.push_back(column);

    /// Split on a clone so bailing out leaves the real filter untouched.
    auto residual = expression.clone();
    auto split = residual.splitActionsForFilterPushDown(
        filter->getFilterColumnName(),
        filter->removesFilterColumn(),
        required,
        all_inputs,
        /*allow_non_deterministic_functions=*/false);
    if (!split)
        return 0;

    const auto * filter_node = split->dag.getOutputs()[split->filter_pos];

    /// the element filter can read row columns (the step broadcasts them), but it must read an element
    if (!anyReachableNode({filter_node}, [&](const auto * node) { return node->type == ActionsDAG::ActionType::INPUT && joined_set.contains(node->result_name); }))
        return 0;

    /// Fully fused iff nothing is left to filter above: the residual filter column was removed, or it
    /// collapsed to a constant.
    const bool fully_fused = !residual.tryFindInOutputs(filter->getFilterColumnName()) || split->is_filter_const_after_push_down;

    /// a conjunct that stays above (e.g. with rand) no longer guards the fused ones and sees fewer rows
    auto ignores_row_count = [](const IFunctionBase & function) { return !function.isStateful() && !function.hasObservableSideEffects(); };
    if (!fully_fused
        && (!std::ranges::all_of(expression.getNodes(), [&](const auto & node) { return allNodeFunctions(node, ignores_row_count); })
            || !std::ranges::all_of(ActionsDAG::extractConjunctionAtoms(filter_node), canBeEvaluatedOnMoreRows)))
        return 0;

    /// With short-circuit off, a FilterStep still masks a throwing atom by splitting the AND into sequential
    /// filters. The fused path runs the whole predicate at once, so don't fuse a multi-atom AND in this mode.
    if (settings.short_circuit_function_evaluation_disabled
        && filter_node->type == ActionsDAG::ActionType::FUNCTION
        && filter_node->function_base && filter_node->function_base->getName() == "and")
        return 0;

    String element_filter_column = filter_node->result_name;

    /// the step broadcasts each input of the element filter: drop the columns that only the conjuncts above read
    NameSet element_filter_columns{element_filter_column};
    for (const auto & name : ActionsDAG::cloneSubDAG({filter_node}, false).getRequiredColumnsNames())
        element_filter_columns.insert(name);
    split->dag.removeUnusedActions(element_filter_columns);

    array_join->setElementFilter(std::move(split->dag), element_filter_column, split->remove_filter);

    /// the residual is an all-columns pass-through if the whole filter is now the element filter
    const auto & array_join_output = child_node->step->getOutputHeader();
    QueryPlanStepPtr residual_step;
    if (fully_fused)
        residual_step = std::make_unique<ExpressionStep>(array_join_output, std::move(residual));
    else
        residual_step = std::make_unique<FilterStep>(array_join_output, std::move(residual), filter->getFilterColumnName(), filter->removesFilterColumn());
    residual_step->setStepDescription(*filter);
    parent = std::move(residual_step);

    return 2;
}

}
