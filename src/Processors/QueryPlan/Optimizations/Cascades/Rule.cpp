#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB
{

bool isTopNSort(const IQueryPlanStep & step)
{
    const auto * sorting_step = typeid_cast<const SortingStep *>(&step);
    return sorting_step != nullptr && sorting_step->getType() == SortingStep::Type::Full && sorting_step->getLimit() > 0;
}

bool isDistributionPassthroughStep(const IQueryPlanStep & step)
{
    return typeid_cast<const ExpressionStep *>(&step) != nullptr
        || typeid_cast<const FilterStep *>(&step) != nullptr
        || typeid_cast<const BuildRuntimeFilterStep *>(&step) != nullptr;
}

GroupExpressionPtr makeEnforcerExpression(
    const GroupExpressionPtr & source,
    QueryPlanStepPtr step,
    ExpressionProperties input_required,
    ExpressionProperties output_properties,
    EnforcerAxis axis)
{
    auto enforcer_expression = std::make_shared<GroupExpression>(std::move(step));
    enforcer_expression->group_id = source->group_id;
    enforcer_expression->inputs.push_back({.group_id = source->group_id, .required_properties = std::move(input_required)});
    enforcer_expression->properties = std::move(output_properties);
    enforcer_expression->enforcer_axis = axis;
    return enforcer_expression;
}

GroupExpressionPtr IOptimizationRule::addTwoStageSplit(Memo & memo, const GroupExpressionPtr & source_expression,
    GroupExpressionPtr partial_expression, QueryPlanStepPtr final_step,
    ExpressionProperties final_input_required) const
{
    partial_expression->inputs = source_expression->inputs;
    GroupId partial_group_id = memo.addGroup(partial_expression);

    auto final_expression = std::make_shared<GroupExpression>(std::move(final_step));
    final_expression->inputs = {{partial_group_id, std::move(final_input_required)}};
    final_expression->setApplied(*this, {});
    memo.getGroup(source_expression->group_id)->addLogicalExpression(final_expression);
    return final_expression;
}

void IOptimizationRule::addPhysicalToMemo(GroupExpressionPtr expression, const ExpressionProperties & required_properties,
    Memo & memo, std::vector<GroupExpressionPtr> & result) const
{
    expression->setApplied(*this, required_properties);
    if (memo.getGroup(expression->group_id)->addPhysicalExpression(expression))
        result.push_back(expression);
}

std::vector<GroupExpressionPtr> IOptimizationRule::apply(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto new_expressions = applyImpl(expression, required_properties, memo);
    expression->setApplied(*this, required_properties);
    return new_expressions;
}

}
