#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <algorithm>

namespace DB
{

GroupExpressionPtr IOptimizationRule::addTwoStageSplit(Memo & memo, const GroupExpressionPtr & source_expression,
    GroupExpressionPtr partial_expression, QueryPlanStepPtr final_step,
    ExpressionProperties final_input_required) const
{
    partial_expression->inputs = source_expression->inputs;
    GroupId partial_group_id = memo.addGroup(partial_expression);

    auto final_expression = std::make_shared<GroupExpression>(std::move(final_step));
    final_expression->inputs = {{partial_group_id, std::move(final_input_required)}};
    final_expression->setApplied(*this, {});
    /// The partial group is new, so the final expression is a duplicate only when the same
    /// split was already registered. Return nothing then, so the dropped expression is not
    /// explored.
    if (!memo.getGroup(source_expression->group_id)->addLogicalExpression(final_expression))
        return nullptr;
    return final_expression;
}

void IOptimizationRule::addPhysicalToMemo(GroupExpressionPtr expression, const ExpressionProperties & required_properties,
    Memo & memo, std::vector<GroupExpressionPtr> & result) const
{
    expression->setApplied(*this, required_properties);
    auto stored_expression = memo.getGroup(expression->group_id)->addPhysicalExpression(expression);
    if (stored_expression == expression)
    {
        result.push_back(expression);
        return;
    }

    /// The group already has an equal expression. If it is an enforcer, it is now made for these
    /// required properties too.
    for (const auto & enforced_properties : expression->enforced_for)
    {
        auto & enforced_for = stored_expression->enforced_for;
        if (std::find(enforced_for.begin(), enforced_for.end(), enforced_properties) == enforced_for.end())
            enforced_for.push_back(enforced_properties);
    }
}

std::vector<GroupExpressionPtr> IOptimizationRule::addPhysicalToMemo(
    GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    std::vector<GroupExpressionPtr> result;
    addPhysicalToMemo(std::move(expression), required_properties, memo, result);
    return result;
}

std::vector<GroupExpressionPtr> IOptimizationRule::apply(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto new_expressions = applyImpl(expression, required_properties, memo);
    expression->setApplied(*this, required_properties);
    return new_expressions;
}

}
