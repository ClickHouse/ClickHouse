#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB
{

GroupExpressionPtr IOptimizationRule::addTwoStageSplit(Memo & memo, const GroupExpressionPtr & source_expression,
    GroupExpressionPtr partial_expression, QueryPlanStepPtr final_step,
    ExpressionProperties final_input_required) const
{
    partial_expression->inputs = source_expression->inputs;
    /// The inputs are final here, so interning is sound: a second firing of the same split finds
    /// the partial group already in the memo, and the final expression below is then a duplicate in
    /// the source group and is dropped - with nothing left behind, because no group was created.
    /// Creating the partial group unconditionally instead would leak it exactly on that path; the
    /// leak was latent only because a fresh partial group id made the final expression unique, so
    /// the rejection below was unreachable.
    /// (A future eager-aggregation pushdown creates its partial group through here too.)
    GroupId partial_group_id = memo.internExpression(partial_expression);

    auto final_expression = std::make_shared<GroupExpression>(std::move(final_step));
    final_expression->inputs = {{partial_group_id, std::move(final_input_required)}};
    final_expression->setApplied(*this, {});
    /// Return nothing on a duplicate, so the dropped expression is not explored.
    if (!memo.addLogicalExpressionToGroup(source_expression->group_id, final_expression))
        return nullptr;
    return final_expression;
}

void IOptimizationRule::addPhysicalToMemo(GroupExpressionPtr expression, const ExpressionProperties & required_properties,
    Memo & memo, std::vector<GroupExpressionPtr> & result) const
{
    expression->setApplied(*this, required_properties);
    if (memo.getGroup(expression->group_id)->addPhysicalExpression(expression))
        result.push_back(expression);
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
