#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Joins.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

class JoinCommutativity : public IOptimizationRule
{
public:
    String getName() const override { return "JoinCommutativity"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 2000; }
    bool isTransformation() const override { return true; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const override;
};

bool JoinCommutativity::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * join_step = typeid_cast<JoinStepLogical*>(expression->getQueryPlanStep());
    if (!join_step)
        return false;

    const auto & join = join_step->getJoinOperator();

    return
        join.kind == JoinKind::Inner ||
        join.kind == JoinKind::Cross ||
        join.strictness == JoinStrictness::Semi ||
        join.strictness == JoinStrictness::Any ||
        join.strictness == JoinStrictness::Anti;
}

/// Make the same JOIN but with left and right inputs swapped
static std::unique_ptr<JoinStepLogical> cloneSwapped(const JoinStepLogical & join_step)
{
    auto swapped_join_step = std::unique_ptr<JoinStepLogical>(dynamic_cast<JoinStepLogical*>(join_step.clone().release()));
    swapped_join_step->swapInputs();
    return swapped_join_step;
}

std::vector<GroupExpressionPtr> JoinCommutativity::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    chassert(expression->inputs.size() == 2);
    const auto * join_step = typeid_cast<JoinStepLogical*>(expression->getQueryPlanStep());
    chassert(join_step);

    auto swapped_join_step = cloneSwapped(*join_step);
    swapped_join_step->setStepDescription(fmt::format("{} swapped", join_step->getStepDescription()), 200);

    GroupExpressionPtr expression_with_swapped_inputs = std::make_shared<GroupExpression>(nullptr);
    expression_with_swapped_inputs->plan_step = std::move(swapped_join_step);
    expression_with_swapped_inputs->inputs = {expression->inputs[1], expression->inputs[0]};
    expression_with_swapped_inputs->setApplied(*this, {});  /// Don't apply commutativity rule to the new expression
    memo.getGroup(expression->group_id)->addLogicalExpression(expression_with_swapped_inputs);

    return {expression_with_swapped_inputs};
}

OptimizationRulePtr createJoinCommutativity() { return std::make_shared<JoinCommutativity>(); }

}
