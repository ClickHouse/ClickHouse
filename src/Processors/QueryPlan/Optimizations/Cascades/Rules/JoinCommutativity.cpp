#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Core/Joins.h>
#include <Common/typeid_cast.h>
#include <Common/Exception.h>
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
    const auto * join_step = typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep());
    if (!join_step)
        return false;

    const auto & join = join_step->getJoinOperator();

    /// ASOF is not commutative: the closest matching value is resolved per left
    /// row, so swapping sides changes the result.
    if (join.strictness == JoinStrictness::Asof)
        return false;

    /// INNER is commutative only with strictness ALL: ANY ("take at most one match")
    /// and RightAny (deduplicate right keys) depend on which side is which, and
    /// swapInputs flips only the kind, never the strictness.
    if (join.kind == JoinKind::Inner)
        return join.strictness == JoinStrictness::All;

    /// `join_any_take_last_row` pins which matching row an ANY join keeps, and that row
    /// comes from the hash-table build side; a swap changes the build side and the result.
    /// The non-Cascades reordering suppresses the swap the same way.
    if (join.strictness == JoinStrictness::Any && join_step->getJoinSettings().join_any_take_last_row)
        return false;

    return
        join.kind == JoinKind::Cross ||
        join.strictness == JoinStrictness::Semi ||
        join.strictness == JoinStrictness::Any ||
        join.strictness == JoinStrictness::Anti;
}

static std::unique_ptr<JoinStepLogical> cloneSwapped(const JoinStepLogical & join_step)
{
    auto swapped_join_step = cloneStepAs(join_step);
    swapped_join_step->swapInputs();
    return swapped_join_step;
}

std::vector<GroupExpressionPtr> JoinCommutativity::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const
{
    chassert(expression->inputs.size() == 2);
    const auto * join_step = typeid_cast<const JoinStepLogical *>(expression->getQueryPlanStep());
    chassert(join_step);

    auto swapped_join_step = cloneSwapped(*join_step);
    swapped_join_step->setStepDescription(fmt::format("{} swapped", join_step->getStepDescription()), 200);

    GroupExpressionPtr expression_with_swapped_inputs = std::make_shared<GroupExpression>(std::move(swapped_join_step));
    expression_with_swapped_inputs->inputs = {expression->inputs[1], expression->inputs[0]};
    expression_with_swapped_inputs->setApplied(*this, {});  /// Mark the swapped join; otherwise the rule would keep swapping it back.
    if (!memo.getGroup(expression->group_id)->addLogicalExpression(expression_with_swapped_inputs))
        return {};

    return {expression_with_swapped_inputs};
}

OptimizationRulePtr createJoinCommutativity() { return std::make_shared<JoinCommutativity>(); }

}
