#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <memory>

namespace DB
{

/// Moves the QueryPlan node to implementation as is
class DefaultImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "DefaultImplementation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const override;
};

bool DefaultImplementation::checkPattern(GroupExpressionPtr /*expression*/, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return true;
}

std::vector<GroupExpressionPtr> DefaultImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
    implementation_expression->plan_step->setStepDescription(fmt::format("IMPL: {}", expression->plan_step->getStepDescription()), 200);
    implementation_expression->setApplied(*this, required_properties);

    /// FIXME: pass required properties only for steps that do not affect them, e.g. Filter and Expression steps
    if (implementation_expression->inputs.size() == 1)
        implementation_expression->inputs[0].required_properties = required_properties;

    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

OptimizationRulePtr createDefaultImplementation() { return std::make_shared<DefaultImplementation>(); }

}
