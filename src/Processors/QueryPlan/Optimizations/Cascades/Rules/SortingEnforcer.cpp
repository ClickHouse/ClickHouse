#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Core/SortDescription.h>
#include <memory>

namespace DB
{

class SortingEnforcer : public IOptimizationRule
{
public:
    String getName() const override { return "SortingEnforcer"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 1000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool SortingEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return !ExpressionProperties::isSortingSatisfiedBy(required_properties.sorting, expression->properties.sorting);
}

std::vector<GroupExpressionPtr> SortingEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);
    auto sorting_step = std::make_unique<SortingStep>(
        expression->getQueryPlanStep()->getOutputHeader(),
        required_properties.sorting,
        0,
        SortingStep::Settings(65000)    /// TODO: construct from settings
    );
    implementation_expression->property_enforcer_steps.push_back(std::move(sorting_step));
    implementation_expression->properties.sorting = required_properties.sorting;
    implementation_expression->inputs = expression->inputs;
    implementation_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

OptimizationRulePtr createSortingEnforcer() { return std::make_shared<SortingEnforcer>(); }

}
