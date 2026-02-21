#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/ShuffleExchangeStep.h>
#include <Processors/QueryPlan/BroadcastExchangeStep.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Common/Exception.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class DistributionEnforcer : public IOptimizationRule
{
public:
    String getName() const override { return "DistributionEnforcer"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 1000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const override;
};

bool DistributionEnforcer::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & /*memo*/) const
{
    return !ExpressionProperties::isDistributionSatisfiedBy(required_properties.distribution, expression->properties.distribution);
}

std::vector<GroupExpressionPtr> DistributionEnforcer::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto implementation_expression = std::make_shared<GroupExpression>(*expression);

    if (required_properties.distribution.columns.empty())
    {
        if (required_properties.distribution.is_replicated)
        {
            auto broadcast_exchange_step = std::make_unique<BroadcastExchangeStep>(
                expression->getQueryPlanStep()->getOutputHeader(),
                required_properties.distribution.node_count);
            implementation_expression->property_enforcer_steps.push_back(std::move(broadcast_exchange_step));
        }
        else if (required_properties.distribution.node_count == 1 && expression->properties.distribution.node_count > 1)
        {
            auto gather_exchange_step = std::make_unique<GatherExchangeStep>(
                expression->getQueryPlanStep()->getOutputHeader(),
                expression->properties.distribution.node_count);
            implementation_expression->property_enforcer_steps.push_back(std::move(gather_exchange_step));
        }
    }
    else
    {
        if (required_properties.distribution.is_replicated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot enforce replicated distribution with specific columns");

        Names shuffle_columns;
        for (const auto & distribution_column : required_properties.distribution.columns)
        {
            /// TODO: take the column that is present in the expression and is equivalent to required distribution column
            shuffle_columns.push_back(*distribution_column.begin());
        }
        auto shuffle_exchange_step = std::make_unique<ShuffleExchangeStep>(
            expression->getQueryPlanStep()->getOutputHeader(),
            std::move(shuffle_columns),
            required_properties.distribution.node_count,
            expression->properties.distribution.node_count);
        implementation_expression->property_enforcer_steps.push_back(std::move(shuffle_exchange_step));
    }
    implementation_expression->properties.distribution = required_properties.distribution;
    implementation_expression->inputs = expression->inputs;
    implementation_expression->setApplied(*this, required_properties);
    memo.getGroup(expression->group_id)->addPhysicalExpression(implementation_expression);
    return {implementation_expression};
}

OptimizationRulePtr createDistributionEnforcer() { return std::make_shared<DistributionEnforcer>(); }

}
