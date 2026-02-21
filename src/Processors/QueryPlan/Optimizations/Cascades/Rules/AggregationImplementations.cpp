#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Common/typeid_cast.h>
#include <memory>

namespace DB
{

class LocalAggregationImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "LocalAggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 3000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const override;
};

class ShuffleAggregationImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "ShuffleAggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 4000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const override;
};

class PartialDistributedAggregationImplementation : public IOptimizationRule
{
public:
    String getName() const override { return "PartialDistributedAggregation"; }
    bool checkPattern(GroupExpressionPtr expression, const ExpressionProperties & required_properties, const Memo & memo) const override;
    Promise getPromise() const override { return 5000; }
    bool isTransformation() const override { return false; }

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, Memo & memo) const override;
};


bool LocalAggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<AggregatingStep *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:");
}

std::vector<GroupExpressionPtr> LocalAggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    auto new_aggregating_step = aggregating_step->clone();
    new_aggregating_step->setStepDescription(fmt::format("Local IMPL: {}", aggregating_step->getStepDescription()), 200);

    GroupExpressionPtr aggregation_expression = std::make_shared<GroupExpression>(*expression);
    aggregation_expression->plan_step = std::move(new_aggregating_step);
    chassert(aggregation_expression->inputs.size() == 1);
    memo.getGroup(expression->group_id)->addPhysicalExpression(aggregation_expression);
    aggregation_expression->setApplied(*this, required_properties);
    return {aggregation_expression};
}

OptimizationRulePtr createLocalAggregationImplementation() { return std::make_shared<LocalAggregationImplementation>(); }


bool ShuffleAggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    const auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    return aggregating_step != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:") &&
        aggregating_step->getParams().keys_size != 0;
}

std::vector<GroupExpressionPtr> ShuffleAggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    auto new_aggregating_step = aggregating_step->clone();
    new_aggregating_step->setStepDescription(fmt::format("Shuffle IMPL: {}", aggregating_step->getStepDescription()), 200);

    GroupExpressionPtr aggregation_expression = std::make_shared<GroupExpression>(*expression);
    aggregation_expression->plan_step = std::move(new_aggregating_step);
    chassert(aggregation_expression->inputs.size() == 1);
    memo.getGroup(expression->group_id)->addPhysicalExpression(aggregation_expression);
    aggregation_expression->setApplied(*this, required_properties);
    return {aggregation_expression};
}

OptimizationRulePtr createShuffleAggregationImplementation() { return std::make_shared<ShuffleAggregationImplementation>(); }


bool PartialDistributedAggregationImplementation::checkPattern(GroupExpressionPtr expression, const ExpressionProperties & /*required_properties*/, const Memo & /*memo*/) const
{
    return typeid_cast<AggregatingStep *>(expression->getQueryPlanStep()) != nullptr &&
        !expression->getQueryPlanStep()->getStepDescription().contains("IMPL:");
}

std::vector<GroupExpressionPtr> PartialDistributedAggregationImplementation::applyImpl(GroupExpressionPtr expression, const ExpressionProperties & required_properties, Memo & memo) const
{
    auto * aggregating_step = typeid_cast<AggregatingStep *>(expression->getQueryPlanStep());

    auto new_aggregating_step = aggregating_step->clone();
    new_aggregating_step->setStepDescription(fmt::format("PartialDistributed IMPL: {}", aggregating_step->getStepDescription()), 200);

    GroupExpressionPtr aggregation_expression = std::make_shared<GroupExpression>(*expression);
    aggregation_expression->plan_step = std::move(new_aggregating_step);
    chassert(aggregation_expression->inputs.size() == 1);

    /// For the input we require partitioned distribution in any way
    size_t node_count = 4;    /// TODO: get actual cluster topology and calculate number of nodes
    DistributionDescription partitioned_distribution;
    partitioned_distribution.node_count = node_count;
    partitioned_distribution.is_replicated = false;

    aggregation_expression->inputs[0].required_properties.distribution = partitioned_distribution;

    memo.getGroup(expression->group_id)->addPhysicalExpression(aggregation_expression);
    aggregation_expression->setApplied(*this, required_properties);
    return {aggregation_expression};
}

OptimizationRulePtr createPartialDistributedAggregationImplementation() { return std::make_shared<PartialDistributedAggregationImplementation>(); }

}
