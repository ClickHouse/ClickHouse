#include <cmath>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>
#include <Common/logger_useful.h>
#include <memory>
#include <optional>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

OptimizationRulePtr createJoinCommutativity();
OptimizationRulePtr createHashJoinImplementation();
OptimizationRulePtr createAggregationImplementation();
OptimizationRulePtr createTwoPhaseAggregationTransformation();
OptimizationRulePtr createParallelReadImplementation();
OptimizationRulePtr createReplicatedReadImplementation();
OptimizationRulePtr createDefaultImplementation();
OptimizationRulePtr createDistributionPassthrough();
OptimizationRulePtr createDistributionEnforcer();
OptimizationRulePtr createSortingEnforcer();

OptimizerContext::OptimizerContext(IOptimizerStatistics & statistics, size_t cluster_node_count, CostConfig cost_config)
    : cost_estimator(memo)
    , statistics_derivation(memo, statistics)
{
    memo.setClusterNodeCount(cluster_node_count);
    memo.setCostConfig(cost_config);

    addRule(createJoinCommutativity());
    addRule(createHashJoinImplementation());
    addRule(createDefaultImplementation());
    addRule(createDistributionPassthrough());
    addRule(createTwoPhaseAggregationTransformation());
    addRule(createAggregationImplementation());
    addRule(createParallelReadImplementation());
    addRule(createReplicatedReadImplementation());
    addEnforcerRule(createDistributionEnforcer());
    addEnforcerRule(createSortingEnforcer());
}

void OptimizerContext::addRule(OptimizationRulePtr rule)
{
    if (rule->isTransformation())
        transformation_rules.push_back(std::move(rule));
    else
        implementation_rules.push_back(std::move(rule));
}

void OptimizerContext::addEnforcerRule(OptimizationRulePtr rule)
{
    enforcer_rules.push_back(std::move(rule));
}

std::pair<GroupId, ExpressionProperties> OptimizerContext::addGroup(QueryPlan::Node & node)
{
    /// TODO: Currently CommonSubplanReferenceStep is expected to be resolved before Cascades optimizer.
    /// But it seem that we can resolve it here by just mapping the target Node to a corresponding Group.
    auto * subplan_reference = typeid_cast<CommonSubplanReferenceStep *>(node.step.get());
    if (subplan_reference)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CommonSubplanReferenceStep, it should be already resolved");

    /// Strip SortingStep::Full — sorting is a physical property, not a logical one.
    /// Return the child's GroupId along with the sorting as required properties, so the caller
    /// can attach them to the input link of the parent group expression.
    auto * sorting_step = typeid_cast<SortingStep *>(node.step.get());
    if (sorting_step && sorting_step->getType() == SortingStep::Type::Full)
    {
        chassert(node.children.size() == 1);
        auto [child_group_id, _] = addGroup(*node.children.front());
        ExpressionProperties stripped_props;
        stripped_props.sorting = sorting_step->getSortDescription();
        stripped_props.sort_limit = sorting_step->getLimit();
        return {child_group_id, stripped_props};
    }

    std::optional<ExpressionStatistics> prepopulated_statistics = estimateStatistics(node);

    auto group_expression = std::make_shared<GroupExpression>(std::move(node.step));
    auto group_id = memo.addGroup(group_expression);
    for (auto * child_node : node.children)
    {
        auto [input_group_id, pending_props] = addGroup(*child_node);
        group_expression->inputs.push_back({input_group_id, pending_props});
    }

    /// Set statistics on the group (shared by all expressions in the group)
    auto group = memo.getGroup(group_id);
    group->statistics = std::move(prepopulated_statistics);

    return {group_id, {}};
}

void OptimizerContext::pushTask(OptimizationTaskPtr task)
{
    tasks.push(std::move(task));
}

GroupPtr OptimizerContext::getGroup(GroupId group_id)
{
    return memo.getGroup(group_id);
}

void OptimizerContext::updateBestPlan(GroupExpressionPtr expression)
{
    auto group_id = expression->group_id;
    auto group = memo.getGroup(group_id);
    auto cost = cost_estimator.estimateCost(expression);
    expression->cost = cost;
    LOG_TEST(log, "group #{} expression '{}' cost {}",
        group_id, expression->getDescription(), cost.subtree_cost.total(memo.getCostConfig()));
    group->updateBestImplementation(expression, memo.getCostConfig());
}

void OptimizerContext::deriveStatistics(GroupId group_id)
{
    statistics_derivation.deriveStatistics(group_id);
}

CostLimit OptimizerContext::computeChildCostLimit(
    GroupExpressionPtr parent_expression,
    size_t child_index,
    CostLimit parent_limit)
{
    if (!std::isfinite(parent_limit))
        return parent_limit;

    const auto & cost_config = memo.getCostConfig();
    CostLimit remaining = parent_limit;

    for (size_t i = 0; i < parent_expression->inputs.size(); ++i)
    {
        if (i == child_index)
            continue;

        const auto & input = parent_expression->inputs[i];
        auto group = getGroup(input.group_id);
        Float64 sibling_cost = group->getBestCostForProperties(input.required_properties, cost_config);
        if (std::isfinite(sibling_cost))
            remaining -= sibling_cost;
    }

    return remaining;
}

bool OptimizerContext::tryUpdateBestPlanDirectly(GroupExpressionPtr expression)
{
    const auto & cost_config = memo.getCostConfig();

    /// Check if all inputs are fully optimized (all stages complete).
    /// Using a weaker check (just having a best implementation) would risk computing
    /// costs with preliminary child bests that could later improve.
    for (const auto & input : expression->inputs)
    {
        auto child_group = getGroup(input.group_id);
        if (!child_group->isFullyDoneFor(input.required_properties))
            return false;
    }

    /// All inputs ready — compute cost directly, bypassing the OptimizeInputsTask chain.
    deriveStatistics(expression->group_id);

    auto group = getGroup(expression->group_id);
    auto cost = cost_estimator.estimateCost(expression);
    Float64 subtree_weighted = cost.subtree_cost.total(cost_config);

    Float64 current_best = group->getBestCostForProperties(expression->properties, cost_config);
    if (std::isfinite(current_best) && subtree_weighted >= current_best)
    {
        LOG_TEST(log, "Pruned expression '{}' in group #{}: "
            "cost {} >= current best {}",
            expression->getDescription(), expression->group_id,
            subtree_weighted, current_best);
        return true;
    }

    expression->cost = cost;
    LOG_TEST(log, "group #{} expression '{}' cost {}",
        expression->group_id, expression->getDescription(), cost.subtree_cost.total(cost_config));
    group->updateBestImplementation(expression, cost_config);
    return true;
}

}
