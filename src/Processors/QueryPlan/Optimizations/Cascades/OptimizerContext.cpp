#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Common/logger_useful.h>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

OptimizerContext::OptimizerContext(IOptimizerStatistics & statistics)
    : cost_estimator(memo, statistics)
{
//    addRule(std::make_shared<JoinAssociativity>());
    addRule(std::make_shared<JoinCommutativity>());
    addRule(std::make_shared<HashJoinImplementation>());
    addRule(std::make_shared<ShuffleHashJoinImplementation>());
    addRule(std::make_shared<BroadcastJoinImplementation>());
    addRule(std::make_shared<DefaultImplementation>());
    addRule(std::make_shared<LocalAggregationImplementation>());
    addRule(std::make_shared<ShuffleAggregationImplementation>());
    addRule(std::make_shared<PartialDistributedAggregationImplementation>());
    addEnforcerRule(std::make_shared<DistributionEnforcer>());
    addEnforcerRule(std::make_shared<SortingEnforcer>());
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

GroupId OptimizerContext::addGroup(QueryPlan::Node & node)
{
    /// TODO: Currently CommonSubplanReferenceStep is expected to be resolved before Cascades optimizer.
    /// But it seem that we can resolve it here by just mapping the target Node to a corresponding Group.
    auto * subplan_reference = typeid_cast<CommonSubplanReferenceStep *>(node.step.get());
    if (subplan_reference)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected CommonSubplanReferenceStep, it should be already resolved");

    auto group_expression = std::make_shared<GroupExpression>(std::move(node.step));
    auto group_id = memo.addGroup(group_expression);
    for (auto * child_node : node.children)
    {
        auto input_group_id = addGroup(*child_node);
        group_expression->inputs.push_back({input_group_id, {}});
    }

    return group_id;
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
        group_id, expression->getDescription(), cost.subtree_cost);
    group->updateBestImplementation(expression);
}

}
