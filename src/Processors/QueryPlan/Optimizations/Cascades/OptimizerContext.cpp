#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/logger_useful.h>
#include <memory>

namespace DB
{

OptimizerContext::OptimizerContext(IOptimizerStatistics & statistics)
    : cost_estimator(memo, statistics)
{
//    addRule(std::make_shared<JoinAssociativity>());
    addRule(std::make_shared<JoinCommutativity>());
    addRule(std::make_shared<HashJoinImplementation>());
    addRule(std::make_shared<DefaultImplementation>());
}

void OptimizerContext::addRule(OptimizationRulePtr rule)
{
    if (rule->isTransformation())
        transformation_rules.push_back(std::move(rule));
    else
        implementation_rules.push_back(std::move(rule));
}

GroupId OptimizerContext::addGroup(QueryPlan::Node & node)
{
    auto group_expression = std::make_shared<GroupExpression>(&node);
    auto group_id = memo.addGroup(group_expression);
    for (auto * child_node : node.children)
    {
        auto input_group_id = addGroup(*child_node);
        group_expression->inputs.push_back(input_group_id);
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
    LOG_TRACE(log, "Group {} expression '{}' cost {}",
        group_id, expression->getDescription(), cost.subtree_cost);
    if (!group->best_implementation.expression || group->best_implementation.cost.subtree_cost > cost.subtree_cost)
    {
        group->best_implementation.expression = expression;
        group->best_implementation.cost = cost;
    }
}

}
