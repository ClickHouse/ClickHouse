#include <memory>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include "Processors/QueryPlan/Optimizations/Cascades/Group.h"
#include "Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h"
#include "Processors/QueryPlan/Optimizations/Cascades/Rule.h"
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

OptimizerContext::OptimizerContext()
{
//    rules.push_back(std::make_shared<JoinAssociativity>());
    rules.push_back(std::make_shared<JoinCommutativity>());
}

GroupId OptimizerContext::addGroup(QueryPlan::Node & node)
{
    auto group_expression = std::make_shared<GroupExpression>(node);
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

void OptimizerContext::getBestPlan(GroupId group_id)
{
    memo.getGroup(group_id);
}

}
