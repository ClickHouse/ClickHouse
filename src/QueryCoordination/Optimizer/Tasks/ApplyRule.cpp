#include <QueryCoordination/Optimizer/Memo.h>
#include <QueryCoordination/Optimizer/Tasks/ApplyRule.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeInputs.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeNode.h>
#include <QueryCoordination/Optimizer/Transform/Transformation.h>


namespace DB
{

ApplyRule::ApplyRule(GroupNode & group_node_, const Optimizer::Transformation & transform_rule_, TaskContextPtr task_context_)
    : OptimizeTask(task_context_), group_node(group_node_), transform_rule(transform_rule_)
{
}

void ApplyRule::execute()
{
    if (!transform_rule.apply)
        return;

    auto sub_query_plans = transform_rule.apply(group_node, getQueryContext());

    if (!sub_query_plans.empty())
    {
        for (auto & sub_query_plan : sub_query_plans)
        {
            auto & group = task_context->getCurrentGroup();
            auto & added_node = task_context->getMemo().addPlanNodeToGroup(sub_query_plan.getRoot(), group);

            pushTask(std::make_unique<OptimizeInputs>(added_node, task_context));
            pushTask(std::make_unique<OptimizeNode>(added_node, task_context));
        }
    }
}

String ApplyRule::getDescription()
{
    return "ApplyRule (" + group_node.getStep()->getName() + ")";
}

}
