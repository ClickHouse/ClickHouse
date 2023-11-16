#include <Optimizer/Memo.h>
#include <Optimizer/Rule/Binder.h>
#include <Optimizer/Tasks/ApplyRule.h>
#include <Optimizer/Tasks/OptimizeInputs.h>
#include <Optimizer/Tasks/OptimizeNode.h>


namespace DB
{

ApplyRule::ApplyRule(GroupNodePtr group_node_, RulePtr rule_, TaskContextPtr task_context_)
    : OptimizeTask(task_context_), group_node(group_node_), rule(rule_)
{
}

void ApplyRule::execute()
{
    if (group_node->hasApplied(rule->getRuleId()))
        return;

    Binder binder(rule->getPattern(), group_node);
    const auto & bind_sub_plans = binder.bind();

    for (const auto & sub_plan : bind_sub_plans)
    {
        auto transformed_sub_plans = rule->transform(*sub_plan, getQueryContext());

        for (auto & transformed_sub_plan : transformed_sub_plans)
        {
            auto & group = task_context->getCurrentGroup();
            auto added_node = task_context->getMemo().addPlanNodeToGroup(*transformed_sub_plan.getRootNode(), &group);

            pushTask(std::make_unique<OptimizeInputs>(added_node, task_context));
            pushTask(std::make_unique<OptimizeNode>(added_node, task_context));
        }
    }
    group_node->setApplied(rule->getRuleId());
}

String ApplyRule::getDescription()
{
    return "ApplyRule (" + group_node->getStep()->getName() + "( " + group_node->getStep()->getStepDescription() + " )" + ")";
}

}
