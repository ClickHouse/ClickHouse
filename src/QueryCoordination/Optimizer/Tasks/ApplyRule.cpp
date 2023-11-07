#include <QueryCoordination/Optimizer/Memo.h>
#include <QueryCoordination/Optimizer/Tasks/ApplyRule.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeInputs.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeNode.h>
#include <QueryCoordination/Optimizer/Rule/Binder.h>


namespace DB
{

ApplyRule::ApplyRule(GroupNodePtr group_node_, RulePtr rule_, TaskContextPtr task_context_)
    : OptimizeTask(task_context_), group_node(group_node_), rule(rule_)
{
}

void ApplyRule::execute()
{
    Binder binder(rule->getPattern(), group_node);
    const auto & bind_step_trees = binder.bind();

    for (const auto & step_tree : bind_step_trees)
    {
        auto transformed_step_trees = rule->transform(*step_tree, getQueryContext());

        for (auto & transformed_step_tree : transformed_step_trees)
        {
            auto & group = task_context->getCurrentGroup();
            auto added_node = task_context->getMemo().addPlanNodeToGroup(*transformed_step_tree.getRootNode(), &group);

            pushTask(std::make_unique<OptimizeInputs>(added_node, task_context));
            pushTask(std::make_unique<OptimizeNode>(added_node, task_context));
        }
    }
}

String ApplyRule::getDescription()
{
    return "ApplyRule (" + group_node->getStep()->getName() + ")";
}

}
