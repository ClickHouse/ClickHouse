#include <QueryCoordination/Optimizer/Tasks/ApplyRule.h>
#include <QueryCoordination/Optimizer/Tasks/DeriveStats.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeNode.h>
#include <QueryCoordination/Optimizer/Rule/RuleSet.h>

namespace DB
{

OptimizeNode::OptimizeNode(GroupNodePtr group_node_, TaskContextPtr task_context_) : OptimizeTask(task_context_), group_node(group_node_)
{
}

void OptimizeNode::execute()
{
    if (group_node->isEnforceNode())
        return;

    pushTask(std::make_unique<DeriveStats>(group_node, true, task_context));

    /// Apply all transformations.
    const auto & rules = Optimizer::getRules();

    for (const auto & rule : rules)
    {
        pushTask(std::make_unique<ApplyRule>(group_node, rule, task_context));
    }
}

String OptimizeNode::getDescription()
{
    return "OptimizeNode (" + group_node->getStep()->getName() + ")";
}


}
