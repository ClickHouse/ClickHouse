#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeGroup.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeInputs.h>
#include <QueryCoordination/Optimizer/Tasks/OptimizeNode.h>

namespace DB
{

OptimizeGroup::OptimizeGroup(TaskContextPtr task_context_) : OptimizeTask(task_context_)
{
}

void OptimizeGroup::execute()
{
    auto & group = task_context->getCurrentGroup();

    if (group.getSatisfyBestGroupNode(task_context->getRequiredProp()))
        return;

    for (auto & group_node : group.getGroupNodes())
        pushTask(std::make_unique<OptimizeInputs>(group_node, task_context));

    for (auto & group_node : group.getGroupNodes())
        pushTask(std::make_unique<OptimizeNode>(group_node, task_context));
}

String OptimizeGroup::getDescription()
{
    return "OptimizeGroup (" + std::to_string(task_context->getCurrentGroup().getId()) + ")";
}

}
