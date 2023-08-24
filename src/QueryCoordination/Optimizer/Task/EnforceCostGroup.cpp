#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/Task/EnforceCostGroup.h>
#include <QueryCoordination/Optimizer/Task/EnforceCostNode.h>


namespace DB
{

void EnforceCostGroup::execute()
{
    for (auto & group_node : group.getGroupNodes())
    {
        auto enforce_cost_node = std::make_unique<EnforceCostNode>(memo, stack, context, group, group_node, required_prop);
        pushTask(std::move(enforce_cost_node));
    }
}

}
