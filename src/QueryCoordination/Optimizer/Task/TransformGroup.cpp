#include <QueryCoordination/Optimizer/Task/TransformGroup.h>
#include <QueryCoordination/Optimizer/Task/TransformNode.h>
#include <QueryCoordination/Optimizer/Group.h>

namespace DB
{

void TransformGroup::execute()
{
    for (auto & group_node : group.getGroupNodes())
    {
        auto transform_node = std::make_unique<TransformNode>(memo, stack, context, group, group_node);
        pushTask(std::move(transform_node));
    }
}

}
