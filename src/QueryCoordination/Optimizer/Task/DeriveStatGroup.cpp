#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/Task/DeriveStatGroup.h>
#include <QueryCoordination/Optimizer/Task/DeriveStatNode.h>

namespace DB
{

void DeriveStatGroup::execute()
{
    bool all_derive_stat = true;
    for (auto & group_node : group.getGroupNodes())
        all_derive_stat &= group_node.isDeriveStat();

    if (all_derive_stat)
    {
        group.setDeriveStat();
        return;
    }

    pushTask(clone());

    for (auto & group_node : group.getGroupNodes())
    {
        auto d_node = std::make_unique<DeriveStatNode>(memo, stack, context, group, group_node);
        pushTask(std::move(d_node));
    }
}

std::unique_ptr<DeriveStatGroup> DeriveStatGroup::clone()
{
    return std::make_unique<DeriveStatGroup>(memo, stack, context, group);
}

}

