#include <QueryCoordination/Optimizer/DeriveStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/Task/DeriveStatGroup.h>
#include <QueryCoordination/Optimizer/Task/DeriveStatNode.h>

namespace DB
{

void DeriveStatNode::execute()
{
    if (group_node.isDeriveStat())
        return;

    std::vector<Statistics> child_statistics;
    for (auto * child_group : group_node.getChildren())
    {
        if (!child_group->isDeriveStat())
        {
            pushTask(clone());

            auto derive_stat_child = std::make_unique<DeriveStatGroup>(memo, stack, context, *child_group);
            pushTask(std::move(derive_stat_child));
            return;
        }
        Statistics stat = child_group->getStatistics();
        child_statistics.emplace_back(stat);
    }

    DeriveStatistics visitor(child_statistics);
    Statistics stat = group_node.accept(visitor);

    group_node.setDeriveStat();

    group.setStatistics(stat);
}

}
