#include <memory>
#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/Tasks/DeriveStats.h>
#include "QueryCoordination/Optimizer/Statistics/DeriveStatistics.h"

namespace DB
{

DeriveStats::DeriveStats(GroupNodePtr group_node_, bool need_derive_child_, TaskContextPtr task_context_)
    : OptimizeTask(task_context_), group_node(group_node_), need_derive_child(need_derive_child_)
{
}


void DeriveStats::execute()
{
    if (group_node->hasStatsDerived())
        return;

    if (need_derive_child)
    {
        pushTask(clone(false));

        for (auto * child_group : group_node->getChildren())
        {
            PhysicalProperties any_prop;
            TaskContextPtr child_task_context = std::make_shared<TaskContext>(*child_group, any_prop, task_context->getOptimizeContext());
            for (auto & child_node : child_group->getGroupNodes())
            {
                if (child_node->hasStatsDerived() || child_node->isEnforceNode())
                    continue;

                pushTask(std::make_unique<DeriveStats>(child_node, true, child_task_context));
            }
        }
    }
    else
    {
        deriveStats();
    }
}

void DeriveStats::deriveStats()
{
    StatisticsList child_statistics;
    for (auto * child_group : group_node->getChildren())
    {
        Statistics stat = child_group->getStatistics();
        child_statistics.emplace_back(stat);
    }

    DeriveStatistics visitor(child_statistics, getQueryContext());
    Statistics stat = group_node->accept(visitor);

    group_node->setStatsDerived();

    /// TODO update group statistics
    task_context->getCurrentGroup().setStatistics(stat);
}

OptimizeTaskPtr DeriveStats::clone(bool need_derive_child_)
{
    return std::make_unique<DeriveStats>(group_node, need_derive_child_, task_context);
}

String DeriveStats::getDescription()
{
    return "DeriveStats (" + group_node->getStep()->getName() + (need_derive_child ? " with children)" : " without children)");
}

}
