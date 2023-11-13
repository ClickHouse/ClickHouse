#include <Common/logger_useful.h>
#include <QueryCoordination/Optimizer/Group.h>
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
        Statistics stats = child_group->getStatistics();
        child_statistics.emplace_back(stats);
    }

    DeriveStatistics visitor(child_statistics, getQueryContext());
    Statistics stats = group_node->accept(visitor);

    auto * log = &Poco::Logger::get(
        "DeriveStats group(" + std::to_string(task_context->getCurrentGroup().getId()) + ") group node(" + group_node->getStep()->getName() + ")");
    LOG_TRACE(log, "got {:.2g}", stats.getOutputRowSize());

    task_context->getCurrentGroup().setStatistics(stats);
    task_context->getCurrentGroup().setStatsDerived();
    group_node->setStatsDerived();
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
