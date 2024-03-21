#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/Scheduler/ResourceRequest.h>

namespace DB
{
void ResourceLink::adjust(ResourceCost estimated_cost, ResourceCost real_cost) const
{
    if (queue)
        queue->adjustBudget(estimated_cost, real_cost);
}

void ResourceLink::consumed(ResourceCost cost) const
{
    if (queue)
        queue->consumeBudget(cost);
}

void ResourceLink::accumulate(DB::ResourceCost cost) const
{
    if (queue)
        queue->accumulateBudget(cost);
}
}

