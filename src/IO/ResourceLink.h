#pragma once

#include <base/types.h>

#include <IO/ResourceRequest.h>
#include <IO/ISchedulerQueue.h>


namespace DB
{

/*
 * Everything required for resource consumption. Connection to a specific resource queue.
 */
struct ResourceLink
{
    ISchedulerQueue * queue = nullptr;
    bool operator==(const ResourceLink &) const = default;

    void adjust(ResourceCost estimated_cost, ResourceCost real_cost) const
    {
        if (queue)
            queue->adjustBudget(estimated_cost, real_cost);
    }

    void consumed(ResourceCost cost) const
    {
        if (queue)
            queue->consumeBudget(cost);
    }

    void accumulate(ResourceCost cost) const
    {
        if (queue)
            queue->accumulateBudget(cost);
    }
};

}
