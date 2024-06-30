#pragma once

#include <base/types.h>

namespace DB
{
class ISchedulerQueue;
using ResourceCost = Int64;

/*
 * Everything required for resource consumption. Connection to a specific resource queue.
 */
struct ResourceLink
{
    ISchedulerQueue * queue = nullptr;
    bool operator==(const ResourceLink &) const = default;

    void adjust(ResourceCost estimated_cost, ResourceCost real_cost) const;

    void consumed(ResourceCost cost) const;

    void accumulate(ResourceCost cost) const;
};

}
