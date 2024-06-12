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
    explicit operator bool() const { return queue != nullptr; }

    void adjust(ResourceCost estimated_cost, ResourceCost real_cost) const;
    void consumed(ResourceCost cost) const;
    void accumulate(ResourceCost cost) const;

    void reset()
    {
        queue = nullptr;
    }
};

/*
 * Everything required for IO scheduling.
 * Note that raw pointer are stored inside, so make sure that `ClassifierPtr` that produced
 * resource links will outlive them. Usually classifier is stored in query `Context`.
 */
struct IOSchedulingSettings
{
    ResourceLink read_resource_link;
    ResourceLink write_resource_link;

    bool operator==(const IOSchedulingSettings &) const = default;
    explicit operator bool() const { return read_resource_link && write_resource_link; }
};

}
