#pragma once

#include <base/types.h>

namespace DB
{

class ISchedulerQueue;
class IAllocationQueue;
using ResourceCost = Int64;

/*
 * Everything required for resource consumption. Connection to a specific resource queue.
 */
struct ResourceLink
{
    /// Queue to enqueue resource requests to. Only one of the two fields is set.
    ISchedulerQueue * queue = nullptr; // queue for time-shared resources (CPU, network, etc)
    IAllocationQueue * allocation_queue = nullptr; // queue for space-shared resources (memory, disk, etc)

    bool operator==(const ResourceLink &) const = default;

    explicit operator bool() const
    {
        return queue != nullptr || allocation_queue != nullptr;
    }

    void reset()
    {
        queue = nullptr;
        allocation_queue = nullptr;
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
