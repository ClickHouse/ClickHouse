#pragma once

#include <Common/Scheduler/Nodes/FifoQueue.h>
#include <Common/Scheduler/ResourceAllocation.h>

namespace DB
{

/// Queue used for holding pending and existing resource allocations.
class AllocationQueue : public FifoQueue
{
public:
    /// Enqueues a request to increase the size of an existing allocation.
    /// NOTE: To create a new allocation, use `enqueueRequest()` with a new `ResourceAllocation` instance.
    void enqueueIncreaseRequest(ResourceAllocation * allocation, ResourceCost size)
    {
        allocation->cost = size;
        enqueueRequest(allocation);
    }

    void getAllocationToReclaim(ResourceAllocation * allocation) override
    {
        // TODO(serxa): implement
    }
};

}
