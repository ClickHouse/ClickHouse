#pragma once

#include <Common/Scheduler/ISchedulerConstraint.h>

namespace DB
{

/// Interface for allocation constraints that can be used to limit resource allocation size.
class IAllocationConstraint : public ISchedulerConstraint
{
public:
    /// Allocation is decreased by `size` amount.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual void decreaseAllocation(ResourceAllocation * allocation, ResourceCost size) = 0;
};

}
