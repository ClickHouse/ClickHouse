#pragma once

#include <Common/Scheduler/ResourceRequest.h>

namespace DB
{

/// Represents a resource allocation (e.g. memory reservation for a query) that could change its size during its lifetime.
/// Both increase and decrease of size are done through interaction with scheduling nodes (see AllocationConstraint and AllocationQueue classes).
/// When resource is exhausted, scheduler may ask allocation to reclaim some of its size.
/// Base `ResourceRequest` is used as a request to increase size of allocation. Increasing size from zero is considered as allocation creation.
class ResourceAllocation : public ResourceRequest
{
public:
    ResourceCost allocated_size = 0;

    explicit ResourceAllocation(ResourceCost size)
        : ResourceRequest(size)
    {}

    /// Reclaim specified `size` of resource, should be called on
    /// IMPORTANT: it is called from the scheduler thread and must be fast,
    /// just triggering procedure, not doing the reclaim itself.
    virtual void reclaim(ResourceCost size) = 0;

    /// Decrease the size of allocation by `size` amount to non-zero size.
    /// NOTE: To completely release allocation `ResourceRequest::finish()` is used instead.
    void decrease(ResourceCost size);
};

}
