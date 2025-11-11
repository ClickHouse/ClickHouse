#pragma once

#include <Common/Scheduler/CostUnit.h>

#include <base/types.h>
#include <exception>

namespace DB
{

// Forward declarations
class ResourceAllocation;

/// Request to increase the resource allocation of a space-shared resource.
///
/// ----1=2222222222222=3----> time
///     ^       ^       ^
///  increase  wait  approve
///
/// 1) Request is reset and allocation is enqueued using IAllocationQueue::increaseAllocation().
/// 2) Allocation competes with others for resource; effectively just waiting in a queue.
/// 3) Whenever request is approved by the scheduler, IncreaseRequest::execute() is called.
///
/// Every ResourceAllocation may have zero or one pending IncreaseRequest.
/// From execute() allocation is allowed to call IAllocationQueue::increaseAllocation() again
/// to request further increase of allocation if necessary.
class IncreaseRequest
{
public:
    /// Allocation increase size.
    /// It must be greater than zero and remain constant until `execute()`.
    ResourceCost size;

    /// Allocation associated with this request.
    ResourceAllocation * const allocation = nullptr;

    /// When allocation is inserted into a queue, allocation should be increased from zero to its initial size.
    /// During this period allocation is pending (i.e. not yet running).
    bool pending_allocation = false;

    explicit IncreaseRequest(ResourceAllocation * allocation_, ResourceCost size_ = 1)
        : allocation(allocation_)
    {
        reset(size_);
    }

    /// The object may be reused again after reset().
    void reset(ResourceCost size_)
    {
        size = size_;
        pending_allocation = false;
    }

    virtual ~IncreaseRequest() = default;

    /// Callback to trigger on approval of the request.
    /// IMPORTANT: it is called from scheduler thread and must be fast.
    virtual void execute() = 0;

    /// Callback to trigger an error in case if resource is unavailable.
    virtual void failed(const std::exception_ptr & ptr) = 0;
};

}
