#pragma once

#include <base/types.h>
#include <limits>

namespace DB
{

// Forward declarations
class ISchedulerQueue;
class ISchedulerNode;
class ISchedulerConstraint;

/// Cost in terms of used resource (e.g. bytes for network IO)
using ResourceCost = Int64;
constexpr ResourceCost ResourceCostMax = std::numeric_limits<int>::max();

/// Internal identifier of a resource (for arrays; unique per scheduler)
using ResourceIdx = size_t;
constexpr ResourceIdx ResourceIdxNotSet = ResourceIdx(-1);

/// Timestamps (nanoseconds since epoch)
using ResourceNs = UInt64;

/*
 * Info required for resource consumption.
 */
struct ResourceLink
{
    ISchedulerQueue * queue = nullptr;
    bool operator==(const ResourceLink &) const = default;
};

/*
 * Request for a resource consumption. The main moving part of the scheduling subsystem.
 * Resource requests processing workflow:
 *
 * ----1=2222222222222=3=4=555555555555555=6-----> time
 *     ^     ^         ^ ^          ^      ^
 *     |     |         | |          |      |
 *  enqueue wait dequeue execute consume finish
 *
 *  1) Request is enqueued using ISchedulerQueue::enqueueRequest().
 *  2) Request competes with others for access to a resource; effectively just waiting in a queue.
 *  3) Scheduler calls ISchedulerNode::dequeueRequest() that returns the request.
 *  4) Callback ResourceRequest::execute() is called to provide access to the resource.
 *  5) The resource consumption is happening outside of the scheduling subsystem.
 *  6) request->constraint->finishRequest() is called when consumption is finished.
 *
 * Steps (5) and (6) can be omitted if constraint is not used by the resource.
 *
 * Request can be created on stack or heap.
 * Request ownership is done outside of the scheduling subsystem.
 * After (6) request can be destructed safely.
 *
 * Request cancelling is not supported yet.
 */
class ResourceRequest
{
public:
    /// Cost of request execution; should be filled before request enqueueing.
    /// NOTE: If cost is not known in advance, credit model can be used:
    /// NOTE: for the first request use 1 and
    ResourceCost cost;

    /// Request outcome
    /// Should be filled during resource consumption
    bool successful = true;

    /// Scheduler node to be notified on consumption finish
    /// Auto-filled during request enqueue/dequeue
    ISchedulerConstraint * constraint = nullptr;

    /// Timestamps for introspection
    ResourceNs enqueue_ns = 0;
    ResourceNs execute_ns = 0;
    ResourceNs finish_ns = 0;

    explicit ResourceRequest(ResourceCost cost_ = 1)
        : cost(cost_)
    {}

    virtual ~ResourceRequest() = default;

    /// Callback to trigger resource consumption.
    /// IMPORTANT: is called from scheduler thread and must be fast,
    /// just triggering start of a consumption, not doing the consumption itself
    /// (e.g. setting an std::promise or creating a job in a thread pool)
    virtual void execute() = 0;
};

}
