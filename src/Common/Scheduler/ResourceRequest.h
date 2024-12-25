#pragma once

#include <boost/intrusive/list.hpp>
#include <base/types.h>
#include <array>
#include <limits>
#include <exception>

namespace DB
{

// Forward declarations
class ISchedulerQueue;
class ISchedulerConstraint;

/// Cost in terms of used resource (e.g. bytes for network IO)
using ResourceCost = Int64;
constexpr ResourceCost ResourceCostMax = std::numeric_limits<int>::max();

/// Max number of constraints for a request to pass though (depth of constraints chain)
constexpr size_t ResourceMaxConstraints = 8;

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
 *  6) ResourceRequest::finish() is called when consumption is finished.
 *
 * Steps (5) and (6) can be omitted if constraint is not used by the resource.
 *
 * Request can be created on stack or heap.
 * Request ownership is done outside of the scheduling subsystem.
 * After (6) request can be destructed safely.
 *
 * Request can also be canceled before (3) using ISchedulerQueue::cancelRequest().
 * Returning false means it is too late for request to be canceled. It should be processed in a regular way.
 * Returning true means successful cancel and therefore steps (4) and (5) are not going to happen.
 */
class ResourceRequest : public boost::intrusive::list_base_hook<>
{
public:
    /// Cost of request execution; should be filled before request enqueueing and remain constant until `finish()`.
    /// NOTE: If cost is not known in advance, ResourceBudget should be used (note that every ISchedulerQueue has it)
    ResourceCost cost;

    /// Scheduler nodes to be notified on consumption finish
    /// Auto-filled during request dequeue
    /// Vector is not used to avoid allocations in the scheduler thread
    std::array<ISchedulerConstraint *, ResourceMaxConstraints> constraints;

    explicit ResourceRequest(ResourceCost cost_ = 1)
    {
        reset(cost_);
    }

    /// ResourceRequest object may be reused again after reset()
    void reset(ResourceCost cost_)
    {
        cost = cost_;
        for (auto & constraint : constraints)
            constraint = nullptr;
        // Note that list_base_hook should be reset independently (by intrusive list)
    }

    virtual ~ResourceRequest() = default;

    /// Callback to trigger resource consumption.
    /// IMPORTANT: it is called from scheduler thread and must be fast,
    /// just triggering start of a consumption, not doing the consumption itself
    /// (e.g. setting an std::promise or creating a job in a thread pool)
    virtual void execute() = 0;

    /// Callback to trigger an error in case if resource is unavailable.
    virtual void failed(const std::exception_ptr & ptr) = 0;

    /// Stop resource consumption and notify resource scheduler.
    /// Should be called when resource consumption is finished by consumer.
    /// ResourceRequest should not be destructed or reset before calling to `finish()`.
    /// It is okay to call finish() even for failed and canceled requests (it will be no-op)
    void finish();

    /// Is called from the scheduler thread to fill `constraints` chain
    /// Returns `true` iff constraint was added successfully
    bool addConstraint(ISchedulerConstraint * new_constraint);
};

}
