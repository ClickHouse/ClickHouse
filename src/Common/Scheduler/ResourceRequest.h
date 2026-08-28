#pragma once

#include <Common/Scheduler/CostUnit.h>

#include <base/types.h>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <array>
#include <exception>
#include <utility>

namespace DB
{

// Forward declarations
class ISchedulerQueue;
class ISchedulerConstraint;
class FifoQueue;
class RequestQueue;
class FifoAlgorithm;
class FairAlgorithm;
class LasAlgorithm;
class PriorityAlgorithm;
class CPUSlotsAllocation;
class ResourceSchedulingContext;

/// Max number of constraints for a request to pass though (depth of constraints chain)
constexpr size_t ResourceMaxConstraints = 8;

/// Request to the resource scheduler. The main moving part of the scheduling for time-shared resources.
///
/// Requests processing workflow:
///
/// ----1=2222222222222=3=4=555555555555555=6-----> time
///     ^     ^         ^ ^          ^      ^
///     |     |         | |          |      |
///  enqueue wait dequeue execute consume finish
///
///  1) Request is enqueued using ISchedulerQueue::enqueueRequest().
///  2) Request competes with others for access to a resource; effectively just waiting in a queue.
///  3) Scheduler calls ITimeSharedNode::dequeueRequest() that returns the request.
///  4) Callback ResourceRequest::execute() is called to provide access to the resource.
///  5) The resource consumption is happening outside of the scheduling subsystem.
///  6) ResourceRequest::finish() is called when consumption is finished.
///
/// Steps (5) and (6) can be omitted if constraint is not used by the resource.
/// For example, memory reservations scheduler does not use constraints and instead checks limits during dequeueing.
///
/// Request can be created on stack or heap.
/// Request ownership is done outside of the scheduling subsystem.
/// After (6) request can be destructed safely.
///
/// Request can be canceled before (3) using ISchedulerQueue::cancelRequest().
/// Returning false means it is too late for request to be canceled. It should be processed in a regular way.
/// Returning true means successful cancel and therefore steps (4) and (5) are not going to happen.
class ResourceRequest
{
public:
    /// Cost of request execution; should be filled before request enqueueing and remain constant until `finish()`.
    /// NOTE: If cost is not known in advance, ResourceBudget should be used (note that every ISchedulerQueue has it)
    ResourceCost cost{};

    /// If true, request is not throttled by the scheduler
    /// This is used for special requests that should not be throttled, e.g. for CPUSlotsAllocation
    bool ignore_throttling = false;

    /// Non-owning link to the per-query scheduling context shared by all requests of one query.
    /// Used by the query-aware schedulers in `RequestQueue` (`fair`, `las`) to look up the query's
    /// weight, age and per-resource attained cost / virtual runtime. `nullptr` means "no query
    /// identity" (e.g. background operations); such requests are scheduled anonymously.
    /// The owning `shared_ptr` lives in the query context (`ThreadGroup`) and outlives every request
    /// the query enqueues (a request is enqueued only while the query runs), so this raw pointer
    /// never dangles while the request is in the queue.
    /// Must be set by the producer just before `enqueueRequest()` and is cleared by `reset()`.
    ResourceSchedulingContext * scheduling_context = nullptr;

    /// Ordering key for the query-aware schedulers (`fair`, `las`) in `RequestQueue`. Computed at
    /// enqueue and constant while the request is in the queue (required by the intrusive ordered
    /// set). `.first` is the virtual runtime (`fair`) or MLFQ level (`las`); `.second` a monotonic
    /// sequence number for a stable FIFO tie-break. Unused by the `fifo` scheduler.
    std::pair<double, UInt64> scheduling_key{0.0, 0};

    /// Scheduler nodes to be notified on consumption finish
    /// Auto-filled during request dequeue
    /// Vector is not used to avoid allocations in the scheduler thread
    /// NOTE: this is not used for allocations (see ResourceAllocation::parent instead)
    std::array<ISchedulerConstraint *, ResourceMaxConstraints> constraints{};

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
        // Clear per-request query identity and ordering key so a reused request (e.g. the
        // thread-local `ResourceGuard::Request`) never carries stale state from a previous query.
        scheduling_context = nullptr;
        scheduling_key = {0.0, 0};
        // Note that the intrusive hooks are reset independently (by their intrusive containers)
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

private:
    friend class FifoQueue;
    friend class FifoAlgorithm; // uses `enqueued_hook` for the `fifo` scheduler
    friend class FairAlgorithm; // uses `scheduling_hook` + `scheduling_key` for the `fair` scheduler
    friend class LasAlgorithm; // uses `scheduling_hook` + `scheduling_key` for the `las` scheduler
    friend class PriorityAlgorithm; // uses `scheduling_hook` + `scheduling_key` for the `priority` scheduler
    friend class RequestQueue;
    friend class CPUSlotsAllocation; // hack for tests only

    /// For an intrusive list of enqueued requests (FifoQueue and the `fifo` scheduler).
    /// NOTE: Can only be accessed under the owning queue's mutex.
    boost::intrusive::list_member_hook<> enqueued_hook;
    using EnqueuedHook = boost::intrusive::member_hook<ResourceRequest, boost::intrusive::list_member_hook<>, &ResourceRequest::enqueued_hook>;
    using EnqueuedList = boost::intrusive::list<ResourceRequest, EnqueuedHook>;

    /// For an intrusive ordered set of enqueued requests (the `fair` and `las` schedulers).
    /// A request is in at most one of the two containers (list or set) at a time.
    /// NOTE: Can only be accessed under the owning queue's mutex.
    boost::intrusive::set_member_hook<> scheduling_hook;
    using SchedulingHook = boost::intrusive::member_hook<ResourceRequest, boost::intrusive::set_member_hook<>, &ResourceRequest::scheduling_hook>;
};

}
