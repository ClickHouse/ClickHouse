#pragma once

#include <Common/Scheduler/CostUnit.h>
#include <Common/Priority.h>

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
class RequestQueue;
class FifoAlgorithm;
class FairAlgorithm;
class LasAlgorithm;
class PriorityAlgorithm;
class CPUSlotsAllocation;
class ResourceSchedulingContext;
struct ResourceQueryState;

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

    /// Query-aware scheduling state (`fair` / `las` / `priority`), filled at enqueue by the
    /// `RequestQueue` schedulers and reset by `reset()`.
    struct
    {
        /// Per-query scheduling cost for the query's own virtual-runtime / attained-service
        /// accounting: the request's ORIGINAL declared cost, captured before any `ResourceBudget`
        /// adjustment. `ResourceBudget::ask()` rewrites `cost` queue-wide, which would let one query's
        /// misestimate bleed into another query's fairness state, so per-query scheduling keeps its
        /// own declared cost.
        ResourceCost cost{};

        /// Effective per-query charge `fair` applies to `vruntime` and `attained_cost`: the declared
        /// `scheduling.cost` adjusted by the query's accumulated real-vs-estimate correction, computed
        /// at enqueue by `FairAlgorithm::push` (`las` folds the correction in at pop). Defaults to
        /// `scheduling.cost` so it is never stale for schedulers that ignore it.
        ResourceCost charge{};

        /// Non-owning pointer to the query's scheduling context (query-global config: weight,
        /// priority, …), stamped onto the link by the classifier and copied here just before
        /// `enqueueRequest()` (cleared by `reset()`). The classifier owns it for the query's
        /// lifetime, so it never dangles while the request is queued.
        ResourceSchedulingContext * context = nullptr;

        /// Non-owning pointer to this query's per-resource state for the target leaf: the classifier
        /// resolved it (one slot per attached leaf) and stamped it onto the link; copied here at
        /// enqueue. Lets `fair`/`las` reach the per-resource state with a single dereference — no map
        /// or lookup. Same lifetime as `context`.
        ResourceQueryState * state = nullptr;

        /// Ordering key for `fair` / `las`, constant while the request is in the intrusive ordered
        /// set. `.first` is the virtual runtime (`fair`) or MLFQ level (`las`); `.second` a monotonic
        /// sequence number for a stable FIFO tie-break (also used by `priority`).
        std::pair<double, UInt64> key{0.0, 0};

        /// Primary ordering key for the `priority` scheduler (lower value first, then `key.second`).
        /// The `workload_priority` query setting (`Int64`, negatives allowed) is copied in at enqueue;
        /// an integer key avoids the precision loss of routing it through the `double` half of `key`.
        Priority priority;

        /// Set at enqueue iff the leaf's scheduler tracks per-query service (`fair`/`las`); gates the
        /// cost-correction feed in `ResourceGuard::finish()`, so a non-accounting leaf (`fifo`/
        /// `priority`) never accumulates a correction it would never drain.
        bool tracks_cost = false;
    } scheduling;

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
        // Capture the declared cost for per-query scheduling BEFORE any `ResourceBudget` adjustment
        // (which later rewrites `cost` only). For queues without a budget the two stay equal.
        scheduling.cost = cost_;
        scheduling.charge = cost_;
        for (auto & constraint : constraints)
            constraint = nullptr;
        // Clear per-request query identity and ordering key so a reused request (e.g. the
        // thread-local `ResourceGuard::Request`) never carries stale state from a previous query.
        scheduling.context = nullptr;
        scheduling.state = nullptr;
        scheduling.key = {0.0, 0};
        scheduling.priority = {};
        scheduling.tracks_cost = false;
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
    friend class FifoAlgorithm; // uses `enqueued_hook` for the `fifo` scheduler
    friend class FairAlgorithm; // uses `scheduling_hook` + `scheduling.key` for the `fair` scheduler
    friend class LasAlgorithm; // uses `scheduling_hook` + `scheduling.key` for the `las` scheduler
    friend class PriorityAlgorithm; // uses `scheduling_hook` + `scheduling.key` for the `priority` scheduler
    friend class RequestQueue;
    friend class CPUSlotsAllocation; // hack for tests only

    /// For an intrusive list of enqueued requests (the `fifo` scheduler).
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
