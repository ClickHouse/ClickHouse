#pragma once

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/ResourceRequest.h>

#include <Common/EventRateMeter.h>
#include <Common/Stopwatch.h>

#include <algorithm>


namespace DB
{

/// Base class for all scheduler nodes that manage time-shared resource.
/// Time-shared resources process requests for resource consumption.
/// Dequeueing request from an inner node will dequeue request from one of active leaf-queues in its subtree.
/// Node is considered to be active iff:
///  - it has at least one pending request in one of leaves of it's subtree;
///  - and enforced constraints, if any, are satisfied
///    (e.g. amount of concurrent requests is not greater than some number).
class ITimeSharedNode : public ISchedulerNode
{
public:
    explicit ITimeSharedNode(EventQueue & event_queue_, const SchedulerNodeInfo & info_ = {})
        : ISchedulerNode(event_queue_, info_)
    {}

    /// Dequeue a request from this node or one of its children.
    /// Returns the first request to be executed as the first component of resulting pair.
    /// The second pair component is true if the node is still active after dequeueing.
    /// Note that due to cancelling even an active node may return `nullptr`.
    virtual std::pair<ResourceRequest *, bool> dequeueRequest() = 0;

    /// Returns true iff node is active
    virtual bool isActive() = 0;

    /// Returns number of active children (for introspection only).
    virtual size_t activeChildren() = 0;

    /// Activation of child due to the first pending request
    /// Recursively propagates activation signal through chain to the root.
    virtual void activateChild(ITimeSharedNode & child) = 0;

    /// Processes activation of this node
    void processActivation() override
    {
        // NOTE: Activations on detached subtrees are possible and should be processed normally until the detached root.
        // NOTE: This root can be then either reattached or simply discarded.
        if (parent)
            castParent().activateChild(*this);
    }

    ITimeSharedNode & castParent() const
    {
        return static_cast<ITimeSharedNode &>(*parent);
    }

    /// Helper for introspection metrics. `still_active` is whether the node is still active after dequeueing.
    /// The counters are written only by the scheduler thread, so a relaxed load and store is enough to update them.
    void incrementDequeued(ResourceCost cost, bool still_active)
    {
        chassert(event_queue.isInSchedulerOrStopped());
        dequeued_requests.store(dequeued_requests.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        dequeued_cost.store(dequeued_cost.load(std::memory_order_relaxed) + cost, std::memory_order_relaxed);
        pending_throughput_cost += cost;
        ++pending_throughput_requests;
        if (!still_active)
            flushThroughputOnDeactivation();
        else if (pending_throughput_requests >= throughput_batch_requests)
            flushThroughput(clock_gettime_ns());
    }

    /// Helper for introspection metrics. Should be called when the node becomes inactive without dequeueing a request:
    /// when `dequeueRequest` finds it inactive (e.g. its remaining requests were canceled) or when a node leaves its
    /// parent's active set by itself (e.g. a constraint whose limits were lowered).
    void flushThroughputOnDeactivation()
    {
        if (pending_throughput_requests > 0)
            flushThroughput(clock_gettime_ns());
        throughput_batch_requests = 1; /// The dequeue rate after reactivation is unknown
    }

    /// Average dequeued_cost per second
    /// WARNING: Should only be called from the scheduler thread, so that locking is not required
    double getThroughput()
    {
        UInt64 now_ns = clock_gettime_ns();
        flushThroughput(now_ns);
        return throughput.rate(static_cast<double>(now_ns) / 1e9);
    }

    /// Arbitrary data accessed/stored by parent (node-specific)
    union {
        size_t idx; // see FairPolicy
        void * ptr; // see TimeSharedScheduler
    } parent_data{};

    /// Introspection
    std::atomic<UInt64> dequeued_requests{0};
    std::atomic<UInt64> canceled_requests{0};
    std::atomic<UInt64> rejected_requests{0};
    std::atomic<ResourceCost> dequeued_cost{0};
    std::atomic<ResourceCost> canceled_cost{0};
    std::atomic<ResourceCost> rejected_cost{0};
    std::atomic<UInt64> busy_periods{0};

private:
    /// Dequeued requests are added to `throughput` in batches to keep clock reads and EWMA updates off the per-dequeue path.
    /// A batch is flushed when it is full, when the node deactivates and on introspection. The batch size spans about
    /// `throughput_batch_duration_ns` at the dequeue rate measured by the previous batch and restarts from one request
    /// after deactivation, so a slowly served node is still updated on every dequeue.
    void flushThroughput(UInt64 now_ns)
    {
        if (pending_throughput_requests == 0)
            return;
        throughput.add(static_cast<double>(now_ns) / 1e9, static_cast<double>(pending_throughput_cost));
        UInt64 elapsed_ns = now_ns - last_throughput_flush_ns;
        throughput_batch_requests = elapsed_ns == 0 ? max_throughput_batch_requests
            : std::clamp<UInt64>(pending_throughput_requests * throughput_batch_duration_ns / elapsed_ns, 1, max_throughput_batch_requests);
        last_throughput_flush_ns = now_ns;
        pending_throughput_cost = 0;
        pending_throughput_requests = 0;
    }

    static constexpr UInt64 max_throughput_batch_requests = 64;
    static constexpr UInt64 throughput_batch_duration_ns = 10'000'000;

    /// WARNING: Should only be accessed from the scheduler thread, so that locking is not required
    EventRateMeter throughput{static_cast<double>(clock_gettime_ns())/1e9, 2, 1};
    ResourceCost pending_throughput_cost = 0;
    UInt64 pending_throughput_requests = 0;
    UInt64 throughput_batch_requests = 1;
    UInt64 last_throughput_flush_ns = 0;
};

using TimeSharedNodePtr = std::shared_ptr<ITimeSharedNode>;

}
