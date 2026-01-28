#pragma once

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/EventQueue.h>
#include <Common/Scheduler/ResourceRequest.h>

#include <Common/EventRateMeter.h>
#include <Common/Stopwatch.h>


namespace DB
{

/// Base class for all scheduler nodes that manage time-shared resource.
/// Time-shared resources process requests for resource consumption.
/// Dequeueing request from an inner node will dequeue request from one of active leaf-queues in its subtree.
/// Node is considered to be active iff:
///  - it has at least one pending request in one of leaves of it's subtree;
///  - and enforced constraints, if any, are satisfied
///    (e.g. amount of concurrent requests is not greater than some number).
class ITimeSharedNode: public ISchedulerNode
{
public:
    explicit ITimeSharedNode(EventQueue & event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    ITimeSharedNode(EventQueue & event_queue_, const SchedulerNodeInfo & info_)
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
        chassert(parent);
        castParent().activateChild(*this);
    }

    ITimeSharedNode & castParent() const
    {
        return static_cast<ITimeSharedNode &>(*parent);
    }

    /// Helper for introspection metrics
    void incrementDequeued(ResourceCost cost)
    {
        dequeued_requests++;
        dequeued_cost += cost;
        throughput.add(static_cast<double>(clock_gettime_ns())/1e9, static_cast<double>(cost));
    }

    /// Arbitrary data accessed/stored by parent (node-specific)
    union {
        size_t idx; // see FairPolicy
        void * ptr; // see TimeSharedScheduler
    } parent_data;

    /// Introspection
    std::atomic<UInt64> dequeued_requests{0};
    std::atomic<UInt64> canceled_requests{0};
    std::atomic<UInt64> rejected_requests{0};
    std::atomic<ResourceCost> dequeued_cost{0};
    std::atomic<ResourceCost> canceled_cost{0};
    std::atomic<ResourceCost> rejected_cost{0};
    std::atomic<UInt64> busy_periods{0};

    /// Average dequeued_cost per second
    /// WARNING: Should only be accessed from the scheduler thread, so that locking is not required
    EventRateMeter throughput{static_cast<double>(clock_gettime_ns())/1e9, 2, 1};
};

using TimeSharedNodePtr = std::shared_ptr<ITimeSharedNode>;

}
