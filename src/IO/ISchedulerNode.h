#pragma once

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/Priority.h>

#include <IO/ResourceRequest.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/XMLConfiguration.h>

#include <boost/noncopyable.hpp>

#include <deque>
#include <functional>
#include <memory>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

class ISchedulerNode;

inline const Poco::Util::AbstractConfiguration & emptyConfig()
{
    static Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration();
    return *config;
}

/*
 * Info read and write for scheduling purposes by parent
 */
struct SchedulerNodeInfo
{
    double weight = 1.0; /// Weight of this node among it's siblings
    Priority priority; /// Priority of this node among it's siblings (lower value means higher priority)

    /// Arbitrary data accessed/stored by parent
    union {
        size_t idx;
        void * ptr;
    } parent;

    SchedulerNodeInfo() = default;

    explicit SchedulerNodeInfo(const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
    {
        setWeight(config.getDouble(config_prefix + ".weight", weight));
        setPriority(config.getInt64(config_prefix + ".priority", priority));
    }

    void setWeight(double value)
    {
        if (value <= 0 || !isfinite(value))
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Negative and non-finite node weights are not allowed: {}",
                value);
        weight = value;
    }

    void setPriority(Int64 value)
    {
        priority.value = value;
    }
};

/*
 * Simple waitable thread-safe FIFO task queue.
 * Intended to hold postponed events for later handling (usually by scheduler thread).
 */
class EventQueue
{
public:
    using Event = std::function<void()>;

    void enqueue(Event&& event)
    {
        std::unique_lock lock{mutex};
        bool was_empty = queue.empty();
        queue.emplace_back(event);
        if (was_empty)
            pending.notify_one();
    }

    /// Process single event if it exists
    /// Returns `true` iff event has been processed
    bool tryProcess()
    {
        std::unique_lock lock{mutex};
        if (queue.empty())
            return false;
        Event event = std::move(queue.front());
        queue.pop_front();
        lock.unlock(); // do not hold queue mutext while processing events
        event();
        return true;
    }

    /// Wait for single event (if not available) and process it
    void process()
    {
        std::unique_lock lock{mutex};
        pending.wait(lock, [&] { return !queue.empty(); });
        Event event = std::move(queue.front());
        queue.pop_front();
        lock.unlock(); // do not hold queue mutext while processing events
        event();
    }

private:
    std::mutex mutex;
    std::condition_variable pending;
    std::deque<Event> queue;
};

/*
 * Node of hierarchy for scheduling requests for resource. Base class for all
 * kinds of scheduling elements (queues, policies, constraints and schedulers).
 *
 * Root node is a scheduler, which has it's thread to dequeue requests,
 * execute requests (see ResourceRequest) and process events in a thread-safe manner.
 * Immediate children of the scheduler represent independent resources.
 * Each resource has it's own hierarchy to achieve required scheduling policies.
 * Non-leaf nodes do not hold requests, but keep scheduling state
 * (e.g. consumption history, amount of in-flight requests, etc).
 * Leafs of hierarchy are queues capable of holding pending requests.
 *
 *        scheduler         (SchedulerRoot)
 *         /     \
 *  constraint  constraint  (SemaphoreConstraint)
 *      |           |
 *   policy      policy     (PriorityPolicy)
 *   /    \      /    \
 *  q1    q2    q3    q4    (FifoQueue)
 *
 * Dequeueing request from an inner node will dequeue request from one of active leaf-queues in its subtree.
 * Node is considered to be active iff:
 *  - it has at least one pending request in one of leaves of it's subtree;
 *  - and enforced constraints, if any, are satisfied
 *    (e.g. amount of concurrent requests is not greater than some number).
 *
 * All methods must be called only from scheduler thread for thread-safety.
 */
class ISchedulerNode : private boost::noncopyable
{
public:
    ISchedulerNode(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : event_queue(event_queue_)
        , info(config, config_prefix)
    {}

    virtual ~ISchedulerNode() {}

    // Checks if two nodes configuration is equal
    virtual bool equals(ISchedulerNode * other) = 0;

    /// Attach new child
    virtual void attachChild(const std::shared_ptr<ISchedulerNode> & child) = 0;

    /// Detach and destroy child
    virtual void removeChild(ISchedulerNode * child) = 0;

    /// Get attached child by name
    virtual ISchedulerNode * getChild(const String & child_name) = 0;

    /// Activation of child due to the first pending request
    /// Should be called on leaf node (i.e. queue) to propagate activation signal through chain to the root
    virtual void activateChild(ISchedulerNode * child) = 0;

    /// Returns true iff node is active
    virtual bool isActive() = 0;

    /// Returns the first request to be executed as the first component of resuting pair.
    /// The second pair component is `true` iff node is still active after dequeueing.
    virtual std::pair<ResourceRequest *, bool> dequeueRequest() = 0;

    /// Returns full path string using names of every parent
    String getPath()
    {
        String result;
        ISchedulerNode * ptr = this;
        while (ptr->parent)
        {
            result = "/" + ptr->basename + result;
            ptr = ptr->parent;
        }
        return result.empty() ? "/" : result;
    }

    /// Attach to a parent (used by attachChild)
    virtual void setParent(ISchedulerNode * parent_)
    {
        parent = parent_;
    }

protected:
    /// Notify parents about the first pending request or constraint becoming satisfied.
    /// Postponed to be handled in scheduler thread, so it is intended to be called from outside.
    void scheduleActivation()
    {
        if (likely(parent))
        {
            event_queue->enqueue([this] { parent->activateChild(this); });
        }
    }

public:
    EventQueue * const event_queue;
    String basename;
    SchedulerNodeInfo info;
    ISchedulerNode * parent = nullptr;
};

using SchedulerNodePtr = std::shared_ptr<ISchedulerNode>;

}
