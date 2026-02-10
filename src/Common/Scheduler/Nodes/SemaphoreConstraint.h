#pragma once

#include "Common/Scheduler/ISchedulerNode.h"
#include <Common/Scheduler/ISchedulerConstraint.h>

#include <mutex>
#include <limits>
#include <utility>

namespace DB
{

/*
 * Limited concurrency constraint.
 * Blocks if either number of concurrent in-flight requests exceeds `max_requests`, or their total cost exceeds `max_cost`
 */
class SemaphoreConstraint final : public ISchedulerConstraint
{
    static constexpr Int64 default_max_requests = std::numeric_limits<Int64>::max();
    static constexpr Int64 default_max_cost = std::numeric_limits<Int64>::max();
public:
    explicit SemaphoreConstraint(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerConstraint(event_queue_, config, config_prefix)
        , max_requests(config.getInt64(config_prefix + ".max_requests", default_max_requests))
        , max_cost(config.getInt64(config_prefix + ".max_cost", config.getInt64(config_prefix + ".max_bytes", default_max_cost)))
    {}

    SemaphoreConstraint(EventQueue * event_queue_, const SchedulerNodeInfo & info_, Int64 max_requests_, Int64 max_cost_)
        : ISchedulerConstraint(event_queue_, info_)
        , max_requests(max_requests_)
        , max_cost(max_cost_)
    {}

    ~SemaphoreConstraint() override
    {
        // We need to clear `parent` in child to avoid dangling references
        if (child)
            removeChild(child.get());
    }

    const String & getTypeName() const override
    {
        static String type_name("inflight_limit");
        return type_name;
    }

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * o = dynamic_cast<SemaphoreConstraint *>(other))
            return max_requests == o->max_requests && max_cost == o->max_cost;
        return false;
    }

    void attachChild(const std::shared_ptr<ISchedulerNode> & child_) override
    {
        // Take ownership
        child = child_;
        child->setParent(this);

        // Activate if required
        if (child->isActive())
            activateChild(child.get());
    }

    void removeChild(ISchedulerNode * child_) override
    {
        if (child.get() == child_)
        {
            child_active = false; // deactivate
            child->setParent(nullptr); // detach
            child.reset();
        }
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (child->basename == child_name)
            return child.get();
        return nullptr;
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        // Dequeue request from the child
        auto [request, child_now_active] = child->dequeueRequest();
        if (!request)
            return {nullptr, false};

        std::unique_lock lock(mutex);
        if (request->addConstraint(this))
        {
            // Update state on request arrival
            requests++;
            cost += request->cost;
        }

        child_active = child_now_active;
        if (!active())
            busy_periods++;
        incrementDequeued(request->cost);
        return {request, active()};
    }

    void finishRequest(ResourceRequest * request) override
    {
        // Update state on request departure
        std::unique_lock lock(mutex);
        bool was_active = active();
        requests--;
        cost -= request->cost;

        // Schedule activation on transition from inactive state
        if (!was_active && active())
            scheduleActivation();
    }

    void activateChild(ISchedulerNode * child_) override
    {
        std::unique_lock lock(mutex);
        if (child_ == child.get())
            if (!std::exchange(child_active, true) && satisfied() && parent)
                parent->activateChild(this);
    }

    /// Update limits.
    /// Should be called from the scheduler thread because it could lead to activation or deactivation
    void updateConstraints(const SchedulerNodePtr & self, Int64 new_max_requests, UInt64 new_max_cost)
    {
        std::unique_lock lock(mutex);
        bool was_active = active();
        max_requests = new_max_requests;
        max_cost = new_max_cost;

        if (parent)
        {
            // Activate on transition from inactive state
            if (!was_active && active())
                parent->activateChild(this);
            // Deactivate on transition into inactive state
            else if (was_active && !active())
            {
                // Node deactivation is usually done in dequeueRequest(), but we do not want to
                // do extra call to active() on every request just to make sure there was no update().
                // There is no interface method to do deactivation, so we do the following trick.
                parent->removeChild(this);
                parent->attachChild(self); // This call is the only reason we have `recursive_mutex`
            }
        }
    }

    bool isActive() override
    {
        std::unique_lock lock(mutex);
        return active();
    }

    size_t activeChildren() override
    {
        std::unique_lock lock(mutex);
        return child_active;
    }

    bool isSatisfied() override
    {
        std::unique_lock lock(mutex);
        return satisfied();
    }

    std::pair<Int64, Int64> getInflights()
    {
        std::unique_lock lock(mutex);
        return {requests, cost};
    }

    std::pair<Int64, Int64> getLimits()
    {
        std::unique_lock lock(mutex);
        return {max_requests, max_cost};
    }

private:
    bool satisfied() const
    {
        return requests < max_requests && cost < max_cost;
    }

    bool active() const
    {
        return satisfied() && child_active;
    }

    Int64 max_requests = default_max_requests;
    Int64 max_cost = default_max_cost;

    std::recursive_mutex mutex;
    Int64 requests = 0;
    Int64 cost = 0;
    bool child_active = false;

    SchedulerNodePtr child;
};

}
