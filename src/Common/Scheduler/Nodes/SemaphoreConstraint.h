#pragma once

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
class SemaphoreConstraint : public ISchedulerConstraint
{
    static constexpr Int64 default_max_requests = std::numeric_limits<Int64>::max();
    static constexpr Int64 default_max_cost = std::numeric_limits<Int64>::max();
public:
    explicit SemaphoreConstraint(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerConstraint(event_queue_, config, config_prefix)
        , max_requests(config.getInt64(config_prefix + ".max_requests", default_max_requests))
        , max_cost(config.getInt64(config_prefix + ".max_cost", config.getInt64(config_prefix + ".max_bytes", default_max_cost)))
    {}

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

        // Request has reference to the first (closest to leaf) `constraint`, which can have `parent_constraint`.
        // The former is initialized here dynamically and the latter is initialized once during hierarchy construction.
        if (!request->constraint)
            request->constraint = this;

        // Update state on request arrival
        std::unique_lock lock(mutex);
        requests++;
        cost += request->cost;
        child_active = child_now_active;
        if (!active())
            busy_periods++;
        incrementDequeued(request->cost);
        return {request, active()};
    }

    void finishRequest(ResourceRequest * request) override
    {
        // Recursive traverse of parent flow controls in reverse order
        if (parent_constraint)
            parent_constraint->finishRequest(request);

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

    const Int64 max_requests = default_max_requests;
    const Int64 max_cost = default_max_cost;

    std::mutex mutex;
    Int64 requests = 0;
    Int64 cost = 0;
    bool child_active = false;

    SchedulerNodePtr child;
};

}
