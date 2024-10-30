#pragma once

#include <Common/Scheduler/ISchedulerConstraint.h>

#include <chrono>
#include <utility>


namespace DB
{

/*
 * Limited throughput constraint. Blocks if token-bucket constraint is violated:
 * i.e. more than `max_burst + duration * max_speed` cost units (aka tokens) dequeued from this node in last `duration` seconds.
 */
class ThrottlerConstraint final : public ISchedulerConstraint
{
public:
    static constexpr double default_burst_seconds = 1.0;

    explicit ThrottlerConstraint(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerConstraint(event_queue_, config, config_prefix)
        , max_speed(config.getDouble(config_prefix + ".max_speed", 0))
        , max_burst(config.getDouble(config_prefix + ".max_burst", default_burst_seconds * max_speed))
        , last_update(event_queue_->now())
        , tokens(max_burst)
    {}

    ThrottlerConstraint(EventQueue * event_queue_, const SchedulerNodeInfo & info_, double max_speed_, double max_burst_)
        : ISchedulerConstraint(event_queue_, info_)
        , max_speed(max_speed_)
        , max_burst(max_burst_)
        , last_update(event_queue_->now())
        , tokens(max_burst)
    {}

    ~ThrottlerConstraint() override
    {
        // We should cancel event on destruction to avoid dangling references from event queue
        event_queue->cancelPostponed(postponed);

        // We need to clear `parent` in child to avoid dangling reference
        if (child)
            removeChild(child.get());
    }

    const String & getTypeName() const override
    {
        static String type_name("bandwidth_limit");
        return type_name;
    }

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * o = dynamic_cast<ThrottlerConstraint *>(other))
            return max_speed == o->max_speed && max_burst == o->max_burst;
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

        // We don't do `request->addConstraint(this)` because `finishRequest()` is no-op

        updateBucket(request->cost);

        child_active = child_now_active;
        if (!active())
            busy_periods++;
        incrementDequeued(request->cost);
        return {request, active()};
    }

    void finishRequest(ResourceRequest *) override
    {
        // NOTE: Token-bucket constraint does not require any action when consumption ends
    }

    void activateChild(ISchedulerNode * child_) override
    {
        if (child_ == child.get())
            if (!std::exchange(child_active, true) && satisfied() && parent)
                parent->activateChild(this);
    }

    /// Update limits.
    /// Should be called from the scheduler thread because it could lead to activation
    void updateConstraints(double new_max_speed, double new_max_burst)
    {
        event_queue->cancelPostponed(postponed);
        postponed = EventQueue::not_postponed;
        bool was_active = active();
        updateBucket(0, true); // To apply previous params for duration since `last_update`
        max_speed = new_max_speed;
        max_burst = new_max_burst;
        updateBucket(0, false); // To postpone (if needed) using new params
        if (!was_active && active() && parent)
            parent->activateChild(this);
    }

    bool isActive() override
    {
        return active();
    }

    size_t activeChildren() override
    {
        return child_active;
    }

    bool isSatisfied() override
    {
        return satisfied();
    }

    double getTokens() const
    {
        auto now = event_queue->now();
        double elapsed = std::chrono::nanoseconds(now - last_update).count() / 1e9;
        return std::min(tokens + max_speed * elapsed, max_burst);
    }

    std::chrono::nanoseconds getThrottlingDuration() const
    {
        return throttling_duration;
    }

    std::pair<double, double> getParams() const
    {
        return {max_speed, max_burst};
    }

private:
    void onPostponed()
    {
        postponed = EventQueue::not_postponed;
        bool was_active = active();
        updateBucket();
        if (!was_active && active() && parent)
            parent->activateChild(this);
    }

    void updateBucket(ResourceCost use = 0, bool do_not_postpone = false)
    {
        auto now = event_queue->now();
        if (max_speed > 0.0)
        {
            double elapsed = std::chrono::nanoseconds(now - last_update).count() / 1e9;
            tokens = std::min(tokens + max_speed * elapsed, max_burst);
            tokens -= use; // This is done outside min() to avoid passing large requests w/o token consumption after long idle period

            // Postpone activation until there is positive amount of tokens
            if (!do_not_postpone && tokens < 0.0)
            {
                auto delay_ns = std::chrono::nanoseconds(static_cast<Int64>(-tokens / max_speed * 1e9));
                if (postponed == EventQueue::not_postponed)
                {
                    postponed = event_queue->postpone(std::chrono::time_point_cast<EventQueue::Duration>(now + delay_ns),
                        [this] { onPostponed(); });
                    throttling_duration += delay_ns;
                }
            }
        }
        last_update = now;
    }

    bool satisfied() const
    {
        return tokens >= 0.0;
    }

    bool active() const
    {
        return satisfied() && child_active;
    }

    double max_speed{0}; /// in tokens per second
    double max_burst{0}; /// in tokens

    EventQueue::TimePoint last_update;
    UInt64 postponed = EventQueue::not_postponed;
    double tokens; /// in ResourceCost units
    bool child_active = false;

    std::chrono::nanoseconds throttling_duration{0};

    SchedulerNodePtr child;
};

}
