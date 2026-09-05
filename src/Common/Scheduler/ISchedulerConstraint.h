#pragma once

#include <Common/Scheduler/ISchedulerNode.h>

namespace DB
{

/*
 * Constraint defined on the set of requests in consumption state.
 * It allows to track two events:
 *  - dequeueRequest(): resource consumption begins
 *  - finishRequest(): resource consumption finishes
 * This allows to keep track of in-flight requests and implement different constraints (e.g. in-flight limit).
 * When constraint is violated, node must be deactivated by dequeueRequest() returning `false`.
 * When constraint is again satisfied, scheduleActivation() is called from finishRequest().
 *
 * Derived class behaviour requirements:
 *  - dequeueRequest() must call `request->addConstraint()`.
 */
class ISchedulerConstraint : public ISchedulerNode
{
public:
    explicit ISchedulerConstraint(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    ISchedulerConstraint(EventQueue * event_queue_, const SchedulerNodeInfo & info_)
        : ISchedulerNode(event_queue_, info_)
    {}

    /// Resource consumption by `request` is finished.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual void finishRequest(ResourceRequest * request) = 0;

    /// For introspection of current state (true = satisfied, false = violated)
    virtual bool isSatisfied() = 0;
};

}
