#pragma once

#include <IO/ISchedulerNode.h>

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
 *  - dequeueRequest() must fill `request->constraint` iff it is nullptr;
 *  - finishRequest() must be recursive: call to `parent_constraint->finishRequest()`.
 */
class ISchedulerConstraint : public ISchedulerNode
{
public:
    ISchedulerConstraint(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    /// Resource consumption by `request` is finished.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual void finishRequest(ResourceRequest * request) = 0;

    void setParent(ISchedulerNode * parent_) override
    {
        ISchedulerNode::setParent(parent_);

        // Assign `parent_constraint` to the nearest parent derived from ISchedulerConstraint
        for (ISchedulerNode * node = parent_; node != nullptr; node = node->parent)
        {
            if (auto * constraint = dynamic_cast<ISchedulerConstraint *>(node))
            {
                parent_constraint = constraint;
                break;
            }
        }
    }

protected:
    // Reference to nearest parent that is also derived from ISchedulerConstraint.
    // Request can traverse through multiple constraints while being dequeue from hierarchy,
    // while finishing request should traverse the same chain in reverse order.
    // NOTE: it must be immutable after initialization, because it is accessed in not thread-safe way from finishRequest()
    ISchedulerConstraint * parent_constraint = nullptr;
};

}
