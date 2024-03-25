#pragma once

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/ResourceBudget.h>
#include <Common/Scheduler/ResourceRequest.h>

#include <memory>


namespace DB
{

/*
 * Queue for pending requests for specific resource, leaf of hierarchy.
 * Note that every queue has budget associated with it.
 */
class ISchedulerQueue : public ISchedulerNode
{
public:
    explicit ISchedulerQueue(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    // Wrapper for `enqueueRequest()` that should be used to account for available resource budget
    void enqueueRequestUsingBudget(ResourceRequest * request)
    {
        request->cost = budget.ask(request->cost);
        enqueueRequest(request);
    }

    // Should be called to account for difference between real and estimated costs
    void adjustBudget(ResourceCost estimated_cost, ResourceCost real_cost)
    {
        budget.adjust(estimated_cost, real_cost);
    }

    // Adjust budget to account for extra consumption of `cost` resource units
    void consumeBudget(ResourceCost cost)
    {
        adjustBudget(0, cost);
    }

    // Adjust budget to account for requested, but not consumed `cost` resource units
    void accumulateBudget(ResourceCost cost)
    {
        adjustBudget(cost, 0);
    }

    /// Enqueue new request to be executed using underlying resource.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual void enqueueRequest(ResourceRequest * request) = 0;

    /// Cancel previously enqueued request.
    /// Returns `false` and does nothing given unknown or already executed request.
    /// Returns `true` if requests has been found and canceled.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual bool cancelRequest(ResourceRequest * request) = 0;

    /// For introspection
    ResourceCost getBudget() const
    {
        return budget.get();
    }

private:
    // Allows multiple consumers to synchronize with common "debit/credit" balance.
    // 1) (positive) to avoid wasting of allocated but not used resource (e.g in case of a failure);
    // 2) (negative) to account for overconsumption (e.g. if cost is not know in advance and estimation from below is applied).
    ResourceBudget budget;
};

}
