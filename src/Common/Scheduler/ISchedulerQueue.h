#pragma once

#include <Common/Scheduler/ITimeSharedNode.h>
#include <Common/Scheduler/ResourceBudget.h>
#include <Common/Scheduler/ResourceRequest.h>


namespace DB
{

/*
 * Queue for pending requests for specific resource, leaf of hierarchy.
 * Note that every queue has budget associated with it.
 */
class ISchedulerQueue : public ITimeSharedNode
{
public:
    explicit ISchedulerQueue(EventQueue & event_queue_, const SchedulerNodeInfo & info_ = {})
        : ITimeSharedNode(event_queue_, info_)
    {}

    // Wrapper for `enqueueRequest()` that should be used to account for available resource budget
    // Returns `estimated_cost` that should be passed later to `adjustBudget()`
    [[ nodiscard ]] ResourceCost enqueueRequestUsingBudget(ResourceRequest * request)
    {
        ResourceCost estimated_cost = request->cost;
        request->cost = budget.ask(estimated_cost);
        try
        {
            enqueueRequest(request);
        }
        catch (...)
        {
            // The request did not enter the scheduler (e.g. the queue is full or is being destructed),
            // so the budget transaction made by `ask` (which assumes the request is granted `request->cost`
            // and consumes `estimated_cost`) must be rolled back, or the queue budget would diverge with
            // every failed enqueue and skew costs of subsequent requests.
            budget.adjust(estimated_cost, request->cost);
            request->cost = estimated_cost;
            throw;
        }
        return estimated_cost;
    }

    // Should be called to account for difference between real and estimated costs
    void adjustBudget(ResourceCost estimated_cost, ResourceCost real_cost)
    {
        budget.adjust(estimated_cost, real_cost);
    }

    /// Enqueue new request to be executed using underlying resource.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual void enqueueRequest(ResourceRequest * request) = 0;

    /// Cancel previously enqueued request.
    /// Returns `false` and does nothing given unknown or already executed request.
    /// Returns `true` if requests has been found and canceled.
    /// Should be called outside of scheduling subsystem, implementation must be thread-safe.
    virtual bool cancelRequest(ResourceRequest * request) = 0;

    /// Fails all the resource requests in queue and marks this queue as not usable.
    /// Afterwards any new request will be failed on `enqueueRequest()`.
    /// NOTE: This is done for queues that are about to be destructed.
    virtual void purgeQueue() = 0;

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
