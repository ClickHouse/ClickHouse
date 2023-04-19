#pragma once

#include <base/types.h>

#include <IO/ResourceRequest.h>
#include <IO/ISchedulerQueue.h>
#include <IO/ISchedulerConstraint.h>

#include <future>

namespace DB
{

/*
 * Scoped resource guard.
 * Waits for resource to be available in constructor and releases resource in destructor
 */
class ResourceGuard
{
public:
    enum ResourceGuardCtor
    {
        LockStraightAway, /// Lock inside constructor (default)
        PostponeLocking /// Don't lock in constructor, but during later `lock()` call
    };

    struct Request : public ResourceRequest
    {
        /// Promise to be set on request execution
        std::promise<void> dequeued;

        explicit Request(ResourceCost cost_ = 1)
            : ResourceRequest(cost_)
        {}

        void execute() override
        {
            // This function is executed inside scheduler thread and wakes thread issued this `request` (using ResourceGuard)
            // That thread will continue execution and do real consumption of requested resource synchronously.
            dequeued.set_value();
        }
    };

    /// Creates pending request for resource; blocks while resource is not available (unless `PostponeLocking`)
    explicit ResourceGuard(ResourceLink link_, ResourceCost cost = 1, ResourceGuardCtor ctor = LockStraightAway)
        : link(link_)
        , request(cost)
    {
        if (link.queue)
        {
            dequeued_future = request.dequeued.get_future();
            link.queue->enqueueRequest(&request);
            if (ctor == LockStraightAway)
                lock();
        }
    }

    ~ResourceGuard()
    {
        unlock();
    }

    /// Blocks until resource is available
    void lock()
    {
        if (link.queue)
            dequeued_future.get();
    }

    /// Report request execution has finished
    void unlock()
    {
        if (link.queue)
        {
            assert(!dequeued_future.valid()); // unlock must be called only after lock()
            if (request.constraint)
                request.constraint->finishRequest(&request);
        }
    }

    /// Mark request as unsuccessful; by default request is considered to be successful
    void setFailure()
    {
        request.successful = false;
    }

public:
    ResourceLink link;
    Request request;
    std::future<void> dequeued_future;
};

}
