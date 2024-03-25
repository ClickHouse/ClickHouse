#pragma once

#include <base/types.h>

#include <Common/Scheduler/ISchedulerConstraint.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ResourceLink.h>

#include <condition_variable>
#include <mutex>


namespace DB
{

/*
 * Scoped resource guard.
 * Waits for resource to be available in constructor and releases resource in destructor
 * IMPORTANT: multiple resources should not be locked concurrently by a single thread
 */
class ResourceGuard
{
public:
    enum ResourceGuardCtor
    {
        LockStraightAway, /// Locks inside constructor (default)

        // WARNING: Only for tests. It is not exception-safe because `lock()` must be called after construction.
        PostponeLocking /// Don't lock in constructor, but send request
    };

    enum RequestState
    {
        Finished, // Last request has already finished; no concurrent access is possible
        Enqueued, // Enqueued into the scheduler; thread-safe access is required
        Dequeued  // Dequeued from the scheduler and is in consumption state; no concurrent access is possible
    };

    class Request : public ResourceRequest
    {
    public:
        void enqueue(ResourceCost cost_, ResourceLink link_)
        {
            // lock(mutex) is not required because `Finished` request cannot be used by the scheduler thread
            chassert(state == Finished);
            state = Enqueued;
            ResourceRequest::reset(cost_);
            link_.queue->enqueueRequestUsingBudget(this);
        }

        // This function is executed inside scheduler thread and wakes thread issued this `request`.
        // That thread will continue execution and do real consumption of requested resource synchronously.
        void execute() override
        {
            {
                std::unique_lock lock(mutex);
                chassert(state == Enqueued);
                state = Dequeued;
            }
            dequeued_cv.notify_one();
        }

        void wait()
        {
            std::unique_lock lock(mutex);
            dequeued_cv.wait(lock, [this] { return state == Dequeued; });
        }

        void finish()
        {
            // lock(mutex) is not required because `Dequeued` request cannot be used by the scheduler thread
            chassert(state == Dequeued);
            state = Finished;
            ResourceRequest::finish();
        }

        static Request & local()
        {
            // Since single thread cannot use more than one resource request simultaneously,
            // we can reuse thread-local request to avoid allocations
            static thread_local Request instance;
            return instance;
        }

    private:
        std::mutex mutex;
        std::condition_variable dequeued_cv;
        RequestState state = Finished;
    };

    /// Creates pending request for resource; blocks while resource is not available (unless `PostponeLocking`)
    explicit ResourceGuard(ResourceLink link_, ResourceCost cost = 1, ResourceGuardCtor ctor = LockStraightAway)
        : link(link_)
        , request(Request::local())
    {
        if (cost == 0)
            link.queue = nullptr; // Ignore zero-cost requests
        else if (link.queue)
        {
            request.enqueue(cost, link);
            if (ctor == LockStraightAway)
                request.wait();
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
            request.wait();
    }

    /// Report resource consumption has finished
    void unlock()
    {
        if (link.queue)
        {
            request.finish();
            link.queue = nullptr;
        }
    }

    ResourceLink link;
    Request & request;
};

}
