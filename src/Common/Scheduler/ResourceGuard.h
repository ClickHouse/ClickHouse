#pragma once

#include <base/types.h>

#include <Common/Scheduler/ISchedulerConstraint.h>
#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Scheduler/ResourceRequest.h>
#include <Common/Scheduler/ResourceLink.h>

#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <condition_variable>
#include <exception>
#include <mutex>


namespace ProfileEvents
{
    extern const Event SchedulerIOReadRequests;
    extern const Event SchedulerIOReadBytes;
    extern const Event SchedulerIOReadWaitMicroseconds;
    extern const Event SchedulerIOWriteRequests;
    extern const Event SchedulerIOWriteBytes;
    extern const Event SchedulerIOWriteWaitMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric SchedulerIOReadScheduled;
    extern const Metric SchedulerIOWriteScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
}

/*
 * Scoped resource guard.
 * Waits for resource to be available in constructor and releases resource in destructor
 * IMPORTANT: multiple resources should not be locked concurrently by a single thread
 */
class ResourceGuard
{
public:
    enum class Lock
    {
        Default, /// Locks inside constructor

        // WARNING: Only for tests. It is not exception-safe because `lock()` must be called after construction.
        Defer /// Don't lock in constructor, but send request
    };

    struct Metrics
    {
        const ProfileEvents::Event requests = ProfileEvents::end();
        const ProfileEvents::Event cost = ProfileEvents::end();
        const ProfileEvents::Event wait_microseconds = ProfileEvents::end();
        const CurrentMetrics::Metric scheduled_count = CurrentMetrics::end();

        static const Metrics * getIORead()
        {
            static Metrics metrics{
                .requests = ProfileEvents::SchedulerIOReadRequests,
                .cost = ProfileEvents::SchedulerIOReadBytes,
                .wait_microseconds = ProfileEvents::SchedulerIOReadWaitMicroseconds,
                .scheduled_count = CurrentMetrics::SchedulerIOReadScheduled
            };
            return &metrics;
        }

        static const Metrics * getIOWrite()
        {
            static Metrics metrics{
                .requests = ProfileEvents::SchedulerIOWriteRequests,
                .cost = ProfileEvents::SchedulerIOWriteBytes,
                .wait_microseconds = ProfileEvents::SchedulerIOWriteWaitMicroseconds,
                .scheduled_count = CurrentMetrics::SchedulerIOWriteScheduled
            };
            return &metrics;
        }
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
            estimated_cost = link_.queue->enqueueRequestUsingBudget(this); // NOTE: it modifies `cost` and enqueues request
        }

        // This function is executed inside scheduler thread and wakes thread issued this `request`.
        // That thread will continue execution and do real consumption of requested resource synchronously.
        void execute() override
        {
            std::unique_lock lock(mutex);
            chassert(state == Enqueued);
            state = Dequeued;
            dequeued_cv.notify_one();
        }

        // This function is executed inside scheduler thread and wakes thread that issued this `request`.
        // That thread will throw an exception.
        void failed(const std::exception_ptr & ptr) override
        {
            std::unique_lock lock(mutex);
            chassert(state == Enqueued);
            state = Dequeued;
            exception = ptr;
            dequeued_cv.notify_one();
        }

        void wait()
        {
            CurrentMetrics::Increment scheduled(metrics->scheduled_count);
            auto timer = CurrentThread::getProfileEvents().timer(metrics->wait_microseconds);
            std::unique_lock lock(mutex);
            dequeued_cv.wait(lock, [this] { return state == Dequeued; });
            if (exception)
                throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
        }

        void finish(ResourceCost real_cost_, ResourceLink link_)
        {
            // lock(mutex) is not required because `Dequeued` request cannot be used by the scheduler thread
            chassert(state == Dequeued);
            state = Finished;
            if (estimated_cost != real_cost_)
                link_.queue->adjustBudget(estimated_cost, real_cost_);
            ResourceRequest::finish();
            ProfileEvents::increment(metrics->requests);
            ProfileEvents::increment(metrics->cost, real_cost_);
        }

        void assertFinished()
        {
            // lock(mutex) is not required because `Finished` request cannot be used by the scheduler thread
            chassert(state == Finished);
        }

        static Request & local(const Metrics * metrics)
        {
            // Since single thread cannot use more than one resource request simultaneously,
            // we can reuse thread-local request to avoid allocations
            static thread_local Request instance;
            instance.metrics = metrics;
            return instance;
        }

        const Metrics * metrics = nullptr; // Must be initialized before use

    private:
        ResourceCost estimated_cost = 0; // Stores initial `cost` value in case budget was used to modify it
        std::mutex mutex;
        std::condition_variable dequeued_cv;
        RequestState state = Finished;
        std::exception_ptr exception;
    };

    /// Creates pending request for resource; blocks while resource is not available (unless `Lock::Defer`)
    explicit ResourceGuard(const Metrics * metrics, ResourceLink link_, ResourceCost cost = 1, ResourceGuard::Lock type = ResourceGuard::Lock::Default)
        : link(link_)
        , request(Request::local(metrics))
    {
        if (cost == 0)
            link.reset(); // Ignore zero-cost requests
        else if (link)
        {
            request.enqueue(cost, link);
            if (type == Lock::Default)
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
        if (link)
            request.wait();
    }

    void consume(ResourceCost cost)
    {
        real_cost += cost;
    }

    /// Report resource consumption has finished
    void unlock(ResourceCost consumed = 0)
    {
        consume(consumed);
        if (link)
        {
            request.finish(real_cost, link);
            link.reset();
        }
    }

    ResourceLink link;
    Request & request;
    ResourceCost real_cost = 0;
};

}
