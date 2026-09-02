#include <Common/Scheduler/ResourceGuard.h>
#include <Common/CurrentThread.h>

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

const ResourceGuard::Metrics * ResourceGuard::Metrics::getIORead()
{
    static Metrics metrics{
        .requests = ProfileEvents::SchedulerIOReadRequests,
        .cost = ProfileEvents::SchedulerIOReadBytes,
        .wait_microseconds = ProfileEvents::SchedulerIOReadWaitMicroseconds,
        .scheduled_count = CurrentMetrics::SchedulerIOReadScheduled
    };
    return &metrics;
}

const ResourceGuard::Metrics * ResourceGuard::Metrics::getIOWrite()
{
    static Metrics metrics{
        .requests = ProfileEvents::SchedulerIOWriteRequests,
        .cost = ProfileEvents::SchedulerIOWriteBytes,
        .wait_microseconds = ProfileEvents::SchedulerIOWriteWaitMicroseconds,
        .scheduled_count = CurrentMetrics::SchedulerIOWriteScheduled
    };
    return &metrics;
}

ResourceGuard::Request & ResourceGuard::Request::local(const Metrics * metrics)
{
    // Since single thread cannot use more than one resource request simultaneously,
    // we can reuse thread-local request to avoid allocations
    static thread_local Request instance;
    instance.metrics = metrics;
    return instance;
}

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
}

void ResourceGuard::Request::wait()
{
    CurrentMetrics::Increment scheduled(metrics->scheduled_count);
    auto timer = CurrentThread::getProfileEvents().timer(metrics->wait_microseconds);
    std::unique_lock lock(mutex);
    dequeued_cv.wait(lock, [this] { return state == Dequeued; });
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Resource request failed: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
}

}
