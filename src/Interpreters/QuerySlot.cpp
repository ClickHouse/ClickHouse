#include <Interpreters/QuerySlot.h>

#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>

namespace ProfileEvents
{
    extern const Event ConcurrentQueryWaitMicroseconds;
    extern const Event ConcurrentQuerySlotsAcquired;
}

namespace CurrentMetrics
{
    extern const Metric ConcurrentQueryScheduled;
    extern const Metric ConcurrentQueryAcquired;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_ACCESS_DENIED;
    extern const int QUERY_SLOT_ACQUISITION_TIMEOUT;
}

QuerySlot::QuerySlot(ResourceLink link_, std::chrono::steady_clock::time_point admission_deadline_)
    : link(link_)
{
    chassert(link);
    link.queue->enqueueRequest(this);
    CurrentMetrics::Increment scheduled(CurrentMetrics::ConcurrentQueryScheduled);
    auto timer = CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrentQueryWaitMicroseconds);
    std::unique_lock lock{mutex};
    // An infinite deadline (`time_point::max()`) means no timeout: wait_until never fires on time and
    // blocks until the slot is granted or the request fails, exactly like an untimed wait.
    if (!cv.wait_until(lock, admission_deadline_, [this] { return granted || exception; }))
    {
        // Admission timed out: the request is still enqueued (neither granted nor failed). Cancel it so
        // the scheduler never hands a slot to an abandoned query. `cancelRequest` takes the scheduler
        // queue mutex, so it must be called without holding `mutex` (lock order: queue mutex -> this
        // mutex, see execute()/failed()).
        lock.unlock();
        if (link.queue->cancelRequest(this))
            throw Exception(ErrorCodes::QUERY_SLOT_ACQUISITION_TIMEOUT,
                "Timed out waiting to acquire a query slot for workload scheduling (exceeded workload_admission_timeout_ms)");

        // The scheduler dequeued the request between the timeout and the cancel attempt, so
        // execute()/failed() has run or will run. Wait for that definite outcome.
        lock.lock();
        cv.wait(lock, [this] { return granted || exception; });
    }
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Unable to obtain a query slot: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
    ProfileEvents::increment(ProfileEvents::ConcurrentQuerySlotsAcquired);
    acquired_slot_increment.emplace(CurrentMetrics::ConcurrentQueryAcquired);
}

QuerySlot::~QuerySlot()
{
    acquired_slot_increment.reset();
    if (granted)
        finish();
}

void QuerySlot::execute()
{
    std::scoped_lock lock{mutex};
    granted = true;
    cv.notify_one();
}

void QuerySlot::failed(const std::exception_ptr & ptr)
{
    std::scoped_lock lock{mutex};
    exception = ptr;
    cv.notify_one();
}

}
