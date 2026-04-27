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
}

QuerySlot::QuerySlot(ResourceLink link_)
    : link(link_)
{
    chassert(link);
    link.queue->enqueueRequest(this);
    CurrentMetrics::Increment scheduled(CurrentMetrics::ConcurrentQueryScheduled);
    auto timer = CurrentThread::getProfileEvents().timer(ProfileEvents::ConcurrentQueryWaitMicroseconds);
    std::unique_lock lock{mutex};
    cv.wait(lock, [this] { return granted || exception; }); // TODO(serxa): add query slot wait deadline w/ canceling
    if (exception)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Unable to obtain a query slot: {}", getExceptionMessage(exception, /* with_stacktrace = */ false));
    ProfileEvents::increment(ProfileEvents::ConcurrentQuerySlotsAcquired);
    acquired_slot_increment.emplace(CurrentMetrics::ConcurrentQueryAcquired);
}

QuerySlot::~QuerySlot()
{
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
