#include <Interpreters/QuerySlot.h>

#include <Common/Scheduler/ISchedulerQueue.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

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
    enqueue();
    wait();
}

QuerySlot::QuerySlot(ResourceLink link_, ClassifierPtr classifier_, std::function<void()> on_ready_)
    : link(link_)
    , classifier(std::move(classifier_))
    , on_ready(std::move(on_ready_))
{
    enqueue();
}

QuerySlot::~QuerySlot()
{
    cancel();

    bool granted = false;
    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return state != State::Enqueued && !callback_running; });
        granted = state == State::Granted;
    }

    acquired_slot_increment.reset();
    if (granted)
        finish();
}

void QuerySlot::enqueue()
{
    chassert(link);
    enqueue_time = std::chrono::steady_clock::now();
    scheduled_increment.emplace(CurrentMetrics::ConcurrentQueryScheduled);
    try
    {
        link.queue->enqueueRequest(this);
    }
    catch (...)
    {
        scheduled_increment.reset();
        throw;
    }
}

void QuerySlot::wait()
{
    std::exception_ptr scheduler_exception;
    bool cancelled = false;
    bool acquired = false;
    bool account_wait = false;
    UInt64 wait_microseconds = 0;

    {
        std::unique_lock lock(mutex);
        cv.wait(lock, [this] { return state != State::Enqueued && !callback_running; });

        if (!wait_accounted)
        {
            wait_accounted = true;
            account_wait = true;
            wait_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - enqueue_time).count();
        }

        scheduler_exception = exception;
        cancelled = state == State::Cancelled;
        acquired = state == State::Granted;
    }

    if (account_wait)
    {
        ProfileEvents::increment(ProfileEvents::ConcurrentQueryWaitMicroseconds, wait_microseconds);
        if (acquired)
            ProfileEvents::increment(ProfileEvents::ConcurrentQuerySlotsAcquired);
    }

    if (scheduler_exception)
        throw Exception(
            ErrorCodes::RESOURCE_ACCESS_DENIED,
            "Unable to obtain a query slot: {}",
            getExceptionMessage(scheduler_exception, /* with_stacktrace = */ false));

    if (cancelled)
        throw Exception(ErrorCodes::RESOURCE_ACCESS_DENIED, "Query slot acquisition was cancelled");
}

bool QuerySlot::cancel()
{
    {
        std::lock_guard lock(mutex);
        if (state != State::Enqueued)
            return false;
    }

    if (!link.queue->cancelRequest(this))
        return false;

    {
        std::lock_guard lock(mutex);
        chassert(state == State::Enqueued);
        state = State::Cancelled;
        on_ready = {};
        scheduled_increment.reset();
        cv.notify_all();
    }
    return true;
}

void QuerySlot::execute()
{
    complete(State::Granted);
}

void QuerySlot::failed(const std::exception_ptr & ptr)
{
    complete(State::Failed, ptr);
}

void QuerySlot::complete(State new_state, const std::exception_ptr & ptr)
{
    std::function<void()> callback;
    {
        std::lock_guard lock(mutex);
        chassert(state == State::Enqueued);
        state = new_state;
        exception = ptr;
        scheduled_increment.reset();
        if (new_state == State::Granted)
            acquired_slot_increment.emplace(CurrentMetrics::ConcurrentQueryAcquired);
        callback = std::move(on_ready);
        callback_running = bool(callback);
        cv.notify_all();
    }

    try
    {
        if (callback)
            callback();
    }
    catch (...)
    {
        /// Resource scheduler threads have no exception boundary around ResourceRequest::execute().
        /// Preserve the error for wait() instead of letting a consumer wakeup failure terminate
        /// the scheduler thread.
        std::lock_guard lock(mutex);
        if (!exception)
            exception = std::current_exception();
        callback_running = false;
        cv.notify_all();
        return;
    }

    {
        std::lock_guard lock(mutex);
        callback_running = false;
        cv.notify_all();
    }
}

}
