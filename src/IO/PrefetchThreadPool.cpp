#include <IO/PrefetchThreadPool.h>
#include <IO/Rope.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Common/setThreadName.h>

#include <chrono>
#include <stdexcept>

namespace CurrentMetrics
{
    extern const Metric ReaderExecutorPrefetchInFlight;
}

namespace DB
{

bool PrefetchHandle::tryCancel()
{
    auto expected = State::Queued;
    return current_state.compare_exchange_strong(expected, State::Cancelled);
}

Rope PrefetchHandle::get()
{
    return future.get();
}

PrefetchHandle::State PrefetchHandle::state() const
{
    return current_state.load();
}

bool PrefetchHandle::isFinished() const noexcept
{
    return future.valid() && future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

PrefetchThreadPool::PrefetchThreadPool(size_t pool_size, size_t queue_size)
    : pool(
        CurrentMetrics::end(),
        CurrentMetrics::end(),
        CurrentMetrics::end(),
        pool_size,
        pool_size,
        queue_size ? queue_size : pool_size * 10)
{
}

PrefetchThreadPool::PrefetchThreadPool(NoWorkers)
    : pool(
        CurrentMetrics::end(),
        CurrentMetrics::end(),
        CurrentMetrics::end(),
        /*max_threads_=*/0,
        /*max_free_threads_=*/0,
        /*queue_size_=*/0)
{
}

std::shared_ptr<PrefetchHandle> PrefetchThreadPool::submit(std::function<Rope()> task)
{
    /// Allocate the handle once, up front. The worker captures a `shared_ptr`
    /// to this same object, so the result/state it sets is exactly what the
    /// caller reads - no second heap object. Allocating before `trySchedule`
    /// also means a bad_alloc here happens before anything is scheduled: there
    /// is no window where the task is live but the caller never received a
    /// handle to join or cancel it (which would run the worker against a
    /// caller that is being destroyed - a use-after-free).
    auto handle = std::make_shared<PrefetchHandle>(PrefetchHandle::ConstructTag{});

    /// Capture the submitter's ThreadGroup so the worker can attach to it.
    /// Without this, the worker has no `current_thread`, and downstream code
    /// (e.g. `ReadBufferFromS3::sendRequest` -> `ReadThrottlingScope`) silently
    /// skips per-user throttler attachment because `attachReadThrottler` bails
    /// on null `current_thread`. The query then bypasses
    /// `max_network_bandwidth_for_user` for every byte that flowed through
    /// the prefetch path.
    auto submitter_thread_group = getCurrentThreadGroup();

    /// Non-blocking schedule: return nullptr immediately if the queue is full.
    /// The caller treats that as "do it synchronously on next read".
    /// Bump `ReaderExecutorPrefetchInFlight` synchronously before schedule so
    /// it covers the queue-wait window as well; the worker decrements
    /// regardless of cancel/success outcome.
    CurrentMetrics::add(CurrentMetrics::ReaderExecutorPrefetchInFlight);
    bool scheduled = pool.trySchedule(
        [handle, t = std::move(task), thread_group = std::move(submitter_thread_group)]() mutable
    {
        SCOPE_EXIT({ CurrentMetrics::sub(CurrentMetrics::ReaderExecutorPrefetchInFlight); });
        ThreadGroupSwitcher switcher(thread_group, ThreadName::PREFETCH_READER);

        auto expected = PrefetchHandle::State::Queued;
        if (!handle->current_state.compare_exchange_strong(expected, PrefetchHandle::State::Running))
        {
            /// Cancelled before we picked it up — set an exception so anyone
            /// who (incorrectly) waits on the future gets a defined failure
            /// rather than a hang on a broken promise. Use std::runtime_error
            /// rather than DB::Exception(LOGICAL_ERROR, ...): in debug builds
            /// the latter aborts via ABORT_ON_LOGICAL_ERROR, and this code
            /// path is reachable in normal operation (not a logic bug).
            handle->promise.set_exception(std::make_exception_ptr(
                std::runtime_error("PrefetchHandle: task was cancelled")));
            return;
        }
        try
        {
            handle->promise.set_value(t());
        }
        catch (...)
        {
            handle->promise.set_exception(std::current_exception());
        }
        handle->current_state.store(PrefetchHandle::State::Done);
    });

    if (!scheduled)
    {
        /// trySchedule rejected the task — undo the gauge bump; the worker
        /// won't run and therefore won't decrement.
        CurrentMetrics::sub(CurrentMetrics::ReaderExecutorPrefetchInFlight);
        return nullptr;
    }

    return handle;
}

std::shared_ptr<PrefetchHandle> PrefetchThreadPool::makeCompletedHandleForTest(Rope rope)
{
    auto handle = std::make_shared<PrefetchHandle>(PrefetchHandle::ConstructTag{});
    handle->current_state.store(PrefetchHandle::State::Done);
    handle->promise.set_value(std::move(rope));
    return handle;
}

}
