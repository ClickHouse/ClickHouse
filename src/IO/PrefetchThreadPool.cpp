#include <IO/PrefetchThreadPool.h>
#include <IO/Rope.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Common/setThreadName.h>

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
    return shared->state.compare_exchange_strong(expected, State::Cancelled);
}

Rope PrefetchHandle::get()
{
    return future.get();
}

PrefetchHandle::State PrefetchHandle::state() const
{
    return shared->state.load();
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

std::unique_ptr<PrefetchHandle> PrefetchThreadPool::submit(std::function<Rope()> task)
{
    auto shared = std::make_shared<PrefetchHandle::SharedState>();
    auto future = shared->promise.get_future();

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
        [shared, t = std::move(task), thread_group = std::move(submitter_thread_group)]() mutable
    {
        SCOPE_EXIT({ CurrentMetrics::sub(CurrentMetrics::ReaderExecutorPrefetchInFlight); });
        ThreadGroupSwitcher switcher(thread_group, ThreadName::PREFETCH_READER);

        auto expected = PrefetchHandle::State::Queued;
        if (!shared->state.compare_exchange_strong(expected, PrefetchHandle::State::Running))
        {
            /// Cancelled before we picked it up — set an exception so anyone
            /// who (incorrectly) waits on the future gets a defined failure
            /// rather than a hang on a broken promise. Use std::runtime_error
            /// rather than DB::Exception(LOGICAL_ERROR, ...): in debug builds
            /// the latter aborts via ABORT_ON_LOGICAL_ERROR, and this code
            /// path is reachable in normal operation (not a logic bug).
            shared->promise.set_exception(std::make_exception_ptr(
                std::runtime_error("PrefetchHandle: task was cancelled")));
            return;
        }
        try
        {
            shared->promise.set_value(t());
        }
        catch (...)
        {
            shared->promise.set_exception(std::current_exception());
        }
        shared->state.store(PrefetchHandle::State::Done);
    });

    if (!scheduled)
    {
        /// trySchedule rejected the task — undo the gauge bump; the worker
        /// won't run and therefore won't decrement.
        CurrentMetrics::sub(CurrentMetrics::ReaderExecutorPrefetchInFlight);
        return nullptr;
    }

    return std::unique_ptr<PrefetchHandle>(new PrefetchHandle(std::move(shared), std::move(future)));
}

}
