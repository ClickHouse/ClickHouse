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

bool JobHandle::tryCancel()
{
    auto expected = State::Queued;
    return current_state.compare_exchange_strong(expected, State::Cancelled);
}

void JobHandle::get()
{
    future.get();
}

JobHandle::State JobHandle::state() const
{
    return current_state.load();
}

bool JobHandle::isFinished() const noexcept
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
    /// Allocate before scheduling, sharing this one object with the worker: a
    /// throw here must precede a live task, else the task would run with no
    /// handle for the caller to join or cancel - a use-after-free.
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
            auto rope = t();
            /// Store Done before set_value: get() unblocks on set_value, so a
            /// later store would let get() return while state() still reads Running.
            handle->current_state.store(PrefetchHandle::State::Done);
            handle->promise.set_value(std::move(rope));
        }
        catch (...)
        {
            handle->current_state.store(PrefetchHandle::State::Done);
            handle->promise.set_exception(std::current_exception());
        }
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

std::shared_ptr<JobHandle> PrefetchThreadPool::submitJob(std::function<void()> task)
{
    /// Allocate before scheduling, sharing this one object with the worker: a
    /// throw here must precede a live task, else the task would run with no
    /// handle for the caller to join or cancel - a use-after-free.
    auto handle = std::make_shared<JobHandle>(JobHandle::ConstructTag{});

    /// Attach the worker to the submitter's ThreadGroup (see `submit`): the
    /// job's I/O and allocations attribute to the submitting query.
    auto submitter_thread_group = getCurrentThreadGroup();

    /// Non-blocking schedule: nullptr immediately if the queue is full. No
    /// in-flight gauge here - the pool cannot know what kind of work the job
    /// carries; a caller that needs one holds its own `CurrentMetrics::Increment`.
    bool scheduled = pool.trySchedule(
        [handle, t = std::move(task), thread_group = std::move(submitter_thread_group)]() mutable
    {
        ThreadGroupSwitcher switcher(thread_group, ThreadName::PREFETCH_READER);

        auto expected = JobHandle::State::Queued;
        if (!handle->current_state.compare_exchange_strong(expected, JobHandle::State::Running))
        {
            /// Cancelled before we picked it up — set an exception so anyone
            /// who (incorrectly) waits on the future gets a defined failure
            /// rather than a hang on a broken promise. std::runtime_error, not
            /// DB::Exception(LOGICAL_ERROR, ...): this path is reachable in
            /// normal operation and must not abort debug builds.
            handle->promise.set_exception(std::make_exception_ptr(
                std::runtime_error("JobHandle: task was cancelled")));
            return;
        }
        try
        {
            t();
            /// Store Done before set_value: get() unblocks on set_value, so a
            /// later store would let get() return while state() still reads Running.
            handle->current_state.store(JobHandle::State::Done);
            handle->promise.set_value();
        }
        catch (...)
        {
            handle->current_state.store(JobHandle::State::Done);
            handle->promise.set_exception(std::current_exception());
        }
    });

    if (!scheduled)
        return nullptr;

    return handle;
}

std::shared_ptr<PrefetchHandle> PrefetchThreadPool::makeCompletedHandleForTest(Rope rope)
{
    auto handle = std::make_shared<PrefetchHandle>(PrefetchHandle::ConstructTag{});
    handle->current_state.store(PrefetchHandle::State::Done);
    handle->promise.set_value(std::move(rope));
    return handle;
}

std::shared_ptr<JobHandle> PrefetchThreadPool::makeCompletedJobHandleForTest()
{
    auto handle = std::make_shared<JobHandle>(JobHandle::ConstructTag{});
    handle->current_state.store(JobHandle::State::Done);
    handle->promise.set_value();
    return handle;
}

}
