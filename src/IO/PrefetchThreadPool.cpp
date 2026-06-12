#include <IO/PrefetchThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/setThreadName.h>

#include <chrono>
#include <stdexcept>

namespace DB
{

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

std::shared_ptr<JobHandle> PrefetchThreadPool::submitJob(std::function<void()> task)
{
    /// Allocate before scheduling, sharing this one object with the worker: a
    /// throw here must precede a live task, else the task would run with no
    /// handle for the caller to join or cancel - a use-after-free.
    auto handle = std::make_shared<JobHandle>(JobHandle::ConstructTag{});

    /// Capture the submitter's ThreadGroup so the worker can attach to it.
    /// Without this, the worker has no `current_thread`, and downstream code
    /// (e.g. `ReadBufferFromS3::sendRequest` -> `ReadThrottlingScope`) silently
    /// skips per-user throttler attachment because `attachReadThrottler` bails
    /// on null `current_thread`. The query then bypasses
    /// `max_network_bandwidth_for_user` for every byte that flowed through
    /// the prefetch path.
    auto submitter_thread_group = getCurrentThreadGroup();

    /// Non-blocking schedule: return nullptr immediately if the queue is full.
    bool scheduled = pool.trySchedule(
        [handle, t = std::move(task), thread_group = std::move(submitter_thread_group)]() mutable
    {
        ThreadGroupSwitcher switcher(thread_group, ThreadName::PREFETCH_READER);

        auto expected = JobHandle::State::Queued;
        if (!handle->current_state.compare_exchange_strong(expected, JobHandle::State::Running))
        {
            /// Revoked before we picked it up (state is already Cancelled,
            /// stored by the revoker's CAS). Resolve the promise with a VALUE:
            /// cancellation is a correct outcome, not an error - a joiner
            /// unblocks cleanly and reads `state()` for the verdict. No
            /// exception ever flows on this path.
            handle->promise.set_value();
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

std::shared_ptr<JobHandle> PrefetchThreadPool::makeCompletedJobHandleForTest()
{
    auto handle = std::make_shared<JobHandle>(JobHandle::ConstructTag{});
    handle->current_state.store(JobHandle::State::Done);
    handle->promise.set_value();
    return handle;
}

}
