#include <IO/PrefetchThreadPool.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/setThreadName.h>

#include <chrono>

namespace DB
{

JobHandle::JobHandle(ConstructTag)
    : future(promise.get_future())
{
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

bool JobHandle::isFinished() const noexcept
{
    return future.valid() && future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

JobHandle::State JobHandle::state() const
{
    return current_state.load();
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

std::shared_ptr<JobHandle> PrefetchThreadPool::submitJob(std::function<void()> task)
{
    /// Allocate before scheduling: a throw here must precede a live task, else
    /// the task would run with no handle for the caller to join or cancel.
    auto handle = std::make_shared<JobHandle>(JobHandle::ConstructTag{});

    /// Carry the submitter's ThreadGroup to the worker: without it downstream
    /// per-user read throttling (`attachReadThrottler`) silently bails on null
    /// `current_thread` and the prefetch path bypasses
    /// `max_network_bandwidth_for_user`.
    auto submitter_thread_group = getCurrentThreadGroup();

    bool scheduled = pool.trySchedule(
        [handle, t = std::move(task), thread_group = std::move(submitter_thread_group)]() mutable
    {
        ThreadGroupSwitcher switcher(thread_group, ThreadName::PREFETCH_READER);

        auto expected = JobHandle::State::Queued;
        if (!handle->current_state.compare_exchange_strong(expected, JobHandle::State::Running))
        {
            /// Revoked before pickup (the revoker's CAS stored Cancelled).
            /// Resolve with a VALUE: cancellation is a correct outcome, not an
            /// error - a joiner unblocks cleanly and reads `state()`.
            handle->promise.set_value();
            return;
        }
        try
        {
            t();
            /// Store Done before set_value: get() unblocks on set_value, so a
            /// later store would let get() return while state() reads Running.
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

}
