#include <IO/PrefetchThreadPool.h>
#include <IO/Rope.h>
#include <Common/logger_useful.h>

#include <stdexcept>

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

PrefetchThreadPool::PrefetchThreadPool(size_t pool_size, size_t queue_factor)
    : pool(
        CurrentMetrics::end(),
        CurrentMetrics::end(),
        CurrentMetrics::end(),
        pool_size,
        pool_size,
        pool_size * queue_factor)
{
}

std::unique_ptr<PrefetchHandle> PrefetchThreadPool::submit(std::function<Rope()> task)
{
    auto shared = std::make_shared<PrefetchHandle::SharedState>();
    auto future = shared->promise.get_future();

    /// Non-blocking schedule: return nullptr immediately if the queue is full.
    /// The caller treats that as "do it synchronously on next read".
    bool scheduled = pool.trySchedule([shared, t = std::move(task)]() mutable
    {
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
        return nullptr;

    return std::unique_ptr<PrefetchHandle>(new PrefetchHandle(std::move(shared), std::move(future)));
}

}
