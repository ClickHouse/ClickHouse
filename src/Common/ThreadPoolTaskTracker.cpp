#include "config.h"

#include "ThreadPoolTaskTracker.h"

namespace ProfileEvents
{
    extern const Event WriteBufferFromS3WaitInflightLimitMicroseconds;
}

namespace DB
{

TaskTracker::TaskTracker(ThreadPoolCallbackRunnerUnsafe<void> scheduler_, size_t max_tasks_inflight_, LogSeriesLimiterPtr limited_log_)
    : is_async(bool(scheduler_))
    , scheduler(scheduler_ ? std::move(scheduler_) : syncRunner())
    , max_tasks_inflight(max_tasks_inflight_)
    , limited_log(limited_log_)
{}

TaskTracker::~TaskTracker()
{
    /// Tasks should be waited outside of dtor.
    /// Important for WriteBufferFromS3/AzureBlobStorage, where TaskTracker is currently used.
    chassert(finished_futures.empty() && futures.empty());

    safeWaitAll();
}

ThreadPoolCallbackRunnerUnsafe<void> TaskTracker::syncRunner()
{
    return [](Callback && callback, int64_t) mutable -> std::future<void>
    {
        auto package = std::packaged_task<void()>(std::move(callback));
        /// No exceptions are propagated, exceptions are packed to future
        package();
        return  package.get_future();
    };
}

void TaskTracker::waitAll()
{
    /// Exceptions are propagated
    for (auto & future : futures)
    {
        future.get();
    }
    futures.clear();

    std::lock_guard lock(mutex);
    finished_futures.clear();
}

void TaskTracker::safeWaitAll()
{
    for (auto & future : futures)
    {
        if (future.valid())
        {
            try
            {
                /// Exceptions are not propagated
                future.get();
            } catch (...)
            {
                /// But at least they are printed
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
    futures.clear();

    std::lock_guard lock(mutex);
    finished_futures.clear();
}

void TaskTracker::waitIfAny()
{
    if (futures.empty())
        return;

    Stopwatch watch;

    {
        std::lock_guard lock(mutex);
        for (auto & it : finished_futures)
        {
            /// actually that call might lock this thread until the future is set finally
            /// however that won't lock us for long, the task is about to finish when the pointer appears in the `finished_futures`
            it->get();

            /// in case of exception in `it->get()`
            /// it it not necessary to remove `it` from list `futures`
            /// `TaskTracker` has to be destroyed after any exception occurs, for this `safeWaitAll` is called.
            /// `safeWaitAll` handles invalid futures in the list `futures`
            futures.erase(it);
        }
        finished_futures.clear();
    }

    watch.stop();
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WaitInflightLimitMicroseconds, watch.elapsedMicroseconds());
}

void TaskTracker::add(Callback && func)
{
    /// All this fuzz is about 2 things. This is the most critical place of TaskTracker.
    /// The first is not to fail insertion in the list `futures`.
    /// In order to face it, the element is allocated at the end of the list `futures` in advance.
    /// The second is not to fail the notification of the task.
    /// In order to face it, the list element, which would be inserted to the list `finished_futures`,
    /// is allocated in advance as an other list `pre_allocated_finished` with one element inside.

    /// preallocation for the first issue
    futures.emplace_back();
    auto future_placeholder = std::prev(futures.end());

    /// preallocation for the second issue
    FinishedList pre_allocated_finished {future_placeholder};

    Callback func_with_notification = [&, my_func = std::move(func), my_pre_allocated_finished = std::move(pre_allocated_finished)]() mutable
    {
        SCOPE_EXIT({
            DENY_ALLOCATIONS_IN_SCOPE;

            std::lock_guard lock(mutex);
            finished_futures.splice(finished_futures.end(), my_pre_allocated_finished);
            has_finished.notify_one();
        });

        my_func();
    };

    /// this move is nothrow
    *future_placeholder = scheduler(std::move(func_with_notification), Priority{});

    waitTilInflightShrink();
}

void TaskTracker::waitTilInflightShrink()
{
    if (!max_tasks_inflight)
        return;

    if (futures.size() >= max_tasks_inflight)
        LOG_TEST(limited_log, "have to wait some tasks finish, in queue {}, limit {}", futures.size(), max_tasks_inflight);

    Stopwatch watch;

    /// Alternative approach is to wait until at least futures.size() - max_tasks_inflight element are finished
    /// However the faster finished task is collected the faster CH checks if there is an exception
    /// The faster an exception is propagated the lesser time is spent for cancellation
    while (futures.size() >= max_tasks_inflight)
    {
        std::unique_lock lock(mutex);

        has_finished.wait(lock, [this] () TSA_REQUIRES(mutex) { return !finished_futures.empty(); });

        for (auto & it : finished_futures)
        {
            it->get();
            futures.erase(it);
        }

        finished_futures.clear();
    }

    watch.stop();
    ProfileEvents::increment(ProfileEvents::WriteBufferFromS3WaitInflightLimitMicroseconds, watch.elapsedMicroseconds());
}

bool TaskTracker::isAsync() const
{
    return is_async;
}

}
