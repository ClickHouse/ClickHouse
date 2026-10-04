#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPoolTaskTracker.h>

namespace ProfileEvents
{
extern const Event WriteBufferFromS3WaitInflightLimitMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_SCHEDULE_TASK;
}

struct TaskTracker::FinalTaskState
{
    std::promise<void> promise;
    TaskTracker::Callback callback;
    bool was_run = false;
    /// Set when the scheduler throws before enqueue; the destructor surfaces it instead of a synthesized error.
    std::exception_ptr schedule_error;

    explicit FinalTaskState(TaskTracker::Callback && callback_)
        : callback(std::move(callback_))
    {
    }

    void run()
    {
        was_run = true;
        try
        {
            {
                /// Destroy the callback's captures before satisfying promise.
                SCOPE_EXIT_SAFE({ [[maybe_unused]] auto released = std::move(callback); });
                callback();
            }
            promise.set_value();
        }
        catch (...)
        {
            promise.set_exception(std::current_exception());
        }
    }

    ~FinalTaskState()
    {
        if (was_run)
            return;

        /// The task never ran: the scheduler threw before enqueue, or the pool dropped the queued job
        /// while shutting down. Release the callback and satisfy the promise, same as run().
        {
            SCOPE_EXIT_SAFE({ [[maybe_unused]] auto released = std::move(callback); });
        }
        std::exception_ptr eptr = schedule_error;
        if (!eptr)
        {
            try
            {
                eptr = std::make_exception_ptr(
                    Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Final task was dropped before execution (thread pool is shutting down)"));
            }
            catch (...)
            {
                // make_exception_ptr may throw under memory pressure.
                eptr = std::current_exception();
            }
        }

        promise.set_exception(eptr);
    }
};

TaskTracker::TaskTracker(ThreadPoolCallbackRunnerUnsafe<void> scheduler_, size_t max_tasks_inflight_, LogSeriesLimiterPtr limited_log_)
    : is_async(bool(scheduler_))
    , scheduler(scheduler_ ? std::move(scheduler_) : syncRunner())
    , max_tasks_inflight(max_tasks_inflight_)
    , limited_log(limited_log_)
{
}

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
        return package.get_future();
    };
}

void TaskTracker::scheduleFinalTask(std::shared_ptr<FinalTaskState> state)
{
    try
    {
        scheduler([s = state]() mutable { s->run(); }, Priority{});
    }
    catch (...)
    {
        /// Scheduler threw before enqueue. Record the error and leave was_run false so ~FinalTaskState
        /// releases the callback and satisfies the promise (surfacing this error), like the drop case.
        state->schedule_error = std::current_exception();
    }
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
            }
            catch (...)
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
    {
        std::lock_guard lock(mutex);
        chassert(!final_task_added && "add must not be called after addFinal");
        ++tasks_added;
    }

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
    FinishedList pre_allocated_finished{future_placeholder};

    Callback func_with_notification
        = [this, my_func = std::move(func), my_pre_allocated_finished = std::move(pre_allocated_finished)]() mutable
    {
        SCOPE_EXIT({
            std::shared_ptr<FinalTaskState> maybe_final_task;
            {
                DENY_ALLOCATIONS_IN_SCOPE;
                std::lock_guard lock(mutex);
                finished_futures.splice(finished_futures.end(), my_pre_allocated_finished);
                ++tasks_finished;
                if (final_task && tasks_finished == tasks_added)
                {
                    maybe_final_task = std::move(final_task);
                }
                has_finished.notify_one();
            }
            if (maybe_final_task)
                scheduleFinalTask(std::move(maybe_final_task));
        });

        my_func();
    };

    /// this move is nothrow
    *future_placeholder = scheduler(std::move(func_with_notification), Priority{});

    waitTilInflightShrink();
}

void TaskTracker::addFinal(Callback && func)
{
    auto state = std::make_shared<FinalTaskState>(std::move(func));
    futures.emplace_back(state->promise.get_future());

    bool run_final_task_now = false;
    {
        std::lock_guard lock(mutex);
        chassert(!final_task_added && "addFinal must be called at most once");
        final_task_added = true;
        if (tasks_finished == tasks_added)
        {
            /// Every previously added task has already finished.
            /// There will be no SCOPE_EXIT to trigger the final callback, so run it here.
            run_final_task_now = true;
        }
        else
        {
            final_task = state;
        }
    }
    if (run_final_task_now)
        scheduleFinalTask(std::move(state));
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

        has_finished.wait(lock, [this]() TSA_REQUIRES(mutex) { return !finished_futures.empty(); });

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
