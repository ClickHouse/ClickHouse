#include <Common/MemoryTracker.h>
#include <Common/ThreadPoolTaskTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <Common/scope_guard_safe.h>

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

/// Carries the addFinal callback together with an explicit promise. The future of this promise
/// is stored in `futures` and waited on by waitAll(). The task runs in exactly one place -- the
/// lambda handed to the scheduler -- via run(). Every other outcome must still satisfy the promise:
///   * the scheduler throws before enqueue (e.g. CANNOT_SCHEDULE_TASK from thread-fuzzer fault
///     injection): scheduleFinalTask satisfies the promise with the in-flight exception;
///   * the pool accepts the job, then drains it unrun while shutting down (ThreadPool::worker
///     resets the pending job via job_data.reset()): the captured shared_ptr drops, this object
///     is destroyed without run() having been called, and the destructor satisfies the promise.
/// A bare std::packaged_task could not cover the queued-drop case: destroying it unrun stores a
/// broken-promise std::future_error (a std::logic_error), which waitAll() then surfaces as a
/// LOGICAL_ERROR and aborts the server in debug/sanitizer builds. Satisfying the promise with a
/// normal DB::Exception instead lets the waiter observe an ordinary, catchable error.
/// Mirrors detail::CallbackRunnerTask in threadPoolCallbackRunner.h.
struct TaskTracker::FinalTaskState
{
    std::promise<void> promise;
    TaskTracker::Callback callback;
    bool was_run = false;

    explicit FinalTaskState(TaskTracker::Callback && callback_) : callback(std::move(callback_)) {}

    void run()
    {
        was_run = true;
        try
        {
            /// Release the callback (destroying its captures) BEFORE satisfying the promise:
            /// set_value can wake a waiter immediately, and a waiter that treats future.get() as
            /// "done" may tear down state the captures reference. Same ordering run() of
            /// detail::CallbackRunnerTask keeps. (No ThreadGroupSwitcher here: the scheduler wrapper
            /// -- threadPoolCallbackRunnerUnsafe -- already runs us inside the right thread group.)
            {
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

        /// The task was dropped without running (the pool was shut down while it was still queued,
        /// so ThreadPool::worker drains it via job_data.reset()). Mirror the run() path: release the
        /// callback (destroying its captures) before satisfying the promise, for the same
        /// waiter-vs-capture-teardown ordering reason. SCOPE_EXIT_SAFE keeps a throwing capture
        /// destructor from escaping this destructor.
        { SCOPE_EXIT_SAFE({ [[maybe_unused]] auto released = std::move(callback); }); }

        /// Satisfy the promise with a normal DB::Exception instead of letting ~promise() store a
        /// broken-promise std::future_error.
        /// Build the exception_ptr first: constructing the DB::Exception captures a stack trace and
        /// may itself throw (e.g. std::bad_alloc under shutdown memory pressure). If it does, fall
        /// back to the in-flight exception so the promise is NEVER left unset (an unset promise here
        /// recreates the very broken-promise std::future_error this object exists to prevent).
        std::exception_ptr eptr;
        try
        {
            eptr = std::make_exception_ptr(
                Exception(ErrorCodes::CANNOT_SCHEDULE_TASK, "Final task was dropped before execution (thread pool is shutting down)"));
        }
        catch (...)
        {
            eptr = std::current_exception();
        }

        try
        {
            promise.set_exception(eptr);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Ok: set_exception throws only if the promise is already satisfied, impossible here
            /// because run() (the only other writer) sets was_run first.
        }
    }
};

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

void TaskTracker::scheduleFinalTask(std::shared_ptr<FinalTaskState> state)
{
    /// `state`'s future is already in `futures` and will be waited on by waitAll().
    try
    {
        scheduler([s = state]() mutable { s->run(); }, Priority{});
    }
    catch (...)
    {
        /// Scheduling threw before the job was enqueued (e.g. CANNOT_SCHEDULE_TASK from
        /// thread-fuzzer fault injection). The task will not run, so satisfy its promise with the
        /// actual scheduling error -- waitAll() then surfaces CANNOT_SCHEDULE_TASK rather than a
        /// broken-promise std::future_error or a falsely-successful future. (Running the callback
        /// inline would make the future succeed and silently mask the real scheduling failure.)
        state->was_run = true;
        try
        {
            state->promise.set_exception(std::current_exception());
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
            /// Ok: set_exception throws only if the promise is already satisfied. It is not:
            /// run() has not been called (scheduling failed before enqueue) and was_run was just
            /// set so the destructor will not touch the promise either.
        }
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
    FinishedList pre_allocated_finished {future_placeholder};

    Callback func_with_notification = [this, my_func = std::move(func), my_pre_allocated_finished = std::move(pre_allocated_finished)]() mutable
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
