#pragma once

#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <exception>
#include <future>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

/// High-order function to run callbacks (functions with 'void()' signature) somewhere asynchronously.
template <typename Result, typename Callback = std::function<Result()>>
using ThreadPoolCallbackRunnerUnsafe = std::function<std::future<Result>(Callback &&, Priority)>;

/// NOTE When using ThreadPoolCallbackRunnerUnsafe you MUST ensure that all async tasks are finished
/// before any objects they may use are destroyed.
/// A common mistake is capturing some some local objects in lambda and passing it to the runner.
/// In case of exception, these local objects will be destroyed before scheduled tasks are finished.

/// Creates CallbackRunner that runs every callback with 'pool->scheduleOrThrowOnError()'.
template <typename Result, typename Callback = std::function<Result()>>
ThreadPoolCallbackRunnerUnsafe<Result, Callback> threadPoolCallbackRunnerUnsafe(ThreadPool & pool, const std::string & thread_name)
{
    return [my_pool = &pool, thread_group = CurrentThread::getGroup(), thread_name](Callback && callback, Priority priority) mutable -> std::future<Result>
    {
        auto task = std::make_shared<std::packaged_task<Result()>>([thread_group, thread_name, my_callback = std::move(callback)]() mutable -> Result
        {
            ThreadGroupSwitcher switcher(thread_group, thread_name.c_str());

            SCOPE_EXIT_SAFE(
            {
                /// Release all captured resources before detaching thread group
                /// Releasing has to use proper memory tracker which has been set here before callback

                [[maybe_unused]] auto tmp = std::move(my_callback);
            });

            return my_callback();
        });

        auto future = task->get_future();

        /// Note: calling method scheduleOrThrowOnError in intentional, because we don't want to throw exceptions
        /// in critical places where this callback runner is used (e.g. loading or deletion of parts)
        my_pool->scheduleOrThrowOnError([my_task = std::move(task)]{ (*my_task)(); }, priority);

        return future;
    };
}

template <typename Result, typename T>
std::future<Result> scheduleFromThreadPoolUnsafe(T && task, ThreadPool & pool, const std::string & thread_name, Priority priority = {})
{
    auto schedule = threadPoolCallbackRunnerUnsafe<Result, T>(pool, thread_name);
    return schedule(std::move(task), priority); /// NOLINT
}

/// NOTE It's still not completely safe.
/// When creating a runner on stack, you MUST make sure that it's created (and destroyed) before local objects captured by task lambda.

template <typename Result, typename PoolT = ThreadPool, typename Callback = std::function<Result()>>
class ThreadPoolCallbackRunnerLocal final
{
    static_assert(!std::is_same_v<PoolT, GlobalThreadPool>, "Scheduling tasks directly on GlobalThreadPool is not allowed because it doesn't set up CurrentThread. Create a new ThreadPool (local or in SharedThreadPools.h) or use ThreadFromGlobalPool.");

    PoolT & pool;
    std::string thread_name;

    enum TaskState
    {
        SCHEDULED = 0,
        RUNNING = 1,
        FINISHED = 2,
        CANCELLED = 3,
    };

    struct Task
    {
        std::future<Result> future;
        std::atomic<TaskState> state = SCHEDULED;
    };

    /// NOTE It will leak for a global object with long lifetime
    std::vector<std::shared_ptr<Task>> tasks;

    void cancelScheduledTasks()
    {
        for (auto & task : tasks)
        {
            TaskState expected = SCHEDULED;
            task->state.compare_exchange_strong(expected, CANCELLED);
        }
    }

    /// Set promise result for non-void callbacks
    template <typename Function, typename FunctionResult>
    static void executeCallback(std::promise<FunctionResult> & promise, Function && callback, ThreadGroupPtr thread_group, const std::string & thread_name)
    {
        /// Release callback before setting value to the promise to avoid
        /// destruction of captured resources after waitForAllToFinish returns.
        try
        {
            FunctionResult res;
            {
                ThreadGroupSwitcher switcher(thread_group, thread_name.c_str());
                res = callback();
                callback = {};
            }
            promise.set_value(std::move(res));
        }
        catch (...)
        {
            callback = {};
            promise.set_exception(std::current_exception());
        }
    }

    /// Set promise result for void callbacks
    template <typename Function>
    static void executeCallback(std::promise<void> & promise, Function && callback, ThreadGroupPtr thread_group, const std::string & thread_name)
    {
        /// Release callback before setting value to the promise to avoid
        /// destruction of captured resources after waitForAllToFinish returns.
        try
        {
            {
                ThreadGroupSwitcher switcher(thread_group, thread_name.c_str());
                callback();
                callback = {};
            }
            promise.set_value();
        }
        catch (...)
        {
            callback = {};
            promise.set_exception(std::current_exception());
        }
    }

public:
    ThreadPoolCallbackRunnerLocal(PoolT & pool_, const std::string & thread_name_)
        : pool(pool_)
        , thread_name(thread_name_)
    {
    }

    ~ThreadPoolCallbackRunnerLocal()
    {
        cancelScheduledTasks();
        waitForAllToFinish();
    }

    void operator() (Callback && callback, Priority priority = {}, std::optional<uint64_t> wait_microseconds = {})
    {
        auto promise = std::make_shared<std::promise<Result>>();
        auto & task = tasks.emplace_back(std::make_shared<Task>());
        task->future = promise->get_future();

        auto task_func = [this, task, thread_group = CurrentThread::getGroup(), my_callback = std::move(callback), promise]() mutable -> void
        {
            TaskState expected = SCHEDULED;
            if (!task->state.compare_exchange_strong(expected, RUNNING))
            {
                if (expected == CANCELLED)
                    return;
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state {} when running a task in {}", expected, thread_name);
            }

            SCOPE_EXIT_SAFE(
            {
                expected = RUNNING;
                if (!task->state.compare_exchange_strong(expected, FINISHED))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state {} when finishing a task in {}", expected, thread_name);
            });

            executeCallback(*promise, std::move(my_callback), std::move(thread_group), thread_name);
        };

        try
        {
            /// Note: calling method scheduleOrThrowOnError in intentional, because we don't want to throw exceptions
            /// in critical places where this callback runner is used (e.g. loading or deletion of parts)
            if (wait_microseconds)
                pool.scheduleOrThrow(std::move(task_func), priority, *wait_microseconds);
            else
                pool.scheduleOrThrowOnError(std::move(task_func), priority);
        }
        catch (...)
        {
            promise->set_exception(std::current_exception());
            throw;
        }
    }

    void waitForAllToFinish()
    {
        for (const auto & task : tasks)
        {
            TaskState state = task->state;
            /// It can be cancelled only when waiting in dtor
            if (state == CANCELLED)
                continue;
            if (task->future.valid())
                task->future.wait();
        }
    }

    void waitForAllToFinishAndRethrowFirstError()
    {
        waitForAllToFinish();

        for (auto & task : tasks)
        {
            /// task->future may be invalid if waitForAllToFinishAndRethrowFirstError() is called multiple times
            /// and previous call has already rethrown the exception and has not cleared tasks.
            if (task->future.valid())
                task->future.get();
        }

        tasks.clear();
    }

};

}
