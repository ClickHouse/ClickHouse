#pragma once

#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
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
            if (thread_group)
                CurrentThread::attachToGroup(thread_group);

            SCOPE_EXIT_SAFE(
            {
                {
                    /// Release all captured resources before detaching thread group
                    /// Releasing has to use proper memory tracker which has been set here before callback

                    [[maybe_unused]] auto tmp = std::move(my_callback);
                }

                if (thread_group)
                    CurrentThread::detachFromGroupIfNotDetached();
            });

            setThreadName(thread_name.data());

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
class ThreadPoolCallbackRunnerLocal
{
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

    void operator() (Callback && callback, Priority priority = {})
    {
        auto & task = tasks.emplace_back(std::make_shared<Task>());

        auto task_func = std::make_shared<std::packaged_task<Result()>>(
        [task, thread_group = CurrentThread::getGroup(), my_thread_name = thread_name, my_callback = std::move(callback)]() mutable -> Result
        {
            TaskState expected = SCHEDULED;
            if (!task->state.compare_exchange_strong(expected, RUNNING))
            {
                if (expected == CANCELLED)
                    return;
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state {} when running a task in {}", expected, my_thread_name);
            }

            SCOPE_EXIT_SAFE(
            {
                expected = RUNNING;
                if (!task->state.compare_exchange_strong(expected, FINISHED))
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state {} when finishing a task in {}", expected, my_thread_name);
            });

            if (thread_group)
                CurrentThread::attachToGroup(thread_group);

            SCOPE_EXIT_SAFE(
            {
                {
                    /// Release all captured resources before detaching thread group
                    /// Releasing has to use proper memory tracker which has been set here before callback

                    [[maybe_unused]] auto tmp = std::move(my_callback);
                }

                if (thread_group)
                    CurrentThread::detachFromGroupIfNotDetached();
            });

            setThreadName(my_thread_name.data());

            return my_callback();
        });

        task->future = task_func->get_future();

        /// Note: calling method scheduleOrThrowOnError in intentional, because we don't want to throw exceptions
        /// in critical places where this callback runner is used (e.g. loading or deletion of parts)
        pool.scheduleOrThrowOnError([my_task = std::move(task_func)]{ (*my_task)(); }, priority);
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
            task->future.get();

        tasks.clear();
    }

};

}
