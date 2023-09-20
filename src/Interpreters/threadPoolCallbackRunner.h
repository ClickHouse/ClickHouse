#pragma once

#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <future>

namespace DB
{

/// High-order function to run callbacks (functions with 'void()' signature) somewhere asynchronously.
template <typename Result, typename Callback = std::function<Result()>>
using ThreadPoolCallbackRunner = std::function<std::future<Result>(Callback &&, Priority)>;

/// Creates CallbackRunner that runs every callback with 'pool->scheduleOrThrowOnError()'.
template <typename Result, typename Callback = std::function<Result()>>
ThreadPoolCallbackRunner<Result, Callback> threadPoolCallbackRunner(ThreadPool & pool, const std::string & thread_name)
{
    return [my_pool = &pool, thread_group = CurrentThread::getGroup(), thread_name](Callback && callback, Priority priority) mutable -> std::future<Result>
    {
        auto task = std::make_shared<std::packaged_task<Result()>>([thread_group, thread_name, my_callback = std::move(callback)]() mutable -> Result
        {
            if (thread_group)
                CurrentThread::attachToGroup(thread_group);

            SCOPE_EXIT_SAFE({
                {
                    /// Release all captutred resources before detaching thread group
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

        /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
        /// Note: calling method scheduleOrThrowOnError in intentional, because we don't want to throw exceptions
        /// in critical places where this callback runner is used (e.g. loading or deletion of parts)
        my_pool->scheduleOrThrowOnError([my_task = std::move(task)]{ (*my_task)(); }, priority);

        return future;
    };
}

template <typename Result, typename T>
std::future<Result> scheduleFromThreadPool(T && task, ThreadPool & pool, const std::string & thread_name, Priority priority = {})
{
    auto schedule = threadPoolCallbackRunner<Result, T>(pool, thread_name);
    return schedule(std::move(task), priority);
}

}
