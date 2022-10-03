#include "threadPoolCallbackRunner.h"

#include <Common/scope_guard_safe.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <IO/AsynchronousReader.h>
#include <future>


namespace DB
{

template <typename Result> ThreadPoolCallbackRunner<Result> threadPoolCallbackRunner(ThreadPool & pool, const std::string & thread_name)
{
    return [pool = &pool, thread_group = CurrentThread::getGroup(), thread_name](std::function<Result()> && callback, size_t priority) mutable -> std::future<Result>
    {
        auto task = std::make_shared<std::packaged_task<Result()>>([thread_group, thread_name, callback = std::move(callback)]() -> Result
        {
            if (thread_group)
                CurrentThread::attachTo(thread_group);

            SCOPE_EXIT_SAFE({
                if (thread_group)
                    CurrentThread::detachQueryIfNotDetached();
            });

            setThreadName(thread_name.data());

            return callback();
        });

        auto future = task->get_future();

        /// ThreadPool is using "bigger is higher priority" instead of "smaller is more priority".
        pool->scheduleOrThrow([task]{ (*task)(); }, -priority);

        return future;
    };
}

template ThreadPoolCallbackRunner<void> threadPoolCallbackRunner(ThreadPool & pool, const std::string & thread_name);
template ThreadPoolCallbackRunner<IAsynchronousReader::Result> threadPoolCallbackRunner(ThreadPool & pool, const std::string & thread_name);

}
