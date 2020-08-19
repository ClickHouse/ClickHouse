#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/MemoryTracker.h>
#include <Common/randomSeed.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>
#include <Storages/MergeTree/BackgroundProcessingPool.h>
#include <Common/CurrentThread.h>
#include <Interpreters/DNSCacheUpdater.h>

#include <ext/scope_guard.h>
#include <pcg_random.hpp>
#include <random>


namespace DB
{

void BackgroundProcessingPoolTaskInfo::signalReadyToRun()
{
    Poco::Timestamp current_time;
    {
        std::unique_lock lock(pool.tasks_mutex);

        /// This check ensures that the iterator is valid. Must be performed under the same mutex as invalidation.
        if (removed)
            return;

        /// If this task did nothing the previous time and still should sleep, then reschedule to cancel the sleep.
        const auto & scheduled_time = iterator->first;
        if (scheduled_time > current_time)
            pool.rescheduleTask(iterator, current_time);

        /// Note that if all threads are currently busy doing their work, this call will not wakeup any thread.
        pool.wake_event.notify_one();
    }
}


BackgroundProcessingPool::BackgroundProcessingPool(int size_,
        const PoolSettings & pool_settings,
        const char * log_name,
        const char * thread_name_)
    : size(size_)
    , thread_name(thread_name_)
    , settings(pool_settings)
{
    logger = &Poco::Logger::get(log_name);
    LOG_INFO(logger, "Create {} with {} threads", log_name, size);

    threads.resize(size);
    for (auto & thread : threads)
        thread = ThreadFromGlobalPool([this] { workLoopFunc(); });
}


BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::createTask(const Task & task)
{
    return std::make_shared<TaskInfo>(*this, task);
}

void BackgroundProcessingPool::startTask(const TaskHandle & task, bool allow_execute_in_parallel)
{
    Poco::Timestamp current_time;

    task->allow_execute_in_parallel = allow_execute_in_parallel;

    {
        std::unique_lock lock(tasks_mutex);
        task->iterator = tasks.emplace(current_time, task);

        wake_event.notify_all();
    }

}

BackgroundProcessingPool::TaskHandle BackgroundProcessingPool::addTask(const Task & task)
{
    TaskHandle res = createTask(task);
    startTask(res);
    return res;
}

void BackgroundProcessingPool::removeTask(const TaskHandle & task)
{
    if (task->removed.exchange(true))
        return;

    /// Wait for all executions of this task.
    {
        std::unique_lock wlock(task->rwlock);
    }

    {
        std::unique_lock lock(tasks_mutex);
        tasks.erase(task->iterator);
        /// Note that the task may be still accessible through TaskHandle (shared_ptr).
    }
}

BackgroundProcessingPool::~BackgroundProcessingPool()
{
    try
    {
        {
            std::lock_guard lock(tasks_mutex);
            shutdown = true;
            wake_event.notify_all();
        }

        for (auto & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void BackgroundProcessingPool::workLoopFunc()
{
    setThreadName(thread_name);

    {
        std::lock_guard lock(tasks_mutex);

        if (thread_group)
        {
            /// Put all threads to one thread pool
            CurrentThread::attachTo(thread_group);
        }
        else
        {
            CurrentThread::initializeQuery();
            thread_group = CurrentThread::getGroup();
        }
    }

    SCOPE_EXIT({ CurrentThread::detachQueryIfNotDetached(); });
    if (auto * const memory_tracker = CurrentThread::getMemoryTracker())
        memory_tracker->setMetric(settings.memory_metric);

    pcg64 rng(randomSeed());
    std::this_thread::sleep_for(std::chrono::duration<double>(std::uniform_real_distribution<double>(0, settings.thread_sleep_seconds_random_part)(rng)));

    Poco::Timestamp scheduled_task_start_time;

    while (true)
    {
        TaskResult task_result = TaskResult::ERROR;
        TaskHandle task;

        {
            std::unique_lock lock(tasks_mutex);

            while (!task && !shutdown)
            {
                for (const auto & [time, handle] : tasks)
                {
                    if (!handle->removed
                        && (handle->allow_execute_in_parallel || handle->concurrent_executors == 0))
                    {
                        task = handle;
                        scheduled_task_start_time = time;
                        ++task->concurrent_executors;
                        break;
                    }
                }

                if (task)
                {
                    Poco::Timestamp current_time;

                    if (scheduled_task_start_time <= current_time)
                        continue;

                    wake_event.wait_for(lock,
                        std::chrono::microseconds(scheduled_task_start_time - current_time
                            + std::uniform_int_distribution<uint64_t>(0, settings.thread_sleep_seconds_random_part * 1000000)(rng)));
                }
                else
                {
                    wake_event.wait_for(lock,
                        std::chrono::duration<double>(settings.thread_sleep_seconds
                            + std::uniform_real_distribution<double>(0, settings.thread_sleep_seconds_random_part)(rng)));
                }
            }

            if (shutdown)
                break;
        }

        std::shared_lock rlock(task->rwlock);

        if (task->removed)
            continue;

        try
        {
            CurrentMetrics::Increment metric_increment{settings.tasks_metric};
            task_result = task->task_function();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        {
            std::unique_lock lock(tasks_mutex);

            if (shutdown)
                break;

            --task->concurrent_executors;

            if (task->removed)
                continue;

            if (task_result == TaskResult::SUCCESS)
                task->count_no_work_done = 0;
            else
                ++task->count_no_work_done;

            /// If task has done work, it could be executed again immediately.
            /// If not, add delay before next run.

            Poco::Timestamp next_time_to_execute;   /// current time
            if (task_result == TaskResult::ERROR)
                next_time_to_execute += 1000000 * (std::min(
                        settings.task_sleep_seconds_when_no_work_max,
                        settings.task_sleep_seconds_when_no_work_min * std::pow(settings.task_sleep_seconds_when_no_work_multiplier, task->count_no_work_done))
                    + std::uniform_real_distribution<double>(0, settings.task_sleep_seconds_when_no_work_random_part)(rng));
            else if (task_result == TaskResult::NOTHING_TO_DO)
                next_time_to_execute += 1000000 * settings.thread_sleep_seconds_if_nothing_to_do;

            rescheduleTask(task->iterator, next_time_to_execute);
        }
    }
}

}
