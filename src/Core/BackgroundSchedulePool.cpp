#include "BackgroundSchedulePool.h"
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>
#include <base/logger_useful.h>
#include <chrono>


namespace DB
{


class TaskNotification final : public Poco::Notification
{
public:
    explicit TaskNotification(const BackgroundSchedulePoolTaskInfoPtr & task_) : task(task_) {}
    void execute() { task->execute(); }

private:
    BackgroundSchedulePoolTaskInfoPtr task;
};


BackgroundSchedulePoolTaskInfo::BackgroundSchedulePoolTaskInfo(
    BackgroundSchedulePool & pool_, const std::string & log_name_, const BackgroundSchedulePool::TaskFunc & function_)
    : pool(pool_), log_name(log_name_), function(function_)
{
}

bool BackgroundSchedulePoolTaskInfo::schedule()
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;

    scheduleImpl(lock);
    return true;
}

bool BackgroundSchedulePoolTaskInfo::scheduleAfter(size_t ms, bool overwrite)
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;
    if (delayed && !overwrite)
        return false;

    pool.scheduleDelayedTask(shared_from_this(), ms, lock);
    return true;
}

void BackgroundSchedulePoolTaskInfo::deactivate()
{
    std::lock_guard lock_exec(exec_mutex);
    std::lock_guard lock_schedule(schedule_mutex);

    if (deactivated)
        return;

    deactivated = true;
    scheduled = false;

    if (delayed)
        pool.cancelDelayedTask(shared_from_this(), lock_schedule);
}

void BackgroundSchedulePoolTaskInfo::activate()
{
    std::lock_guard lock(schedule_mutex);
    deactivated = false;
}

bool BackgroundSchedulePoolTaskInfo::activateAndSchedule()
{
    std::lock_guard lock(schedule_mutex);

    deactivated = false;
    if (scheduled)
        return false;

    scheduleImpl(lock);
    return true;
}

void BackgroundSchedulePoolTaskInfo::execute()
{
    Stopwatch watch;
    CurrentMetrics::Increment metric_increment{pool.tasks_metric};

    std::lock_guard lock_exec(exec_mutex);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        if (deactivated)
            return;

        scheduled = false;
        executing = true;
    }

    function();
    UInt64 milliseconds = watch.elapsedMilliseconds();

    /// If the task is executed longer than specified time, it will be logged.
    static const int32_t slow_execution_threshold_ms = 200;

    if (milliseconds >= slow_execution_threshold_ms)
        LOG_TRACE(&Poco::Logger::get(log_name), "Execution took {} ms.", milliseconds);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        executing = false;

        /// In case was scheduled while executing (including a scheduleAfter which expired) we schedule the task
        /// on the queue. We don't call the function again here because this way all tasks
        /// will have their chance to execute

        if (scheduled)
            pool.queue.enqueueNotification(new TaskNotification(shared_from_this()));
    }
}

void BackgroundSchedulePoolTaskInfo::scheduleImpl(std::lock_guard<std::mutex> & schedule_mutex_lock)
{
    scheduled = true;

    if (delayed)
        pool.cancelDelayedTask(shared_from_this(), schedule_mutex_lock);

    /// If the task is not executing at the moment, enqueue it for immediate execution.
    /// But if it is currently executing, do nothing because it will be enqueued
    /// at the end of the execute() method.
    if (!executing)
        pool.queue.enqueueNotification(new TaskNotification(shared_from_this()));
}

Coordination::WatchCallback BackgroundSchedulePoolTaskInfo::getWatchCallback()
{
     return [t = shared_from_this()](const Coordination::WatchResponse &)
     {
         t->schedule();
     };
}


BackgroundSchedulePool::BackgroundSchedulePool(size_t size_, CurrentMetrics::Metric tasks_metric_, const char *thread_name_)
    : size(size_)
    , tasks_metric(tasks_metric_)
    , thread_name(thread_name_)
{
    LOG_INFO(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name), "Create BackgroundSchedulePool with {} threads", size);

    threads.resize(size);
    for (auto & thread : threads)
        thread = ThreadFromGlobalPool([this] { threadFunction(); });

    delayed_thread = ThreadFromGlobalPool([this] { delayExecutionThreadFunction(); });
}


BackgroundSchedulePool::~BackgroundSchedulePool()
{
    try
    {
        {
            std::unique_lock lock(delayed_tasks_mutex);
            shutdown = true;
            wakeup_cond.notify_all();
        }

        queue.wakeUpAll();
        delayed_thread.join();

        LOG_TRACE(&Poco::Logger::get("BackgroundSchedulePool/" + thread_name), "Waiting for threads to finish.");
        for (auto & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


BackgroundSchedulePool::TaskHolder BackgroundSchedulePool::createTask(const std::string & name, const TaskFunc & function)
{
    return TaskHolder(std::make_shared<TaskInfo>(*this, name, function));
}


void BackgroundSchedulePool::scheduleDelayedTask(const TaskInfoPtr & task, size_t ms, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */)
{
    Poco::Timestamp current_time;

    {
        std::lock_guard lock(delayed_tasks_mutex);

        if (task->delayed)
            delayed_tasks.erase(task->iterator);

        task->iterator = delayed_tasks.emplace(current_time + (ms * 1000), task);
        task->delayed = true;
    }

    wakeup_cond.notify_all();
}


void BackgroundSchedulePool::cancelDelayedTask(const TaskInfoPtr & task, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */)
{
    {
        std::lock_guard lock(delayed_tasks_mutex);
        delayed_tasks.erase(task->iterator);
        task->delayed = false;
    }

    wakeup_cond.notify_all();
}


void BackgroundSchedulePool::attachToThreadGroup()
{
    std::lock_guard lock(delayed_tasks_mutex);

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


void BackgroundSchedulePool::threadFunction()
{
    setThreadName(thread_name.c_str());

    attachToThreadGroup();

    while (!shutdown)
    {
        /// We have to wait with timeout to prevent very rare deadlock, caused by the following race condition:
        /// 1. Background thread N: threadFunction(): checks for shutdown (it's false)
        /// 2. Main thread: ~BackgroundSchedulePool(): sets shutdown to true, calls queue.wakeUpAll(), it triggers
        ///    all existing Poco::Events inside Poco::NotificationQueue which background threads are waiting on.
        /// 3. Background thread N: threadFunction(): calls queue.waitDequeueNotification(), it creates
        ///    new Poco::Event inside Poco::NotificationQueue and starts to wait on it
        /// Background thread N will never be woken up.
        /// TODO Do we really need Poco::NotificationQueue? Why not to use std::queue + mutex + condvar or maybe even DB::ThreadPool?
        constexpr size_t wait_timeout_ms = 500;
        if (Poco::AutoPtr<Poco::Notification> notification = queue.waitDequeueNotification(wait_timeout_ms))
        {
            TaskNotification & task_notification = static_cast<TaskNotification &>(*notification);
            task_notification.execute();
        }
    }
}


void BackgroundSchedulePool::delayExecutionThreadFunction()
{
    setThreadName((thread_name + "/D").c_str());

    attachToThreadGroup();

    while (!shutdown)
    {
        TaskInfoPtr task;
        bool found = false;

        {
            std::unique_lock lock(delayed_tasks_mutex);

            while (!shutdown)
            {
                Poco::Timestamp min_time;

                if (!delayed_tasks.empty())
                {
                    auto t = delayed_tasks.begin();
                    min_time = t->first;
                    task = t->second;
                }

                if (!task)
                {
                    wakeup_cond.wait(lock);
                    continue;
                }

                Poco::Timestamp current_time;

                if (min_time > current_time)
                {
                    wakeup_cond.wait_for(lock, std::chrono::microseconds(min_time - current_time));
                    continue;
                }
                else
                {
                    /// We have a task ready for execution
                    found = true;
                    break;
                }
            }
        }

        if (found)
            task->schedule();
    }
}

}
