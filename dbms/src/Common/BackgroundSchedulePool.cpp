#include <Common/BackgroundSchedulePool.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <common/logger_useful.h>
#include <chrono>

namespace CurrentMetrics
{
    extern const Metric BackgroundSchedulePoolTask;
    extern const Metric MemoryTrackingInBackgroundSchedulePool;
}

namespace DB
{


// TaskNotification

class TaskNotification final : public Poco::Notification
{
public:
    explicit TaskNotification(const BackgroundSchedulePool::TaskHandle & task) : task(task) {}
    void execute() { task->execute(); }

private:
    BackgroundSchedulePool::TaskHandle task;
};


// BackgroundSchedulePool::TaskInfo

BackgroundSchedulePool::TaskInfo::TaskInfo(BackgroundSchedulePool & pool, const std::string & name, const Task & function):
    name(name),
    pool(pool),
    function(function)
{
}


bool BackgroundSchedulePool::TaskInfo::schedule()
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;

    scheduled = true;

    if (!executing)
    {
        if (delayed)
            pool.cancelDelayedTask(shared_from_this(), lock);

        pool.queue.enqueueNotification(new TaskNotification(shared_from_this()));
    }

    return true;
}


bool BackgroundSchedulePool::TaskInfo::scheduleAfter(size_t ms)
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;

    pool.scheduleDelayedTask(shared_from_this(), ms, lock);
    return true;
}


void BackgroundSchedulePool::TaskInfo::deactivate()
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


void BackgroundSchedulePool::TaskInfo::activate()
{
    std::lock_guard lock(schedule_mutex);
    deactivated = false;
}


void BackgroundSchedulePool::TaskInfo::execute()
{
    std::lock_guard lock_exec(exec_mutex);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        if (deactivated)
            return;

        scheduled = false;
        executing = true;
    }

    CurrentMetrics::Increment metric_increment{CurrentMetrics::BackgroundSchedulePoolTask};

    Stopwatch watch;
    function();
    UInt64 milliseconds = watch.elapsedMilliseconds();

    /// If the task is executed longer than specified time, it will be logged.
    static const int32_t slow_execution_threshold_ms = 50;

    if (milliseconds >= slow_execution_threshold_ms)
        LOG_INFO(&Logger::get("BackgroundSchedulePool"), "Executing " << name << " took " << milliseconds << " ms.");

    {
        std::lock_guard lock_schedule(schedule_mutex);

        executing = false;

        /// In case was scheduled while executing (including a scheduleAfter which expired) we schedule the task
		/// on the queue. We don't call the function again here because this way all tasks
		/// will have their chance to execute

        if(scheduled)
                pool.queue.enqueueNotification(new TaskNotification(shared_from_this()));
    }

}

zkutil::WatchCallback BackgroundSchedulePool::TaskInfo::getWatchCallback()
{
     return [t=shared_from_this()](const ZooKeeperImpl::ZooKeeper::WatchResponse &) {
         t->schedule();
     };
}


// BackgroundSchedulePool

BackgroundSchedulePool::BackgroundSchedulePool(size_t size)
    : size(size)
{
    LOG_INFO(&Logger::get("BackgroundSchedulePool"), "Create BackgroundSchedulePool with " << size << " threads");

    threads.resize(size);
    for (auto & thread : threads)
        thread = std::thread([this] { threadFunction(); });

    delayed_thread = std::thread([this] { delayExecutionThreadFunction(); });
}


BackgroundSchedulePool::~BackgroundSchedulePool()
{
    try
    {
        {
            std::unique_lock lock(delayed_tasks_lock);
            shutdown = true;
            wakeup_cond.notify_all();
        }

        queue.wakeUpAll();
        delayed_thread.join();

        LOG_TRACE(&Logger::get("BackgroundSchedulePool"), "Waiting for threads to finish.");
        for (std::thread & thread : threads)
            thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


BackgroundSchedulePool::TaskHandle BackgroundSchedulePool::addTask(const std::string & name, const Task & task)
{
    return std::make_shared<TaskInfo>(*this, name, task);
}


void BackgroundSchedulePool::removeTask(const TaskHandle & task)
{
    task->deactivate();
}


void BackgroundSchedulePool::scheduleDelayedTask(const TaskHandle & task, size_t ms, std::lock_guard<std::mutex> & /* schedule_mutex_lock */)
{
    Poco::Timestamp current_time;

    {
        std::lock_guard lock(delayed_tasks_lock);

        if (task->delayed)
            delayed_tasks.erase(task->iterator);

        task->iterator = delayed_tasks.emplace(current_time + (ms * 1000), task);
        task->delayed = true;
    }

    wakeup_cond.notify_all();
}


void BackgroundSchedulePool::cancelDelayedTask(const TaskHandle & task, std::lock_guard<std::mutex> & /* schedule_mutex_lock */)
{
    {
        std::lock_guard lock(delayed_tasks_lock);
        delayed_tasks.erase(task->iterator);
        task->delayed = false;
    }

    wakeup_cond.notify_all();
}


void BackgroundSchedulePool::threadFunction()
{
    setThreadName("BackgrSchedPool");

    MemoryTracker memory_tracker;
    memory_tracker.setMetric(CurrentMetrics::MemoryTrackingInBackgroundSchedulePool);
    current_memory_tracker = &memory_tracker;

    while (!shutdown)
    {
        if (Poco::AutoPtr<Poco::Notification> notification = queue.waitDequeueNotification())
        {
            TaskNotification & task_notification = static_cast<TaskNotification &>(*notification);
            task_notification.execute();
        }
    }

    current_memory_tracker = nullptr;
}


void BackgroundSchedulePool::delayExecutionThreadFunction()
{
    setThreadName("BckSchPoolDelay");

    while (!shutdown)
    {
        TaskHandle task;
        bool found = false;

        {
            std::unique_lock lock(delayed_tasks_lock);

            while(!shutdown)
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

        if(found)
            task->schedule();
    }
}

}
