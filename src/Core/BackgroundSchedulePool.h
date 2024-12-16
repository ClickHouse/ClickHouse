#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <thread>
#include <vector>
#include <base/scope_guard.h>
#include <boost/noncopyable.hpp>
#include <Poco/Notification.h>
#include <Poco/NotificationQueue.h>
#include <Poco/Timestamp.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/ZooKeeper/Types.h>


namespace DB
{

class TaskNotification;
class BackgroundSchedulePoolTaskInfo;
class BackgroundSchedulePoolTaskHolder;


/** Executes functions scheduled at a specific point in time.
  * Basically all tasks are added in a queue and precessed by worker threads.
  *
  * The most important difference between this and BackgroundProcessingPool
  *  is that we have the guarantee that the same function is not executed from many workers in the same time.
  *
  * The usage scenario: instead starting a separate thread for each task,
  *  register a task in BackgroundSchedulePool and when you need to run the task,
  *  call schedule or scheduleAfter(duration) method.
  */
class BackgroundSchedulePool
{
public:
    friend class BackgroundSchedulePoolTaskInfo;

    using TaskInfo = BackgroundSchedulePoolTaskInfo;
    using TaskInfoPtr = std::shared_ptr<TaskInfo>;
    using TaskFunc = std::function<void()>;
    using TaskHolder = BackgroundSchedulePoolTaskHolder;
    using DelayedTasks = std::multimap<Poco::Timestamp, TaskInfoPtr>;

    TaskHolder createTask(const std::string & log_name, const TaskFunc & function);

    /// As for MergeTreeBackgroundExecutor we refuse to implement tasks eviction, because it will
    /// be error prone. We support only increasing number of threads at runtime.
    void increaseThreadsCount(size_t new_threads_count);

    /// thread_name_ cannot be longer then 13 bytes (2 bytes is reserved for "/D" suffix for delayExecutionThreadFunction())
    BackgroundSchedulePool(size_t size_, CurrentMetrics::Metric tasks_metric_, CurrentMetrics::Metric size_metric_, const char *thread_name_);
    ~BackgroundSchedulePool();

private:
    /// BackgroundSchedulePool schedules a task on its own task queue, there's no need to construct/restore tracing context on this level.
    /// This is also how ThreadPool class treats the tracing context. See ThreadPool for more information.
    using Threads = std::vector<ThreadFromGlobalPoolNoTracingContextPropagation>;

    void threadFunction();
    void delayExecutionThreadFunction();

    void scheduleTask(TaskInfoPtr task_info);

    /// Schedule task for execution after specified delay from now.
    void scheduleDelayedTask(const TaskInfoPtr & task_info, size_t ms, std::lock_guard<std::mutex> & task_schedule_mutex_lock);

    /// Remove task, that was scheduled with delay, from schedule.
    void cancelDelayedTask(const TaskInfoPtr & task_info, std::lock_guard<std::mutex> & task_schedule_mutex_lock);

    std::atomic<bool> shutdown {false};

    /// Tasks.
    std::condition_variable tasks_cond_var;
    std::mutex tasks_mutex;
    std::deque<TaskInfoPtr> tasks;
    Threads threads;

    /// Delayed tasks.

    std::condition_variable delayed_tasks_cond_var;
    std::mutex delayed_tasks_mutex;
    /// Thread waiting for next delayed task.
    std::unique_ptr<ThreadFromGlobalPoolNoTracingContextPropagation> delayed_thread;
    /// Tasks ordered by scheduled time.
    DelayedTasks delayed_tasks;

    CurrentMetrics::Metric tasks_metric;
    CurrentMetrics::Increment size_metric;
    std::string thread_name;
};


class BackgroundSchedulePoolTaskInfo : public std::enable_shared_from_this<BackgroundSchedulePoolTaskInfo>, private boost::noncopyable
{
public:
    BackgroundSchedulePoolTaskInfo(BackgroundSchedulePool & pool_, const std::string & log_name_, const BackgroundSchedulePool::TaskFunc & function_);

    /// Schedule for execution as soon as possible (if not already scheduled).
    /// If the task was already scheduled with delay, the delay will be ignored.
    bool schedule();

    /// Schedule for execution after specified delay.
    /// If overwrite is set, and the task is already scheduled with a delay (delayed == true),
    /// the task will be re-scheduled with the new delay.
    /// If only_if_scheduled is set, don't do anything unless the task is already scheduled with a delay.
    bool scheduleAfter(size_t milliseconds, bool overwrite = true, bool only_if_scheduled = false);

    /// Further attempts to schedule become no-op. Will wait till the end of the current execution of the task.
    void deactivate();

    void activate();

    /// Atomically activate task and schedule it for execution.
    bool activateAndSchedule();

    /// get Coordination::WatchCallback needed for notifications from ZooKeeper watches.
    Coordination::WatchCallback getWatchCallback();

    /// Returns lock that protects from concurrent task execution.
    /// This lock should not be held for a long time.
    std::unique_lock<std::mutex> getExecLock();

private:
    friend class TaskNotification;
    friend class BackgroundSchedulePool;

    void execute();

    void scheduleImpl(std::lock_guard<std::mutex> & schedule_mutex_lock);

    BackgroundSchedulePool & pool;
    std::string log_name;
    BackgroundSchedulePool::TaskFunc function;

    std::mutex exec_mutex;
    std::mutex schedule_mutex;

    /// Invariants:
    /// * If deactivated is true then scheduled, delayed and executing are all false.
    /// * scheduled and delayed cannot be true at the same time.
    bool deactivated = false;
    bool scheduled = false;
    bool delayed = false;
    bool executing = false;

    /// If the task is scheduled with delay, points to element of delayed_tasks.
    BackgroundSchedulePool::DelayedTasks::iterator iterator;
};

using BackgroundSchedulePoolTaskInfoPtr = std::shared_ptr<BackgroundSchedulePoolTaskInfo>;


class BackgroundSchedulePoolTaskHolder
{
public:
    BackgroundSchedulePoolTaskHolder() = default;
    explicit BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskInfoPtr & task_info_) : task_info(task_info_) {}
    BackgroundSchedulePoolTaskHolder(const BackgroundSchedulePoolTaskHolder & other) = delete;
    BackgroundSchedulePoolTaskHolder(BackgroundSchedulePoolTaskHolder && other) noexcept = default;
    BackgroundSchedulePoolTaskHolder & operator=(const BackgroundSchedulePoolTaskHolder & other) noexcept = delete;
    BackgroundSchedulePoolTaskHolder & operator=(BackgroundSchedulePoolTaskHolder && other) noexcept = default;

    ~BackgroundSchedulePoolTaskHolder()
    {
        if (task_info)
            task_info->deactivate();
    }

    explicit operator bool() const { return task_info != nullptr; }

    BackgroundSchedulePoolTaskInfo * operator->() { return task_info.get(); }
    const BackgroundSchedulePoolTaskInfo * operator->() const { return task_info.get(); }

private:
    BackgroundSchedulePoolTaskInfoPtr task_info;
};

}
