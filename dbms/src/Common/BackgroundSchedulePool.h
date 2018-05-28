#pragma once

#include <Poco/Notification.h>
#include <Poco/NotificationQueue.h>
#include <Poco/Timestamp.h>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <map>
#include <functional>
#include <boost/noncopyable.hpp>
#include <Common/ZooKeeper/Types.h>

namespace DB
{

class TaskNotification;


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
    class TaskInfo;
    using TaskHandle = std::shared_ptr<TaskInfo>;
    using Tasks = std::multimap<Poco::Timestamp, TaskHandle>;
    using Task = std::function<void()>;

    class TaskInfo : public std::enable_shared_from_this<TaskInfo>, private boost::noncopyable
    {
    public:
        TaskInfo(BackgroundSchedulePool & pool, const std::string & name, const Task & function);

        /// All these methods waits for current execution of task.

        /// Schedule for execution as soon as possible (if not already scheduled).
        /// If the task was already scheduled with delay, the delay will be ignored.
        bool schedule();

        /// Schedule for execution after specified delay.
        bool scheduleAfter(size_t ms);

        /// Further attempts to schedule become no-op.
        void deactivate();
        void activate();

        /// get zkutil::WatchCallback needed for zookeeper callbacks.

        zkutil::WatchCallback getWatchCallback();

    private:
        friend class TaskNotification;
        friend class BackgroundSchedulePool;

        void execute();

        std::mutex schedule_mutex;
        std::mutex exec_mutex;

        std::string name;
        bool deactivated = false;
        bool scheduled = false;
        bool delayed = false;
        bool executing = false;
        BackgroundSchedulePool & pool;
        Task function;

        /// If the task is scheduled with delay, points to element of delayed_tasks.
        Tasks::iterator iterator;
    };

    BackgroundSchedulePool(size_t size);
    ~BackgroundSchedulePool();

    TaskHandle addTask(const std::string & name, const Task & task);
    void removeTask(const TaskHandle & task);
    size_t getNumberOfThreads() const { return size; }

private:
    using Threads = std::vector<std::thread>;

    void threadFunction();
    void delayExecutionThreadFunction();

    /// Schedule task for execution after specified delay from now.
    void scheduleDelayedTask(const TaskHandle & task, size_t ms, std::lock_guard<std::mutex> &);

    /// Remove task, that was scheduled with delay, from schedule.
    void cancelDelayedTask(const TaskHandle & task, std::lock_guard<std::mutex> &);

    /// Number for worker threads.
    const size_t size;
    std::atomic<bool> shutdown {false};
    Threads threads;
    Poco::NotificationQueue queue;

    /// Delayed notifications.

    std::condition_variable wakeup_cond;
    std::mutex delayed_tasks_lock;
    /// Thread waiting for next delayed task.
    std::thread delayed_thread;
    /// Tasks ordered by scheduled time.
    Tasks delayed_tasks;
};

using BackgroundSchedulePoolPtr = std::shared_ptr<BackgroundSchedulePool>;

}
