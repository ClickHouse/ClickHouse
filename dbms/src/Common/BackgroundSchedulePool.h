#pragma once

#include <Poco/Notification.h>
#include <Poco/NotificationQueue.h>
#include <Poco/Timestamp.h>

#include <thread>
#include <atomic>
#include <mutex>
#include <vector>
#include <map>
#include <functional>
#include <boost/noncopyable.hpp>


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

        /// Schedule for execution as soon as possible (if not already scheduled).
        bool schedule();

        /// Schedule for execution after specified delay.
        bool scheduleAfter(size_t ms);

        void pause();
        void resume();

    private:
        using Guard = std::lock_guard<std::recursive_mutex>;

        friend class TaskNotification;
        friend class BackgroundSchedulePool;

        void invalidate();
        void execute();

        std::recursive_mutex lock;
        std::atomic<bool> removed {false};
        std::string name;
        bool scheduled = false;
        BackgroundSchedulePool & pool;
        Task function;
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
    void scheduleDelayedTask(const TaskHandle & task, size_t ms);
    void cancelDelayedTask(const TaskHandle & task);

    const size_t size;
    std::atomic<bool> shutdown {false};
    Threads threads;
    Poco::NotificationQueue queue;

    // delayed notifications

    std::condition_variable wake_event;
    std::mutex delayed_tasks_lock;
    std::thread delayed_thread;
    Tasks delayed_tasks;
};

using BackgroundSchedulePoolPtr = std::shared_ptr<BackgroundSchedulePool>;

}
