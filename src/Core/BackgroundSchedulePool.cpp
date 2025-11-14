#include <Core/BackgroundSchedulePool.h>
#include <Core/UUID.h>

#include <IO/WriteHelpers.h>
#include <base/defines.h>

#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/CurrentThread.h>
#include <Common/UniqueLock.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>

#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_SCHEDULE_TASK;
    extern const int ABORTED;
}

///
/// BackgroundSchedulePoolTaskInfo
///

BackgroundSchedulePoolTaskInfo::BackgroundSchedulePoolTaskInfo(
    BackgroundSchedulePoolWeakPtr pool_, const std::string & log_name_, const BackgroundSchedulePool::TaskFunc & function_)
    : pool_ref(pool_)
    , log_name(log_name_)
    , function(function_)
{
}

bool BackgroundSchedulePoolTaskInfo::schedule()
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;

    return scheduleImpl(lock);
}

bool BackgroundSchedulePoolTaskInfo::scheduleAfter(size_t milliseconds, bool overwrite, bool only_if_scheduled)
{
    std::lock_guard lock(schedule_mutex);

    if (deactivated || scheduled)
        return false;
    if (delayed && !overwrite)
        return false;
    if (!delayed && only_if_scheduled)
        return false;

    auto pool_ptr = pool_ref.lock();
    if (!pool_ptr)
        return false;

    pool_ptr->scheduleDelayedTask(*this, milliseconds, lock);
    return true;
}

bool BackgroundSchedulePoolTaskInfo::deactivate()
{
    std::lock_guard lock_exec(exec_mutex);
    std::lock_guard lock_schedule(schedule_mutex);

    if (deactivated)
        return false;

    deactivated = true;
    scheduled = false;

    if (delayed)
    {
        auto pool_ptr = pool_ref.lock();
        if (!pool_ptr)
            return false;

        pool_ptr->cancelDelayedTask(*this, lock_schedule);
    }

    return true;
}

bool BackgroundSchedulePoolTaskInfo::activate()
{
    std::lock_guard lock(schedule_mutex);
    deactivated = false;
    return true;
}

bool BackgroundSchedulePoolTaskInfo::activateAndSchedule()
{
    std::lock_guard lock(schedule_mutex);

    deactivated = false;
    if (scheduled)
        return false;

    return scheduleImpl(lock);
}

std::unique_lock<std::mutex> BackgroundSchedulePoolTaskInfo::getExecLock()
{
    return std::unique_lock{exec_mutex};
}

void BackgroundSchedulePoolTaskInfo::execute(BackgroundSchedulePool & pool)
{
    Stopwatch watch;
    CurrentMetrics::Increment metric_increment(pool.tasks_metric);

    std::lock_guard lock_exec(exec_mutex);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        if (deactivated)
            return;

        scheduled = false;
        executing = true;
    }

    /// Using this tmp query_id storage to prevent bad_alloc thrown under the try/catch.
    std::string task_query_id = fmt::format("{}::{}", pool.thread_name, UUIDHelpers::generateV4());

    try
    {
        chassert(current_thread); /// Thread from global thread pool
        current_thread->setQueryId(std::move(task_query_id));

        function();

        current_thread->clearQueryId();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false && "Tasks in BackgroundSchedulePool cannot throw");
    }
    UInt64 milliseconds = watch.elapsedMilliseconds();

    /// If the task is executed longer than specified time, it will be logged.
    static constexpr UInt64 slow_execution_threshold_ms = 200;

    if (milliseconds >= slow_execution_threshold_ms)
        LOG_TRACE(getLogger(log_name), "Execution took {} ms.", milliseconds);

    {
        std::lock_guard lock_schedule(schedule_mutex);

        executing = false;

        /// In case was scheduled while executing (including a scheduleAfter which expired) we schedule the task
        /// on the queue. We don't call the function again here because this way all tasks
        /// will have their chance to execute

        if (scheduled)
            pool.scheduleTask(*this);
    }
}

bool BackgroundSchedulePoolTaskInfo::scheduleImpl(std::lock_guard<std::mutex> & schedule_mutex_lock) TSA_REQUIRES(schedule_mutex)
{
    scheduled = true;

    auto pool_ptr = pool_ref.lock();
    if (!pool_ptr)
        return false;

    if (delayed)
        pool_ptr->cancelDelayedTask(*this, schedule_mutex_lock);

    /// If the task is not executing at the moment, enqueue it for immediate execution.
    /// But if it is currently executing, do nothing because it will be enqueued
    /// at the end of the execute() method.
    if (!executing)
        pool_ptr->scheduleTask(*this);

    return true;
}

Coordination::WatchCallbackPtr BackgroundSchedulePoolTaskInfo::getWatchCallback()
{
    /// We cannot initialize it inside ctor, since weak_from_this() will return empty ptr, that will never become valid (shared_from_this() will throw)
    callOnce(watch_callback_initialized, [&] {
        watch_callback = std::make_shared<Coordination::WatchCallback>([task_weak = weak_from_this()](const Coordination::WatchResponse &)
        {
            if (auto task = task_weak.lock())
                task->schedule();
        });
    });
    return watch_callback;
}


///
/// BackgroundSchedulePool
///

BackgroundSchedulePoolPtr BackgroundSchedulePool::create(size_t size, size_t max_parallel_tasks_per_type, CurrentMetrics::Metric tasks_metric, CurrentMetrics::Metric size_metric, ThreadName thread_name)
{
    return std::shared_ptr<BackgroundSchedulePool>(new BackgroundSchedulePool(size, max_parallel_tasks_per_type, tasks_metric, size_metric, thread_name));
}

BackgroundSchedulePool::BackgroundSchedulePool(size_t size_, size_t max_parallel_tasks_per_type_, CurrentMetrics::Metric tasks_metric_, CurrentMetrics::Metric size_metric_, ThreadName thread_name_)
    : logger(getLogger(fmt::format("BackgroundSchedulePool/{}", toString(thread_name_))))
    , tasks_metric(tasks_metric_)
    , size_metric(size_metric_, size_)
    , thread_name(thread_name_)
    , max_parallel_tasks_per_type(max_parallel_tasks_per_type_ ? max_parallel_tasks_per_type_ : size_)
{
    LOG_INFO(logger, "Create BackgroundSchedulePool with {} threads", size_);

    threads.resize(size_);

    try
    {
        for (auto & thread : threads)
            thread = ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); });

        delayed_thread = std::make_unique<ThreadFromGlobalPoolNoTracingContextPropagation>([this] { delayExecutionThreadFunction(); });
    }
    catch (...)
    {
        LOG_FATAL(
            logger,
            "Couldn't get {} threads from global thread pool: {}",
            size_,
            getCurrentExceptionCode() == ErrorCodes::CANNOT_SCHEDULE_TASK
                ? "Not enough threads. Please make sure max_thread_pool_size is considerably "
                  "bigger than background_schedule_pool_size."
                : getCurrentExceptionMessage(/* with_stacktrace */ true));
        abort();
    }
}


void BackgroundSchedulePool::increaseThreadsCount(size_t new_threads_count)
{
    if (shutdown)
        throw Exception(ErrorCodes::ABORTED, "Pool already destroyed");

    const size_t old_threads_count = threads.size();

    if (new_threads_count < old_threads_count)
    {
        LOG_WARNING(logger,
            "Tried to increase the number of threads but the new threads count ({}) is not greater than old one ({})", new_threads_count, old_threads_count);
        return;
    }

    threads.resize(new_threads_count);
    for (size_t i = old_threads_count; i < new_threads_count; ++i)
        threads[i] = ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); });

    size_metric.changeTo(new_threads_count);
}


void BackgroundSchedulePool::join()
{
    try
    {
        shutdown = true;

        /// Unlock threads
        tasks_cond_var.notify_all();
        delayed_tasks_cond_var.notify_all();

        /// Join all worker threads to avoid any recursive calls to schedule()/scheduleAfter() from the task callbacks
        {
            Stopwatch watch;
            LOG_TRACE(logger, "Waiting for threads to finish.");
            delayed_thread->join();
            delayed_thread.reset();
            for (auto & thread : threads)
                thread.join();
            threads.clear();
            LOG_TRACE(logger, "Threads finished in {}ms.", watch.elapsedMilliseconds());
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

BackgroundSchedulePool::~BackgroundSchedulePool()
{
    chassert(shutdown == true, "BackgroundSchedulePool::join() has not been called");
    chassert(static_cast<bool>(delayed_thread) == false, "BackgroundSchedulePool::delayed_thread has not been joined");
    chassert(threads.empty(), "BackgroundSchedulePool::threads have not been joined");
}


BackgroundSchedulePool::TaskHolder BackgroundSchedulePool::createTask(const std::string & name, const TaskFunc & function)
{
    return TaskHolder(std::shared_ptr<TaskInfo>(new TaskInfo(weak_from_this(), name, function)));
}

template<typename T>
UInt64 getFunctionID(const std::function<T> & func)
{
    /// Get a pointer to the task function and use it as an identifier of the task type
    auto * func_ptr = func.template target<T *>();
    if (func_ptr)
        return reinterpret_cast<UInt64>(func_ptr);

    /// Lambdas have weird types, and we cannot get a pointer. Let's use has of the lambda type name,
    /// which is usually smth like "ZN2DB22BackgroundJobsAssignee5startEvE3" or "DB::BackgroundJobsAssignee::start()::$_0"
    /// And it's a good identifier
    SipHash hash;
    hash.update(func.target_type().name());
    return hash.get64();
}

void BackgroundSchedulePool::scheduleTask(TaskInfo & task_info)
{
    {
        std::lock_guard tasks_lock(tasks_mutex);
        /// Get a pointer to the task function and use it as an identifier of the task type
        UInt64 task_type = getFunctionID(task_info.function);
        auto & group = task_groups[task_type];
        group.tasks.emplace_back(task_info.shared_from_this());
        if (!group.runnable_list_pos && group.num_running < max_parallel_tasks_per_type)
        {
            group.runnable_list_pos = runnable_task_types.size();
            runnable_task_types.push_back(task_type);
        }
    }

    tasks_cond_var.notify_one();
}

void BackgroundSchedulePool::scheduleDelayedTask(TaskInfo & task, size_t ms, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */) TSA_REQUIRES(task.schedule_mutex)
{
    Poco::Timestamp current_time;

    {
        std::lock_guard lock(delayed_tasks_mutex);

        if (task.delayed)
            delayed_tasks.erase(task.iterator);

        task.iterator = delayed_tasks.emplace(current_time + (ms * 1000), task.shared_from_this());
        task.delayed = true;
    }

    delayed_tasks_cond_var.notify_all();
}


void BackgroundSchedulePool::cancelDelayedTask(TaskInfo & task, std::lock_guard<std::mutex> & /* task_schedule_mutex_lock */) TSA_REQUIRES(task.schedule_mutex)
{
    {
        std::lock_guard lock(delayed_tasks_mutex);
        delayed_tasks.erase(task.iterator);
        task.delayed = false;
        task.iterator = delayed_tasks.end();
    }

    delayed_tasks_cond_var.notify_all();
}


void BackgroundSchedulePool::threadFunction()
{
    DB::setThreadName(thread_name);

    while (!shutdown)
    {
        UInt64 task_type_to_run;
        TaskInfoPtr task;

        current_thread->flushUntrackedMemory();

        {
            UniqueLock tasks_lock(tasks_mutex);

            /// TSA_NO_THREAD_SAFETY_ANALYSIS because it doesn't understand within the lambda that the
            /// tasks_lock has already locked tasks_mutex.
            tasks_cond_var.wait(tasks_lock.getUnderlyingLock(), [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
            {
                return shutdown || !runnable_task_types.empty();
            });
            if (shutdown)
                break;

            if (runnable_task_types.empty())
                continue;

            task_type_to_run = runnable_task_types[thread_local_rng() % runnable_task_types.size()];
            auto & group = task_groups[task_type_to_run];
            chassert(!group.tasks.empty());
            chassert(group.num_running < max_parallel_tasks_per_type);

            task = group.tasks.front();
            group.tasks.pop_front();
            ++group.num_running;

            if (group.num_running == max_parallel_tasks_per_type || group.tasks.empty())
            {
                /// Tasks from this group are not runnable anymore
                auto & other_group = task_groups[runnable_task_types.back()];
                std::swap(runnable_task_types[*group.runnable_list_pos], runnable_task_types.back());
                runnable_task_types.pop_back();
                other_group.runnable_list_pos = group.runnable_list_pos;
                group.runnable_list_pos.reset();
                if (group.num_running == max_parallel_tasks_per_type)
                    LOG_WARNING(logger, "Temporarily pause scheduling of tasks with id {}, example log_name={}", task_type_to_run, task->log_name);
            }
        }

        if (task)
        {
            task->execute(*this);

            UniqueLock tasks_lock(tasks_mutex);
            auto & group = task_groups[task_type_to_run];
            chassert(group.num_running);
            --group.num_running;
            if (!group.tasks.empty() && !group.runnable_list_pos)
            {
                chassert(group.num_running < max_parallel_tasks_per_type);
                group.runnable_list_pos = runnable_task_types.size();
                runnable_task_types.push_back(task_type_to_run);
                tasks_cond_var.notify_one();
            }
        }

        current_thread->flushUntrackedMemory();
    }
}


void BackgroundSchedulePool::delayExecutionThreadFunction()
{
    DB::setThreadName(ThreadName::POOL_DELAYED_EXECUTION);

    while (!shutdown)
    {
        TaskInfoPtr task;
        bool found = false;

        {
            UniqueLock lock(delayed_tasks_mutex);

            while (!shutdown)
            {
                Poco::Timestamp current_time;
                Poco::Timestamp min_time = current_time;

                if (!delayed_tasks.empty())
                {
                    auto t = delayed_tasks.begin();
                    min_time = t->first;
                    task = t->second;
                }

                if (!task)
                {
                    delayed_tasks_cond_var.wait(lock.getUnderlyingLock());
                    if (shutdown)
                        break;
                    continue;
                }

                if (min_time > current_time)
                {
                    delayed_tasks_cond_var.wait_for(lock.getUnderlyingLock(), std::chrono::microseconds(min_time - current_time));
                    if (shutdown)
                        break;
                    continue;
                }

                /// We have a task ready for execution
                found = true;
                break;
            }
        }

        if (found)
            task->schedule();
    }
}

}
