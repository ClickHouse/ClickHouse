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

#include <Interpreters/Context.h>
#include <Interpreters/BackgroundSchedulePoolLog.h>

#include <unordered_set>


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
    BackgroundSchedulePoolWeakPtr pool_, const StorageID & storage_, const std::string & log_name_, const BackgroundSchedulePool::TaskFunc & function_)
    : pool_ref(pool_)
    , storage(storage_)
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

bool BackgroundSchedulePoolTaskInfo::execute(BackgroundSchedulePool & pool)
{
    CurrentMetrics::Increment metric_increment(pool.tasks_metric);

    std::lock_guard lock_exec(exec_mutex);

    /// Using this tmp query_id storage to prevent bad_alloc thrown under the try/catch.
    String task_query_id;
    String task_query_id_for_log;

    {
        std::lock_guard lock_schedule(schedule_mutex);

        if (deactivated)
            return false;

        scheduled = false;
        executing = true;

        query_id = fmt::format("{}::{}", toString(pool.thread_name), UUIDHelpers::generateV4());
        task_query_id = query_id;
        task_query_id_for_log = query_id;
    }

    watch.restart();
    UInt16 error_code = 0;
    String exception_message;
    try
    {
        chassert(current_thread); /// Thread from global thread pool
        current_thread->setQueryId(std::move(task_query_id));

        function();

        current_thread->clearQueryId();
    }
    catch (...)
    {
        error_code = static_cast<UInt16>(getCurrentExceptionCode());
        exception_message = getCurrentExceptionMessage(false);
        tryLogCurrentException(__PRETTY_FUNCTION__);
        chassert(false && "Tasks in BackgroundSchedulePool cannot throw");
    }
    UInt64 milliseconds = watch.elapsedMilliseconds();

    /// If the task is executed longer than specified time, it will be logged.
    static constexpr UInt64 slow_execution_threshold_ms = 200;

    if (milliseconds >= slow_execution_threshold_ms)
        LOG_TRACE(getLogger(log_name), "Execution took {} ms.", milliseconds);

    /// Add entry to BackgroundSchedulePoolLog
    try
    {
        if (auto context = Context::getGlobalContextInstance())
        {
            auto background_schedule_pool_log = context->getBackgroundSchedulePoolLog();
            if (background_schedule_pool_log && milliseconds >= background_schedule_pool_log->getDurationMillisecondsThreshold())
            {
                BackgroundSchedulePoolLogElement elem;

                const auto time_now = std::chrono::system_clock::now();
                elem.event_time = timeInSeconds(time_now);
                elem.event_time_microseconds = timeInMicroseconds(time_now);

                elem.query_id = task_query_id_for_log;
                elem.database_name = storage.database_name;
                elem.table_name = storage.table_name;
                elem.table_uuid = storage.uuid;
                elem.log_name = log_name;

                elem.duration_ms = milliseconds;

                elem.error = error_code;
                elem.exception = exception_message;

                background_schedule_pool_log->add(std::move(elem));
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    {
        std::lock_guard lock_schedule(schedule_mutex);

        query_id.clear();
        executing = false;

        /// In case was scheduled while executing (including a scheduleAfter which expired) we schedule the task
        /// on the queue. We don't call the function again here because this way all tasks
        /// will have their chance to execute
        if (scheduled)
        {
            pool.scheduleTask(*this);
            return true;
        }
        else
            return false;
    }
}

bool BackgroundSchedulePoolTaskInfo::scheduleImpl(std::lock_guard<std::mutex> & schedule_mutex_lock) TSA_REQUIRES(schedule_mutex)
{
    if (scheduled)
        return true;

    scheduled = true;

    auto pool_ptr = pool_ref.lock();
    if (!pool_ptr)
        return false;

    /// If the task is not executing at the moment, enqueue it for immediate execution.
    /// But if it is currently executing, do nothing because it will be enqueued
    /// at the end of the execute() method.
    ///
    /// NOTE: scheduleTask must be called before cancelDelayedTask to ensure the task
    /// is always present in at least one of the pool's collections (task_groups,
    /// running_tasks, delayed_tasks). This prevents getTasks() from missing the task
    /// during the transition.
    if (!executing)
    {
        try
        {
            pool_ptr->scheduleTask(*this);
        }
        catch (...)
        {
            /// scheduleTask rolls back its own enqueue state on the failure path; reset `scheduled`
            /// so the caller can retry once the underlying issue is resolved. Otherwise a follow-up
            /// schedule call would short-circuit on the early `if (scheduled) return true;` above
            /// and never re-enqueue the task.
            scheduled = false;
            throw;
        }
    }

    if (delayed)
        pool_ptr->cancelDelayedTask(*this, schedule_mutex_lock);

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

BackgroundSchedulePoolPtr BackgroundSchedulePool::create(size_t size, size_t initial_size, size_t max_parallel_tasks_per_type, CurrentMetrics::Metric tasks_metric, CurrentMetrics::Metric size_metric, ThreadName thread_name)
{
    return std::shared_ptr<BackgroundSchedulePool>(new BackgroundSchedulePool(size, initial_size, max_parallel_tasks_per_type, tasks_metric, size_metric, thread_name));
}

BackgroundSchedulePool::BackgroundSchedulePool(size_t size_, size_t initial_size_, size_t max_parallel_tasks_per_type_, CurrentMetrics::Metric tasks_metric_, CurrentMetrics::Metric size_metric_, ThreadName thread_name_)
    : logger(getLogger(fmt::format("BackgroundSchedulePool/{}", toString(thread_name_))))
    , tasks_metric(tasks_metric_)
    /// The metric reports the configured cap (so `system.server_settings` displays
    /// `background_*_schedule_pool_size` consistently with the user-facing setting),
    /// not the number of currently spawned workers — that is tracked separately by `threads.size()`.
    , size_metric(size_metric_, size_)
    , thread_name(thread_name_)
    , max_parallel_tasks_per_type(max_parallel_tasks_per_type_ ? max_parallel_tasks_per_type_ : size_)
{
    const size_t initial_size = std::min(size_, initial_size_);
    LOG_INFO(logger, "Create BackgroundSchedulePool with {} initial threads (grows lazily up to {})", initial_size, size_);

    try
    {
        {
            std::lock_guard tasks_lock(tasks_mutex);
            max_size = size_;
            threads.reserve(size_);
            for (size_t i = 0; i < initial_size; ++i)
                spawnThreadLocked();
        }

        delayed_thread = std::make_unique<ThreadFromGlobalPoolNoTracingContextPropagation>([this] { delayExecutionThreadFunction(); });
    }
    catch (...)
    {
        LOG_FATAL(
            logger,
            "Couldn't get {} threads from global thread pool: {}",
            initial_size,
            getCurrentExceptionCode() == ErrorCodes::CANNOT_SCHEDULE_TASK
                ? "Not enough threads. Please make sure max_thread_pool_size is considerably "
                  "bigger than background_schedule_pool_size."
                : getCurrentExceptionMessage(/* with_stacktrace */ true));
        abort();
    }
}


void BackgroundSchedulePool::spawnThreadLocked()
{
    /// `emplace_back` may reallocate `threads`. If that reallocation throws (e.g. out of memory)
    /// after the joinable `ThreadFromGlobalPoolNoTracingContextPropagation` temporary has been
    /// constructed — and so the worker thread already started — unwinding destroys that still
    /// joinable temporary, which calls `std::abort`. This would turn a recoverable allocation
    /// failure (e.g. during the best-effort cap increase in `increaseThreadsCount`) into a server
    /// crash. Reserve capacity up front so the `emplace_back` below cannot reallocate while a
    /// joinable temporary is alive; a throwing `reserve` here runs before any thread is started and
    /// propagates safely (the move constructor used during reallocation is `noexcept`).
    if (threads.size() == threads.capacity())
        threads.reserve(threads.size() < max_size ? max_size : threads.size() + 1);

    threads.emplace_back(ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); }));
}


bool BackgroundSchedulePool::runnableTasksExceedLocked(size_t threshold) const
{
    size_t count = 0;
    for (UInt64 task_type : runnable_task_types)
    {
        const auto & group = task_groups.at(task_type);
        /// Invariant: a group present in runnable_task_types has headroom to run at least one
        /// more task (it is removed from the list once num_running reaches the per-type cap).
        chassert(group.num_running < max_parallel_tasks_per_type);
        count += std::min(group.tasks.size(), max_parallel_tasks_per_type - group.num_running);
        if (count > threshold)
            return true;
    }
    return false;
}


void BackgroundSchedulePool::increaseThreadsCount(size_t new_threads_count)
{
    std::lock_guard tasks_lock(tasks_mutex);

    if (shutdown)
        throw Exception(ErrorCodes::ABORTED, "Pool already destroyed");

    if (new_threads_count < max_size)
    {
        LOG_WARNING(logger,
            "Tried to raise the schedule pool cap but the new cap ({}) is not greater than the current one ({})", new_threads_count, max_size);
        return;
    }

    max_size = new_threads_count;
    size_metric.changeTo(new_threads_count);

    /// Consume already-queued demand with the freshly-raised cap. A saturated pool has no parked
    /// workers to wake and may receive no further scheduleTask call for a while, so without spawning
    /// here it would keep executing at the old concurrency until some unrelated future task happens
    /// to trigger lazy growth. Each newly-spawned worker becomes idle and immediately picks up a
    /// runnable task (runnable_task_types is non-empty), so we account for it via `spawned`.
    try
    {
        size_t spawned = 0;
        while (threads.size() < max_size && runnableTasksExceedLocked(idle_threads + spawned))
        {
            spawnThreadLocked();
            ++spawned;
        }
    }
    catch (...)
    {
        /// Spawning is best-effort here: if the global thread pool is momentarily exhausted, leave
        /// the cap raised and let lazy growth in scheduleTask retry later rather than failing the
        /// config reload.
        tryLogCurrentException(logger, "Failed to eagerly spawn schedule pool workers after raising the cap");
    }
}


void BackgroundSchedulePool::join()
{
    try
    {
        shutdown = true;

        Threads threads_to_join;

        /// Unlock threads and steal them out for joining without holding tasks_mutex
        /// (workers re-acquire tasks_mutex on wake-up to observe shutdown and exit).
        {
            std::lock_guard tasks_lock(tasks_mutex);
            tasks_cond_var.notify_all();
            threads_to_join = std::move(threads);
        }
        {
            std::lock_guard tasks_lock(delayed_tasks_mutex);
            delayed_tasks_cond_var.notify_all();
        }

        /// Join all worker threads to avoid any recursive calls to schedule()/scheduleAfter() from the task callbacks
        {
            Stopwatch watch;
            LOG_TRACE(logger, "Waiting for threads to finish.");
            delayed_thread->join();
            delayed_thread.reset();
            for (auto & thread : threads_to_join)
                thread.join();
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
    std::lock_guard tasks_lock(tasks_mutex);
    chassert(threads.empty(), "BackgroundSchedulePool::threads have not been joined");
}


BackgroundSchedulePool::TaskHolder BackgroundSchedulePool::createTask(const StorageID & storage, const std::string & log_name, const TaskFunc & function)
{
    return TaskHolder(std::shared_ptr<TaskInfo>(new TaskInfo(weak_from_this(), storage, log_name, function)));
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
        auto task_ptr = task_info.shared_from_this();
        group.tasks.emplace_back(task_ptr);
        if (!group.runnable_list_pos && group.num_running < max_parallel_tasks_per_type)
        {
            group.runnable_list_pos = runnable_task_types.size();
            runnable_task_types.push_back(task_type);
        }
        running_tasks.erase(task_ptr);

        /// Grow the pool lazily: if there are more runnable tasks than parked workers available to
        /// pick them up, and we have headroom, spawn an extra worker. Demand is the number of tasks
        /// that could run right now — counted per group and bounded by `max_parallel_tasks_per_type`
        /// — not the number of distinct runnable task *types*: a single group (e.g. the same periodic
        /// task across many replicated tables, or many Kafka/ObjectStorageQueue consumers) can run up
        /// to `max_parallel_tasks_per_type` tasks at once, so type count would leave such a burst
        /// stuck at `initial_size`. We compare against `idle_threads` (not `idle_threads == 0`) and,
        /// crucially, each `scheduleTask` enqueues exactly one task and spawns at most one worker, so
        /// parked workers woken by a prior `notify_one` — already "claimed" by previously-enqueued
        /// tasks though they have not yet decremented `idle_threads` — never cause over-spawning.
        if (runnableTasksExceedLocked(idle_threads) && threads.size() < max_size && !shutdown)
        {
            try
            {
                spawnThreadLocked();
                return; /// The new worker will pick up the task; no need to notify an existing one.
            }
            catch (...)
            {
                /// If there are existing workers, fall through to `notify_one` — one of them will
                /// pick up the task once it finishes its current work. But if the pool is still
                /// empty (`background_schedule_pool_initial_size = 0` and the very first spawn
                /// just failed), there is nobody to notify and the queued task would be orphaned.
                /// Fail closed in that case so the caller learns about the failure rather than
                /// silently dropping the work. Roll back the enqueue we did above so the caller's
                /// retry — after `scheduleImpl` resets `scheduled` to false — actually re-enqueues
                /// the task instead of finding a stale entry already in `task_groups`.
                if (threads.empty())
                {
                    chassert(!group.tasks.empty() && group.tasks.back() == task_ptr);
                    group.tasks.pop_back();
                    if (group.tasks.empty() && group.runnable_list_pos)
                    {
                        chassert(*group.runnable_list_pos == runnable_task_types.size() - 1);
                        runnable_task_types.pop_back();
                        group.runnable_list_pos.reset();
                    }
                    throw;
                }
                tryLogCurrentException(logger, "Failed to spawn an additional schedule pool worker");
                /// Fall through to notify_one — an existing worker will eventually take the task.
            }
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
        UInt64 task_type_to_run = 0;
        TaskInfoPtr task;

        current_thread->flushUntrackedMemory();

        {
            UniqueLock tasks_lock(tasks_mutex);

            ++idle_threads;
            /// TSA_NO_THREAD_SAFETY_ANALYSIS because it doesn't understand within the lambda that the
            /// tasks_lock has already locked tasks_mutex.
            tasks_cond_var.wait(tasks_lock.getUnderlyingLock(), [&]() TSA_NO_THREAD_SAFETY_ANALYSIS
            {
                return shutdown || !runnable_task_types.empty();
            });
            --idle_threads;
            if (shutdown)
                break;

            if (runnable_task_types.empty())
                continue;

            task_type_to_run = runnable_task_types[thread_local_rng() % runnable_task_types.size()];
            auto & group = task_groups[task_type_to_run];
            chassert(!group.tasks.empty());
            chassert(group.num_running < max_parallel_tasks_per_type);

            task = group.tasks.front();
            running_tasks.insert(task);
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
            bool scheduled = task->execute(*this);

            UniqueLock tasks_lock(tasks_mutex);
            /// In case it was scheduled, the task will be removed in scheduleTask() from running_tasks
            if (!scheduled)
                running_tasks.erase(task);
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
        {
            try
            {
                task->schedule();
            }
            catch (...)
            {
                /// scheduleTask fails closed (rethrows) when it cannot spawn the very first worker —
                /// this happens only with `background_schedule_pool_initial_size = 0` and an exhausted
                /// global thread pool. Letting the exception escape would terminate this delayed thread,
                /// after which no future scheduleAfter task in this pool could ever be woken. Keep the
                /// thread alive and retry the task with a small backoff instead. The task is still
                /// present in `delayed_tasks` (scheduleImpl rolls back before removing it), so we
                /// re-stamp it rather than leaving a past-due entry that would busy-loop.
                tryLogCurrentException(logger, "Failed to schedule a delayed task; will retry");
                static constexpr size_t delayed_task_retry_ms = 100;
                task->scheduleAfter(delayed_task_retry_ms);
            }
        }
    }
}

std::vector<BackgroundSchedulePool::TaskInfoSnapshot> BackgroundSchedulePool::getTasks()
{
    std::vector<TaskInfoSnapshot> result;

    std::unordered_set<TaskInfoPtr> unique_tasks;

    {
        /// Hold both locks simultaneously to get a consistent snapshot.
        /// In scheduleImpl, a task is first added to task_groups (under tasks_mutex)
        /// and then removed from delayed_tasks (under delayed_tasks_mutex).
        /// By holding both locks, we guarantee that we see the task in at least one
        /// of the collections during such a transition.
        std::lock_guard lock1(tasks_mutex);
        std::lock_guard lock2(delayed_tasks_mutex);

        for (const auto & [task_type, group] : task_groups)
        {
            for (const auto & task : group.tasks)
            {
                unique_tasks.insert(task);
            }
        }

        for (const auto & task : running_tasks)
        {
            unique_tasks.insert(task);
        }

        for (const auto & [timestamp, task] : delayed_tasks)
        {
            unique_tasks.insert(task);
        }
    }

    for (const auto & task : unique_tasks)
    {
        std::lock_guard lock(task->schedule_mutex);
        result.emplace_back(TaskInfoSnapshot{
            .storage = task->storage,
            .log_name = task->log_name,
            .query_id = task->query_id,
            .elapsed_ms = task->executing ? task->watch.elapsedMilliseconds() : 0,
            .deactivated = task->deactivated,
            .scheduled = task->scheduled,
            .delayed = task->delayed,
            .executing = task->executing,
        });
    }

    return result;
}

}
