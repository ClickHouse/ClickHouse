#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>

#include <algorithm>

#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/noexcept_scope.h>
#include <Common/logger_useful.h>


namespace CurrentMetrics
{
    extern const Metric MergeTreeBackgroundExecutorThreads;
    extern const Metric MergeTreeBackgroundExecutorThreadsActive;
    extern const Metric MergeTreeBackgroundExecutorThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int INVALID_CONFIG_PARAMETER;
}


template <class Queue>
MergeTreeBackgroundExecutor<Queue>::MergeTreeBackgroundExecutor(
    String name_,
    size_t threads_count_,
    size_t max_tasks_count_,
    CurrentMetrics::Metric metric_,
    CurrentMetrics::Metric max_tasks_metric_,
    std::string_view policy)
    : name(name_)
    , threads_count(threads_count_)
    , max_tasks_count(max_tasks_count_)
    , metric(metric_)
    , max_tasks_metric(max_tasks_metric_, 2 * max_tasks_count) // active + pending
    , pool(std::make_unique<ThreadPool>(
          CurrentMetrics::MergeTreeBackgroundExecutorThreads, CurrentMetrics::MergeTreeBackgroundExecutorThreadsActive, CurrentMetrics::MergeTreeBackgroundExecutorThreadsScheduled))
{
    if (max_tasks_count == 0)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Task count for MergeTreeBackgroundExecutor must not be zero");

    pending.setCapacity(max_tasks_count);
    active.set_capacity(max_tasks_count);

    pool->setMaxThreads(std::max(1UL, threads_count));
    pool->setMaxFreeThreads(std::max(1UL, threads_count));
    pool->setQueueSize(std::max(1UL, threads_count));

    for (size_t number = 0; number < threads_count; ++number)
        pool->scheduleOrThrowOnError([this] { threadFunction(); });

    if (!policy.empty())
        pending.updatePolicy(policy);
}

template <class Queue>
MergeTreeBackgroundExecutor<Queue>::~MergeTreeBackgroundExecutor()
{
    wait();
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::wait()
{
    {
        std::lock_guard lock(mutex);
        shutdown = true;
        has_tasks.notify_all();
    }

    pool->wait();
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::increaseThreadsAndMaxTasksCount(size_t new_threads_count, size_t new_max_tasks_count)
{
    std::lock_guard lock(mutex);

    /// Do not throw any exceptions from global pool. Just log a warning and silently return.
    if (new_threads_count < threads_count)
    {
        LOG_WARNING(log, "Loaded new threads count for {}Executor from top level config, but new value ({}) is not greater than current {}", name, new_threads_count, threads_count);
        return;
    }

    if (new_max_tasks_count < max_tasks_count.load(std::memory_order_relaxed))
    {
        LOG_WARNING(log, "Loaded new max tasks count for {}Executor from top level config, but new value ({}) is not greater than current {}", name, new_max_tasks_count, max_tasks_count);
        return;
    }

    LOG_INFO(log, "Loaded new threads count ({}) and max tasks count ({}) for {}Executor", new_threads_count, new_max_tasks_count, name);

    pending.setCapacity(new_max_tasks_count);
    active.set_capacity(new_max_tasks_count);

    pool->setMaxThreads(std::max(1UL, new_threads_count));
    pool->setMaxFreeThreads(std::max(1UL, new_threads_count));
    pool->setQueueSize(std::max(1UL, new_threads_count));

    for (size_t number = threads_count; number < new_threads_count; ++number)
        pool->scheduleOrThrowOnError([this] { threadFunction(); });

    max_tasks_metric.changeTo(2 * new_max_tasks_count); // pending + active
    max_tasks_count.store(new_max_tasks_count, std::memory_order_relaxed);
    threads_count = new_threads_count;
}

template <class Queue>
size_t MergeTreeBackgroundExecutor<Queue>::getMaxThreads() const
{
    std::lock_guard lock(mutex);
    return threads_count;
}

template <class Queue>
size_t MergeTreeBackgroundExecutor<Queue>::getMaxTasksCount() const
{
    return max_tasks_count.load(std::memory_order_relaxed);
}

template <class Queue>
bool MergeTreeBackgroundExecutor<Queue>::trySchedule(ExecutableTaskPtr task)
{
    std::lock_guard lock(mutex);

    if (shutdown)
        return false;

    auto & value = CurrentMetrics::values[metric];
    if (value.load() >= static_cast<int64_t>(max_tasks_count))
        return false;

    pending.push(std::make_shared<TaskRuntimeData>(std::move(task), metric));

    has_tasks.notify_one();
    return true;
}

void printExceptionWithRespectToAbort(LoggerPtr log, const String & query_id)
{
    std::exception_ptr ex = std::current_exception();

    if (ex == nullptr)
        return;

    try
    {
        std::rethrow_exception(ex);
    }
    catch (const TestException &) // NOLINT
    {
        /// Exception from a unit test, ignore it.
    }
    catch (const Exception & e)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            /// Cancelled merging parts is not an error - log normally.
            if (e.code() == ErrorCodes::ABORTED)
                LOG_DEBUG(log, getExceptionMessageAndPattern(e, /* with_stacktrace */ false));
            else
                tryLogCurrentException(log, "Exception while executing background task {" + query_id + "}");
        });
    }
    catch (...)
    {
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            tryLogCurrentException(log, "Exception while executing background task {" + query_id + "}");
        });
    }
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::removeTasksCorrespondingToStorage(StorageID id)
{
    std::vector<TaskRuntimeDataPtr> tasks_to_wait;
    {
        std::lock_guard lock(mutex);

        /// Erase storage related tasks from pending and select active tasks to wait for
        try
        {
            /// An exception context is needed to proper delete write buffers without finalization
            /// See WriteBuffer::~WriteBuffer for more context
            throw std::runtime_error("Storage is about to be deleted. Done pending task as if it was aborted.");
        }
        catch (...)
        {
            pending.remove(id);
        }

        /// Copy items to wait for their completion
        std::copy_if(active.begin(), active.end(), std::back_inserter(tasks_to_wait),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });

        for (auto & item : tasks_to_wait)
            item->is_currently_deleting = true;
    }

    /// Wait for each task to be executed
    for (auto & item : tasks_to_wait)
    {
        item->is_done.wait();
        item.reset();
    }
}

template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::routine(TaskRuntimeDataPtr item)
{
    /// FIXME Review exception-safety of this, remove NOEXCEPT_SCOPE and ALLOW_ALLOCATIONS_IN_SCOPE if possible
    DENY_ALLOCATIONS_IN_SCOPE;

    /// All operations with queues are considered no to do any allocations

    auto erase_from_active = [this](TaskRuntimeDataPtr & item_) TSA_REQUIRES(mutex)
    {
        active.erase(std::remove(active.begin(), active.end(), item_), active.end());
    };

    auto on_task_done = [] (TaskRuntimeDataPtr && item_) TSA_REQUIRES(mutex)
    {
        /// We have to call reset() under a lock, otherwise a race is possible.
        /// Imagine, that task is finally completed (last execution returned false),
        /// we removed the task from both queues, but still have pointer.
        /// The thread that shutdowns storage will scan queues in order to find some tasks to wait for, but will find nothing.
        /// So, the destructor of a task and the destructor of a storage will be executed concurrently.
        NOEXCEPT_SCOPE({
            ALLOW_ALLOCATIONS_IN_SCOPE;
            item_->task.reset();
        });
        item_->is_done.set();
        item_.reset();
    };

    auto on_task_restart = [this](TaskRuntimeDataPtr && item_) TSA_REQUIRES(mutex)
    {
        /// After the `guard` destruction `item` has to be in moved from state
        /// Not to own the object it points to.
        /// Otherwise the destruction of the task won't be ordered with the destruction of the
        /// storage.
        pending.push(std::move(item_));
        has_tasks.notify_one();
    };

    String query_id;

    auto release_task = [this, &erase_from_active, &on_task_done, &query_id](TaskRuntimeDataPtr && item_)
    {
        std::lock_guard guard(mutex);

        erase_from_active(item_);
        has_tasks.notify_one();

        try
        {
            ALLOW_ALLOCATIONS_IN_SCOPE;
            /// In a situation of a lack of memory this method can throw an exception,
            /// because it may interact somehow with BackgroundSchedulePool, which may allocate memory
            /// But it is rather safe, because we have try...catch block here, and another one in ThreadPool.
            item_->task->onCompleted();
        }
        catch (...)
        {
            printExceptionWithRespectToAbort(log, query_id);
        }

        on_task_done(std::move(item_));
    };

    bool need_execute_again = false;

    try
    {
        ALLOW_ALLOCATIONS_IN_SCOPE;
        query_id = item->task->getQueryId();
        need_execute_again = item->task->executeStep();
    }
    catch (...)
    {
        if (item->task->printExecutionException())
            printExceptionWithRespectToAbort(log, query_id);
        /// Release the task with exception context.
        /// An exception context is needed to proper delete write buffers without finalization
        release_task(std::move(item));
        return;
    }

    if (!need_execute_again)
    {
        release_task(std::move(item));
        return;
    }

    {
        std::lock_guard guard(mutex);
        erase_from_active(item);

        if (item->is_currently_deleting)
        {
            try
            {
                ALLOW_ALLOCATIONS_IN_SCOPE;
                /// An exception context is needed to proper delete write buffers without finalization
                throw Exception(ErrorCodes::ABORTED, "Storage is about to be deleted. Done active task as if it was aborted.");
            }
            catch (...)
            {
                printExceptionWithRespectToAbort(log, query_id);
                on_task_done(std::move(item));
                return;
            }
        }

        on_task_restart(std::move(item));
    }
}


template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::threadFunction()
{
    setThreadName(name.c_str());

    DENY_ALLOCATIONS_IN_SCOPE;

    while (true)
    {
        try
        {
            TaskRuntimeDataPtr item;
            {
                std::unique_lock lock(mutex);
                has_tasks.wait(lock, [this]() TSA_REQUIRES(mutex) { return !pending.empty() || shutdown; });

                if (shutdown)
                    break;

                item = std::move(pending.pop());
                active.push_back(item);
            }

            routine(std::move(item));
        }
        catch (...)
        {
            NOEXCEPT_SCOPE({
                ALLOW_ALLOCATIONS_IN_SCOPE;
                tryLogCurrentException(__PRETTY_FUNCTION__);
            });
        }
    }
}


template class MergeTreeBackgroundExecutor<RoundRobinRuntimeQueue>;
template class MergeTreeBackgroundExecutor<PriorityRuntimeQueue>;
template class MergeTreeBackgroundExecutor<DynamicRuntimeQueue>;
}
