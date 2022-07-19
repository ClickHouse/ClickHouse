#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>

#include <algorithm>

#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}


template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::wait()
{
    {
        std::lock_guard lock(mutex);
        shutdown = true;
        has_tasks.notify_all();
    }

    pool.wait();
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


template <class Queue>
void MergeTreeBackgroundExecutor<Queue>::removeTasksCorrespondingToStorage(StorageID id)
{
    std::vector<TaskRuntimeDataPtr> tasks_to_wait;
    {
        std::lock_guard lock(mutex);

        /// Erase storage related tasks from pending and select active tasks to wait for
        pending.remove(id);

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
    DENY_ALLOCATIONS_IN_SCOPE;

    /// All operations with queues are considered no to do any allocations

    auto erase_from_active = [this, item]
    {
        active.erase(std::remove(active.begin(), active.end(), item), active.end());
    };

    bool need_execute_again = false;

    try
    {
        ALLOW_ALLOCATIONS_IN_SCOPE;
        need_execute_again = item->task->executeStep();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ABORTED)    /// Cancelled merging parts is not an error - log as info.
            LOG_INFO(log, fmt::runtime(getCurrentExceptionMessage(false)));
        else
            tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (need_execute_again)
    {
        std::lock_guard guard(mutex);

        if (item->is_currently_deleting)
        {
            erase_from_active();

            /// This is significant to order the destructors.
            item->task.reset();
            item->is_done.set();
            item = nullptr;
            return;
        }

        /// After the `guard` destruction `item` has to be in moved from state
        /// Not to own the object it points to.
        /// Otherwise the destruction of the task won't be ordered with the destruction of the
        /// storage.
        pending.push(std::move(item));
        erase_from_active();
        has_tasks.notify_one();
        item = nullptr;
        return;
    }

    {
        std::lock_guard guard(mutex);
        erase_from_active();
        has_tasks.notify_one();

        try
        {
            ALLOW_ALLOCATIONS_IN_SCOPE;
            /// In a situation of a lack of memory this method can throw an exception,
            /// because it may interact somehow with BackgroundSchedulePool, which may allocate memory
            /// But it is rather safe, because we have try...catch block here, and another one in ThreadPool.
            item->task->onCompleted();
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::ABORTED)    /// Cancelled merging parts is not an error - log as info.
                LOG_INFO(log, fmt::runtime(getCurrentExceptionMessage(false)));
            else
                tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        /// We have to call reset() under a lock, otherwise a race is possible.
        /// Imagine, that task is finally completed (last execution returned false),
        /// we removed the task from both queues, but still have pointer.
        /// The thread that shutdowns storage will scan queues in order to find some tasks to wait for, but will find nothing.
        /// So, the destructor of a task and the destructor of a storage will be executed concurrently.
        item->task.reset();
        item->is_done.set();
        item = nullptr;
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
                has_tasks.wait(lock, [this](){ return !pending.empty() || shutdown; });

                if (shutdown)
                    break;

                item = std::move(pending.pop());
                active.push_back(item);
            }

            routine(std::move(item));
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


template class MergeTreeBackgroundExecutor<MergeMutateRuntimeQueue>;
template class MergeTreeBackgroundExecutor<OrdinaryRuntimeQueue>;

}
