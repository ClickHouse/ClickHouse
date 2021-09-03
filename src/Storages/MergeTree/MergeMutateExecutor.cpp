#include <Storages/MergeTree/MergeMutateExecutor.h>

#include <Common/setThreadName.h>
#include <Storages/MergeTree/BackgroundJobsExecutor.h>


namespace DB
{


String MergeTreeBackgroundExecutor::toString(Type type)
{
    switch (type)
    {
        case Type::MERGE_MUTATE:
            return "MergeMutate";
        case Type::FETCH:
            return "Fetch";
        case Type::MOVE:
            return "Move";
    }
}


void MergeTreeBackgroundExecutor::updateConfiguration()
{
    auto new_threads_count = threads_count_getter();
    auto new_max_tasks_count = max_task_count_getter();

    try
    {
        pending.set_capacity(new_max_tasks_count);
        active.set_capacity(new_max_tasks_count);

        pool.setMaxFreeThreads(new_threads_count);
        pool.setMaxThreads(new_threads_count);
        pool.setQueueSize(new_max_tasks_count);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    threads_count = new_threads_count;
    max_tasks_count = new_max_tasks_count;
}


void MergeTreeBackgroundExecutor::wait()
{
    {
        std::lock_guard lock(mutex);
        shutdown = true;
        has_tasks.notify_all();
    }

    if (scheduler.joinable())
        scheduler.join();

    pool.wait();
}


bool MergeTreeBackgroundExecutor::trySchedule(ExecutableTaskPtr task)
{
    std::lock_guard lock(mutex);

    if (shutdown)
        return false;

    try
    {
        /// This is needed to increase / decrease the number of threads at runtime
        if (update_timer.compareAndRestartDeferred(10.))
            updateConfiguration();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    auto & value = CurrentMetrics::values[metric];
    if (value.load() >= static_cast<int64_t>(max_tasks_count))
        return false;

    /// Just check if the main scheduler thread in excellent condition
    if (!scheduler.joinable())
    {
        LOG_ERROR(&Poco::Logger::get("MergeTreeBackgroundExecutor"), "Scheduler thread is dead. Trying to alive..");
        scheduler = ThreadFromGlobalPool([this]() { schedulerThreadFunction(); });

        if (!scheduler.joinable())
            LOG_FATAL(&Poco::Logger::get("MergeTreeBackgroundExecutor"), "Scheduler thread is dead permanently. Restart is needed");
    }


    pending.push_back(std::make_shared<Item>(std::move(task), metric));

    has_tasks.notify_one();
    return true;
}


void MergeTreeBackgroundExecutor::removeTasksCorrespondingToStorage(StorageID id)
{
    /// Executor is global, so protect from any concurrent storage shutdowns
    std::lock_guard remove_lock(remove_mutex);

    std::vector<ItemPtr> tasks_to_wait;
    {
        std::lock_guard lock(mutex);

        /// Mark this StorageID as deleting
        currently_deleting.emplace(id);

        /// Erase storage related tasks from pending and select active tasks to wait for
        auto it = std::remove_if(pending.begin(), pending.end(),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; } );
        pending.erase(it, pending.end());

        /// Copy items to wait for their completion
        std::copy_if(active.begin(), active.end(), std::back_inserter(tasks_to_wait),
            [&] (auto item) -> bool { return item->task->getStorageID() == id; });
    }


    for (auto & item : tasks_to_wait)
        item->is_done.wait();

    {
        std::lock_guard lock(mutex);

        for (auto & item : tasks_to_wait)
        {
            assert(item.use_count() == 1);
            item.reset();
        }

        currently_deleting.erase(id);
    }
}


void MergeTreeBackgroundExecutor::routine(ItemPtr item)
{
    setThreadName(name.c_str());

    auto erase_from_active = [&]
    {
        DENY_ALLOCATIONS_IN_SCOPE;
        active.erase(std::remove(active.begin(), active.end(), item), active.end());
    };

    try
    {
        if (item->task->execute())
        {
            DENY_ALLOCATIONS_IN_SCOPE;
            std::lock_guard guard(mutex);

            if (currently_deleting.contains(item->task->getStorageID()))
            {
                erase_from_active();
                return;
            }

            pending.push_back(item);
            erase_from_active();
            item->is_done.reset();
            has_tasks.notify_one();
            return;
        }

        std::lock_guard guard(mutex);
        erase_from_active();
        has_tasks.notify_one();
        /// In a situation of a lack of memory this method can throw an exception,
        /// because it may interact somehow with BackgroundSchedulePool, which may allocate memory
        /// But it is rather safe, because we have try...catch block here, and another one in ThreadPool.
        item->task->onCompleted();
    }
    catch(...)
    {
        std::lock_guard guard(mutex);
        erase_from_active();
        has_tasks.notify_one();
        tryLogCurrentException(__PRETTY_FUNCTION__);
        /// Do not want any exceptions
        try { item->task->onCompleted(); } catch (...) {}
    }
}


void MergeTreeBackgroundExecutor::schedulerThreadFunction()
{
    DENY_ALLOCATIONS_IN_SCOPE;

    while (true)
    {
        std::unique_lock lock(mutex);

        has_tasks.wait(lock, [this](){ return !pending.empty() || shutdown; });

        if (shutdown)
            break;

        ItemPtr item = std::move(pending.front());
        pending.pop_front();

        /// Execute a piece of task
        bool res = pool.trySchedule([this, item]
        {
            routine(item);
            /// When storage shutdowns it will wait until all related background tasks
            /// are finished, because they may want to interact with its fields
            /// and this will cause segfault.
            item->is_done.set();
        });

        if (!res)
        {
            active.erase(std::remove(active.begin(), active.end(), item), active.end());
            pending.push_back(item);
            continue;
        }

        active.push_back(std::move(item));
    }
}


}
