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


void MergeTreeBackgroundExecutor::removeTasksCorrespondingToStorage(StorageID id)
{
    std::lock_guard remove_lock(remove_mutex);

    std::vector<ItemPtr> tasks_to_wait;
    {
        std::lock_guard lock(mutex);

        /// Mark this StorageID as deleting
        currently_deleting.emplace(id);

        /// Erase storage related tasks from pending and select active tasks to wait for
        pending.eraseAll([&] (auto item) -> bool { return item->task->getStorageID() == id; });
        tasks_to_wait = active.getAll([&] (auto item) -> bool { return item->task->getStorageID() == id; });
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

    bool checked{false};

    auto check_if_currently_deleting = [&] ()
    {
        checked = true;
        return active.eraseAll([&] (auto & x) { return x == item; });
    };


    SCOPE_EXIT({
        if (checked)
            return;
        std::lock_guard guard(mutex);
        check_if_currently_deleting();
    });

    try
    {
        if (item->task->execute())
        {
            std::lock_guard guard(mutex);

            if (check_if_currently_deleting())
                return;

            pending.tryPush(item);
            item->is_done.reset();
            has_tasks.notify_one();
            return;
        }

        /// In a situation of a lack of memory this method can throw an exception,
        /// because it may interact somehow with BackgroundSchedulePool, which may allocate memory
        /// But it is rather safe, because we have try...catch block here, and another one in ThreadPool.
        item->task->onCompleted();

        std::lock_guard guard(mutex);
        has_tasks.notify_one();
    }
    catch(...)
    {
        std::lock_guard guard(mutex);
        has_tasks.notify_one();
        try
        {
            item->task->onCompleted();
        }
        catch (...) {}
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void MergeTreeBackgroundExecutor::schedulerThreadFunction()
{
    DENY_ALLOCATIONS_IN_SCOPE;

    bool status;

    while (true)
    {
        std::unique_lock lock(mutex);

        has_tasks.wait(lock, [this](){ return !pending.empty() || shutdown_suspend; });

        if (shutdown_suspend)
            break;

        ItemPtr item;
        if (!pending.tryPop(&item))
            continue;

        bool res = pool.trySchedule([this, item]
        {
            routine(item);
            item->is_done.set();
        });

        if (!res)
        {
            active.eraseAll([&] (auto x) { return x == item; });
            status = pending.tryPush(item);
            assert(status);
            continue;
        }

        status = active.tryPush(std::move(item));
        assert(status);
    }
}


}
