#include <Storages/MergeTree/MergeMutateExecutor.h>

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


    for (const auto & item : tasks_to_wait)
    {
        assert(item->future.valid());
        item->future.wait();
    }

    {
        std::lock_guard lock(mutex);
        currently_deleting.erase(id);
    }
}


void MergeTreeBackgroundExecutor::schedulerThreadFunction()
{
    while (true)
    {
        std::unique_lock lock(mutex);

        has_tasks.wait(lock, [this](){ return !pending.empty() || shutdown_suspend; });

        if (shutdown_suspend)
            break;

        ItemPtr item;
        if (!pending.tryPop(&item))
            continue;

        active.tryPush(item);

        try
        {
            /// This is needed to increase / decrease the number of threads at runtime
            if (update_timer.compareAndRestartDeferred(1.))
                updateConfiguration();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }


        bool res = pool.trySchedule([this, item] ()
        {
            setThreadName(name.c_str());

            auto check_if_deleting = [&] () -> bool
            {
                active.eraseAll([&] (auto x) { return x == item; });

                for (auto & id : currently_deleting)
                {
                    if (item->task->getStorageID() == id)
                    {
                        item->promise.set_value();
                        return true;
                    }
                }

                return false;
            };

            SCOPE_EXIT({
                std::lock_guard guard(mutex);
                check_if_deleting();
            });

            try
            {
                if (item->task->execute())
                {
                    std::lock_guard guard(mutex);

                    if (check_if_deleting())
                        return;

                    pending.tryPush(item);
                    has_tasks.notify_one();
                    return;
                }

                item->task->onCompleted();

                std::lock_guard guard(mutex);
                has_tasks.notify_one();
            }
            catch(...)
            {
                item->task->onCompleted();
                std::lock_guard guard(mutex);
                has_tasks.notify_one();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

        });

        if (!res)
        {
            active.eraseAll([&] (auto x) { return x == item; });
            pending.tryPush(item);
        }

    }
}


}
