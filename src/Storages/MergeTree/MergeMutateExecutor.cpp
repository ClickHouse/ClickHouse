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

        std::erase_if(pending, [&] (auto item) -> bool { return item->task->getStorageID() == id; });

        /// Find pending to wait
        for (const auto & item : active)
            if (item->task->getStorageID() == id)
                tasks_to_wait.emplace_back(item);
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

        auto item = std::move(pending.front());
        pending.pop_front();

        active.emplace(item);

        /// This is needed to increase / decrease the number of threads at runtime
        updatePoolConfiguration();

        bool res = pool.trySchedule([this, item] ()
        {
            setThreadName(name.c_str());

            auto check_if_deleting = [&] () -> bool
            {
                active.erase(item);

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

                    pending.emplace_back(item);
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
            active.erase(item);
            pending.emplace_back(item);
        }

    }
}


}
