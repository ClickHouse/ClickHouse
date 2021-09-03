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

        status = active.tryPush(item);
        assert(status);


        bool res = pool.trySchedule([this, item] ()
        {
            setThreadName(name.c_str());

            /// Storage may want to destroy and it needs to finish all task related to it.
            /// But checking causes some interaction with storage methods, for example it calls getStorageID.
            /// So, we must execute this checking once, signal another thread that we are finished and be destroyed.
            /// Not doing any complex stuff, especially interaction with Storage...
            /// Calling this check twice may cause segfault.
            auto check_if_currently_deleting = [&] () -> bool
            {
                active.eraseAll([&] (auto x) { return x == item; });

                for (const auto & id : currently_deleting)
                {
                    if (item->task->getStorageID() == id)
                    {
                        item->promise.set_value();
                        return true;
                    }
                }

                return false;
            };

            bool checked{false};

            SCOPE_EXIT({
                if (checked)
                    return;
                checked = true;
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

        });

        if (!res)
        {
            active.eraseAll([&] (auto x) { return x == item; });
            status = pending.tryPush(item);
            assert(status);
        }

    }
}


}
