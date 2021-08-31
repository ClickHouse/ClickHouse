#include <Storages/MergeTree/MergeMutateExecutor.h>

#include <Storages/MergeTree/BackgroundJobsExecutor.h>


namespace DB
{

void MergeTreeBackgroundExecutor::removeTasksCorrespondingToStorage(StorageID id)
{
    std::lock_guard remove_lock(remove_mutex);

    /// First stop the scheduler thread
    {
        std::unique_lock lock(mutex);
        shutdown_suspend = true;
        has_tasks.notify_one();
    }

    scheduler.join();

    /// Remove tasks
    {
        std::lock_guard lock(currently_executing_mutex);

        for (auto & [task, future] : currently_executing)
        {
            if (task->getStorageID() == id)
                future.wait();
        }

        /// Remove tasks from original queue
        size_t erased_count = std::erase_if(tasks, [id = std::move(id)] (auto task) -> bool { return task->getStorageID() == id; });
        CurrentMetrics::sub(metric, erased_count);
    }

    shutdown_suspend = false;
    scheduler = ThreadFromGlobalPool([this]() { schedulerThreadFunction(); });
}


void MergeTreeBackgroundExecutor::schedulerThreadFunction()
{
    while (true)
    {
        ExecutableTaskPtr current;
        auto current_promise = std::make_shared<std::promise<void>>();
        {
            std::unique_lock lock(mutex);
            has_tasks.wait(lock, [this](){ return !tasks.empty() || shutdown_suspend; });

            if (shutdown_suspend)
                break;

            current = std::move(tasks.front());
            tasks.pop_front();

            /// This is needed to increase / decrease the number of threads at runtime
            updatePoolConfiguration();
        }

        {
            std::lock_guard lock(currently_executing_mutex);
            currently_executing.emplace(current, current_promise->get_future());
        }

        bool res = pool.trySchedule([this, task = current, promise = current_promise] () mutable
        {
            auto on_exit = [&] ()
            {
                promise->set_value();
                {
                    std::lock_guard lock(currently_executing_mutex);
                    currently_executing.erase(task);
                }
            };

            SCOPE_EXIT({ on_exit(); });

            try
            {
                bool result = task->execute();

                if (result)
                {
                    std::lock_guard guard(mutex);
                    tasks.emplace_back(task);
                    has_tasks.notify_one();
                    return;
                }

                decrementTasksCount();
                task->onCompleted();

                std::lock_guard guard(mutex);
                has_tasks.notify_one();
            }
            catch(...)
            {
                decrementTasksCount();
                task->onCompleted();
                std::lock_guard guard(mutex);
                has_tasks.notify_one();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        });

        if (!res)
        {
            std::lock_guard guard(mutex);
            tasks.emplace_back(current);
        }
    }
}


}
