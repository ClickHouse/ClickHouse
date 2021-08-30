#include <Storages/MergeTree/MergeMutateExecutor.h>

#include <Storages/MergeTree/BackgroundJobsExecutor.h>


namespace DB
{


void MergeTreeBackgroundExecutor::schedulerThreadFunction()
{
    while (true)
    {
        ExecutableTaskPtr current;
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

        bool res = pool.trySchedule([this, task = current] ()
        {
            try
            {
                if (task->execute())
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
