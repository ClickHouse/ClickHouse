#include <Storages/MergeTree/MergeMutateExecutor.h>


namespace DB
{


void MergeTreeBackgroundExecutor::schedulerThreadFunction()
{
    while (true)
    {
        BackgroundTaskPtr current;
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

                task->onCompleted();

                decrementTasksCount();
            }
            catch(...)
            {
                decrementTasksCount();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        });

        if (!res)
        {
            std::lock_guard guard(mutex);
            tasks.emplace_back(current);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
}


// static MergeTreeBackgroundExecutor merge_mutate_executor;
// static MergeTreeBackgroundExecutor fetch_executor;
// static MergeTreeBackgroundExecutor moves_executor;


}
