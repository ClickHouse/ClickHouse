#include <Storages/MergeTree/MergeMutateExecutor.h>

#include <Storages/MergeTree/BackgroundJobsExecutor.h>


namespace DB
{


/// This is a RAII class which only decrements metric.
/// It is added because after all other fixes a bug non-executing merges was occurred again.
/// Last hypothesis: task was successfully added to pool, however, was not executed because of internal exception in it.
class ParanoidMetricDecrementor
{
public:
    explicit ParanoidMetricDecrementor(CurrentMetrics::Metric metric_) : metric(metric_) {}
    void alarm() { is_alarmed = true; }
    void decrement()
    {
        if (is_alarmed.exchange(false))
        {
            CurrentMetrics::values[metric]--;
        }
    }

    ~ParanoidMetricDecrementor() { decrement(); }

private:

    CurrentMetrics::Metric metric;
    std::atomic_bool is_alarmed = false;
};


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
            auto metric_decrementor = std::make_shared<ParanoidMetricDecrementor>(metric);
            metric_decrementor->alarm();

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

                metric_decrementor->decrement();
                task->onCompleted();

                std::lock_guard guard(mutex);
                has_tasks.notify_one();
            }
            catch(...)
            {
                metric_decrementor->decrement();
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
