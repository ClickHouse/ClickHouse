#pragma once

#include <Common/CurrentMetrics.h>
#include <base/types.h>

#include <memory>
#include <unordered_set>

namespace CurrentMetrics
{
    extern const Metric StatelessWorkerQueryControllers;
}

namespace DB
{

/// The tasks of one distributed query on this worker. Guarded by the owner's mutex (`StatelessTaskExecutor::tasks_mutex`).
class WorkerQueryController
{
public:
    explicit WorkerQueryController(String unique_query_id_) : unique_query_id(std::move(unique_query_id_)) {}

    [[nodiscard]] const String & getUniqueQueryId() const { return unique_query_id; }

    void addTask(const String & unique_task_id) { task_ids.insert(unique_task_id); }

    /// Returns true when it was the query's last task on this worker.
    bool removeTask(const String & unique_task_id)
    {
        task_ids.erase(unique_task_id);
        return task_ids.empty();
    }

private:
    const String unique_query_id;
    std::unordered_set<String> task_ids;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::StatelessWorkerQueryControllers};
};

using WorkerQueryControllerPtr = std::shared_ptr<WorkerQueryController>;

}
