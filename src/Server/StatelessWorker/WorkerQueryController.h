#pragma once

#include <Common/CurrentMetrics.h>
#include <base/defines.h>
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
    void removeTask(const String & unique_task_id) { task_ids.erase(unique_task_id); }

    void admit() { ++admissions; }
    void release()
    {
        chassert(admissions > 0);
        --admissions;
    }

    /// Returns true when neither a task nor an admission holds the query's slot on this worker.
    [[nodiscard]] bool isIdle() const { return task_ids.empty() && admissions == 0; }

private:
    const String unique_query_id;
    std::unordered_set<String> task_ids;
    size_t admissions = 0;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::StatelessWorkerQueryControllers};
};

using WorkerQueryControllerPtr = std::shared_ptr<WorkerQueryController>;

}
