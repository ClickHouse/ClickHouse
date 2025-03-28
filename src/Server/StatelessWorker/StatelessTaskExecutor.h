#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <base/defines.h>
#include "QueryPipeline/DistributedPlanExecutor.h"

namespace DB
{


class StatelessTaskExecutor
{
public:
    StatelessTaskExecutor() = default;
    virtual ~StatelessTaskExecutor() = default;

    enum Result
    {
        Ok = 0,
        UnknownTaskId = 1,
        TaskRunnig = 2,
        TaskFinished = 3,
        TaskCancelled = 4,
        TaskFailed = 5,
    };

    struct TaskStatus
    {
        Result result;
        String message;
    };

    Result startTask(const String & unique_task_id, const DistributedQueryTaskDescription & task, const String & unique_temp_file_path);
    TaskStatus getStatus(const String & task_id, UInt64 wait_milliseconds);
    Result cancelTask(const String & task_id);
    Result forgetTask(const String & task_id);

    void shutdown();

private:
    static void executeTask();

//    std::atomic<bool> shutdown_called{false};

    struct TaskState
    {
        std::shared_future<String> completion_future;
        std::shared_ptr<std::atomic<bool>> cancelled = std::make_shared<std::atomic<bool>>(false);
    };

    using TaskStatePtr = std::shared_ptr<TaskState>;

    std::unordered_map<String, TaskStatePtr> tasks TSA_GUARDED_BY(tasks_mutex);
    std::mutex tasks_mutex;
};

}
