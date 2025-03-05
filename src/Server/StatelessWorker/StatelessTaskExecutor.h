#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <base/types.h>
#include <base/defines.h>

namespace DB
{

struct DistributedQueryTask;

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
    };

    struct TaskStatus
    {
        Result result;
        String message;
    };

    Result startTask(const String & serialized_query_plan, const DistributedQueryTask & task);
    TaskStatus getStatus(const String & task_id, UInt64 wait_milliseconds);
    Result cancelTask(const String & task_id);
    Result forgetTask(const String & task_id);

    void shutdown();

private:
    static void executeTask();

//    std::atomic<bool> shutdown_called{false};

    struct TaskState
    {
        std::shared_future<void> completion_future;
        std::atomic<bool> cancelled{false};
    };

    using TaskStatePtr = std::shared_ptr<TaskState>;

    std::unordered_map<String, TaskStatePtr> tasks TSA_GUARDED_BY(tasks_mutex);
    std::mutex tasks_mutex;
};

}
