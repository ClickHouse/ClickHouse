#pragma once
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <IO/Progress.h>
#include <Common/ThreadPool.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/types.h>
#include <base/defines.h>

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{


class StatelessTaskExecutor
{
public:
    StatelessTaskExecutor(size_t max_threads, size_t max_free_threads, size_t queue_size);
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
        Progress progress;
        /// Log lines collected on the worker since the previous status poll
        /// (InternalTextLogsQueue block format); empty when logs are not requested.
        Block logs;
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
        std::shared_ptr<Progress> progress = std::make_shared<Progress>();
        /// Created only when the task's forwarded `send_logs_level` is above `none`;
        /// filled by the task's threads via the thread-group attachment, drained by
        /// status polls in `getStatus`.
        InternalTextLogsQueuePtr logs_queue;
    };

    using TaskStatePtr = std::shared_ptr<TaskState>;

    ThreadPool thread_pool;

    std::unordered_map<String, TaskStatePtr> tasks TSA_GUARDED_BY(tasks_mutex);
    std::mutex tasks_mutex;
};

}
