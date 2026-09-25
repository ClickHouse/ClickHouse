#pragma once
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <IO/Progress.h>
#include <Common/ThreadPool.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/types.h>
#include <base/defines.h>

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
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
        /// Error code of a failed task, 0 otherwise.
        int error_code = 0;
        /// Log lines drained on this poll together with the loss counters; set only when the task
        /// collects logs (see `startTask`).
        std::optional<TaskLogsPayload> logs;
        /// What the coordinator asked to collect at `start`; empty for an unknown task id.
        TaskCollectors collectors;
    };

    /// The error a task ended with.
    struct TaskFailure
    {
        int code = 0;
        String message;
    };

    /// Draining the queue and advancing the counter happen under one lock, so concurrent polls (a retried
    /// request overlapping the original one) get offsets in the same order as the lines they drained.
    struct ForwardedLogsCounter
    {
        std::mutex mutex;
        UInt64 count TSA_GUARDED_BY(mutex) = 0;
    };

    Result startTask(const String & unique_task_id, const DistributedQueryTaskDescription & task, const String & unique_temp_file_path, const TaskCollectors & collectors);
    TaskStatus getStatus(const String & task_id, UInt64 wait_milliseconds);
    Result cancelTask(const String & task_id);
    Result forgetTask(const String & task_id);

    void shutdown();

private:
    static void executeTask();

//    std::atomic<bool> shutdown_called{false};

    struct TaskState
    {
        /// Fulfilled when the task ends: with nothing on success, with the failure otherwise.
        std::shared_future<std::optional<TaskFailure>> completion_future;
        std::shared_ptr<std::atomic<bool>> cancelled = std::make_shared<std::atomic<bool>>(false);
        std::shared_ptr<Progress> progress = std::make_shared<Progress>();
        /// What the coordinator asked to collect for this task; decides which payloads a status reply carries.
        TaskCollectors collectors;
        /// Created only when the coordinator asked for logs and the task's forwarded `send_logs_level`
        /// is above `none`; filled by the task's threads via the thread-group attachment, drained by
        /// status polls in `getStatus`.
        InternalTextLogsQueuePtr logs_queue;
        /// Cumulative log lines drained into status replies; lets the coordinator detect lines lost to a
        /// retried status poll (the worker cannot observe that loss itself).
        std::shared_ptr<ForwardedLogsCounter> forwarded_logs = std::make_shared<ForwardedLogsCounter>();
    };

    using TaskStatePtr = std::shared_ptr<TaskState>;

    ThreadPool thread_pool;

    std::unordered_map<String, TaskStatePtr> tasks TSA_GUARDED_BY(tasks_mutex);
    std::mutex tasks_mutex;
};

}
