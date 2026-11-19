#pragma once
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <Server/StatelessWorker/WorkerQueryController.h>
#include <IO/Progress.h>
#include <Common/ThreadPool.h>
#include <Common/OpenTelemetryTracingContext.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <base/types.h>
#include <base/defines.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

class QueryStatus;

class StatelessTaskExecutor
{
public:
    explicit StatelessTaskExecutor(size_t max_queries_);
    virtual ~StatelessTaskExecutor() = default;

    enum Result
    {
        Ok = 0,
        UnknownTaskId = 1,
        TaskRunnig = 2,
        TaskFinished = 3,
        TaskCancelled = 4,
        TaskFailed = 5,
        /// The task has failed and still frees its resources, e.g. removes its spilled data.
        TaskFailing = 6,
    };

    struct TaskStatus
    {
        Result result;
        String message;
        Progress progress;
        /// Error code of a failed or failing task, 0 otherwise.
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

    /// Ignores a task cancelled before its start arrived.
    Result startTask(const String & unique_task_id, DistributedQueryTaskDescription task, const String & unique_temp_file_path, const TaskCollectors & collectors);
    /// Starts the tasks of a `start_tasks` batch of the query `unique_query_id`, skipping those cancelled before their start or
    /// already started. A task that cannot be scheduled fails the call; the tasks scheduled before it keep running.
    /// Returns false, having started nothing, for a query new on this worker when `max_queries` queries run already.
    bool startTasks(const String & unique_query_id, DistributedQueryTaskBatch batch, const String & unique_temp_file_path, const TaskCollectors & collectors);
    TaskStatus getStatus(const String & task_id, UInt64 wait_milliseconds);
    /// Waits up to `wait_milliseconds` until one of the tasks ends or reports a failure that an earlier reply did not carry,
    /// then returns the status of each, in order.
    std::vector<TaskStatus> getStatuses(const Strings & task_ids, UInt64 wait_milliseconds);
    /// Remembers an unknown id: the coordinator sends the starts of a stage concurrently, so a cancel can overtake a start.
    Result cancelTask(const String & task_id);
    void cancelTasks(const Strings & task_ids);
    Result forgetTask(const String & task_id);
    void forgetTasks(const Strings & task_ids);

    void shutdown();

private:
    static void executeTask();

//    std::atomic<bool> shutdown_called{false};

    /// The failure of a task, known as soon as the task fails and before it frees its resources.
    class FailureSlot
    {
    public:
        void set(TaskFailure failure_);
        std::optional<TaskFailure> get() const;

    private:
        mutable std::mutex mutex;
        std::optional<TaskFailure> failure TSA_GUARDED_BY(mutex);
    };

    /// Cancels a task: the flag covers the time before the task's process-list entry exists, the entry's query the rest.
    class TaskCancellation
    {
    public:
        void cancel();
        /// Publishes the running task's query; false when the task was cancelled before, so it must not run.
        bool setQuery(std::weak_ptr<QueryStatus> query_);

    private:
        std::mutex mutex;
        bool cancelled TSA_GUARDED_BY(mutex) = false;
        std::weak_ptr<QueryStatus> query TSA_GUARDED_BY(mutex);
    };

    /// Not guarded by `tasks_mutex`: the task sets it, and `shutdown` holds that mutex while it waits for the tasks.
    class LogsQueueSlot
    {
    public:
        void set(InternalTextLogsQueuePtr logs_queue_);
        InternalTextLogsQueuePtr get() const;

    private:
        mutable std::mutex mutex;
        InternalTextLogsQueuePtr logs_queue TSA_GUARDED_BY(mutex);
    };

    using TaskPromisePtr = std::shared_ptr<std::promise<std::optional<TaskFailure>>>;

    struct TaskState
    {
        /// Fulfilled when the task ends: with nothing on success, with the failure otherwise.
        std::shared_future<std::optional<TaskFailure>> completion_future;
        std::shared_ptr<FailureSlot> failure = std::make_shared<FailureSlot>();
        std::shared_ptr<TaskCancellation> cancellation = std::make_shared<TaskCancellation>();
        std::shared_ptr<Progress> progress = std::make_shared<Progress>();
        /// What the coordinator asked to collect for this task; decides which payloads a status reply carries.
        TaskCollectors collectors;
        /// Filled by the task's setup only when logs are collected; drained by `getStatus`.
        std::shared_ptr<LogsQueueSlot> logs_queue = std::make_shared<LogsQueueSlot>();
        /// Lines handed to status replies so far.
        std::shared_ptr<ForwardedLogsCounter> forwarded_logs = std::make_shared<ForwardedLogsCounter>();
        /// The query of a task started by `startTasks`, which keys its `WorkerQueryController`; empty for `startTask`.
        String unique_query_id;
    };

    using TaskStatePtr = std::shared_ptr<TaskState>;

    /// The status of a task now; `completion_future` is the caller's copy, since one future object is not shared by threads.
    static TaskStatus currentStatus(const TaskState & task_state, const std::shared_future<std::optional<TaskFailure>> & completion_future);

    /// Wakes `getStatuses` when a task reports its failure or ends. Shared with the tasks, which can end after `shutdown`.
    struct StatusChanges
    {
        std::mutex mutex;
        std::condition_variable changed;

        void notify();
    };

    /// The query context and the rest of what a task needs besides its description.
    struct TaskSetup;

    static TaskSetup setupTask(const String & unique_task_id, const DistributedQueryTaskDescription & task_description,
        const String & unique_temp_file_path, const TaskCollectors & collectors, const OpenTelemetry::TracingContext & trace_context);

    /// Adds a task to `tasks`; false, and nothing added, for a task cancelled before its start or already started.
    bool tryRegisterTaskLocked(const String & unique_task_id, const TaskStatePtr & task_state) TSA_REQUIRES(tasks_mutex);
    /// Removes a task from `tasks`; false for an unknown one.
    bool eraseTaskLocked(const String & unique_task_id) TSA_REQUIRES(tasks_mutex);

    /// Schedules a registered task in `thread_pool`; `trace_context_` is the dispatch request's.
    void scheduleTask(const String & unique_task_id, TaskStatePtr task_state, TaskPromisePtr task_promise,
        DistributedQueryTaskDescription task_description_, const String & unique_temp_file_path, OpenTelemetry::TracingContext trace_context_);

    /// The most queries with tasks on this worker; a query holds its slot while it has a `WorkerQueryController`.
    const size_t max_queries;
    /// A thread for every task, as many as the global thread pool has: a task waiting for its peers holds only its own.
    ThreadPool thread_pool;

    std::unordered_map<String, TaskStatePtr> tasks TSA_GUARDED_BY(tasks_mutex);
    /// Ids cancelled before their start arrived. Bounded: past the cap the oldest is dropped.
    static constexpr size_t MAX_CANCELLED_BEFORE_START = 1000;
    std::unordered_set<String> cancelled_before_start TSA_GUARDED_BY(tasks_mutex);
    std::deque<String> cancelled_before_start_order TSA_GUARDED_BY(tasks_mutex);
    std::unordered_map<String, WorkerQueryControllerPtr> query_controllers TSA_GUARDED_BY(tasks_mutex);
    std::shared_ptr<StatusChanges> status_changes = std::make_shared<StatusChanges>();
    std::mutex tasks_mutex;
};

}
