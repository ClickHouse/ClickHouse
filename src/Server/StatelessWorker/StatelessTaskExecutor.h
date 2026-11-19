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

#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
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
        /// collects logs (see `startTasks`).
        std::optional<TaskLogsPayload> logs;
        /// What the coordinator asked to collect at the start; empty for an unknown task id.
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

    /// Woken when a task it was given at the start ends or reports its failure. Shared with the tasks, which can end after
    /// `shutdown`.
    class StatusChangeNotifier
    {
    public:
        virtual ~StatusChangeNotifier() = default;
        virtual void notify() = 0;
    };
    using StatusChangeNotifierPtr = std::shared_ptr<StatusChangeNotifier>;

    /// The tasks of one status request, taken when it arrives.
    class StatusWait;

    /// Takes a `max_queries` slot for the query, or shares the one it holds; false when it holds none and none is free.
    /// Every admission is ended by `releaseQuery`.
    bool admitQuery(const String & unique_query_id);
    /// Ends an admission; the query keeps its slot while its tasks are registered here.
    void releaseQuery(const String & unique_query_id);

    /// Starts the tasks of a `StartTasks` batch of the query `unique_query_id`, skipping those already started. A task that
    /// cannot be scheduled fails the call; the tasks scheduled before it keep running. Returns false, having started nothing,
    /// when `admitQuery` refuses the query. The tasks wake `changes`.
    bool startTasks(const String & unique_query_id, DistributedQueryTaskBatch batch, const String & unique_temp_file_path, const TaskCollectors & collectors,
        const StatusChangeNotifierPtr & changes);
    StatusWait beginStatusWait(const Strings & task_ids);
    /// Ignores unknown ids.
    void cancelTasks(const Strings & task_ids);
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
        /// Filled by the task's setup only when logs are collected; drained by `StatusWait::collect`.
        std::shared_ptr<LogsQueueSlot> logs_queue = std::make_shared<LogsQueueSlot>();
        /// Lines handed to status replies so far.
        std::shared_ptr<ForwardedLogsCounter> forwarded_logs = std::make_shared<ForwardedLogsCounter>();
        /// Keys the query's `WorkerQueryController`.
        String unique_query_id;
        StatusChangeNotifierPtr changes;
    };

    using TaskStatePtr = std::shared_ptr<TaskState>;

    /// The status of a task now; `completion_future` is the caller's copy, since one future object is not shared by threads.
    static TaskStatus currentStatus(const TaskState & task_state, const std::shared_future<std::optional<TaskFailure>> & completion_future);

    /// The query context and the rest of what a task needs besides its description.
    struct TaskSetup;

    static TaskSetup setupTask(const String & unique_task_id, const DistributedQueryTaskDescription & task_description,
        const String & unique_temp_file_path, const TaskCollectors & collectors, const OpenTelemetry::TracingContext & trace_context);

    /// Adds a task to `tasks`; false, and nothing added, for a task already started.
    bool tryRegisterTaskLocked(const String & unique_task_id, const TaskStatePtr & task_state) TSA_REQUIRES(tasks_mutex);
    /// Removes a task from `tasks`; false for an unknown one.
    bool eraseTaskLocked(const String & unique_task_id) TSA_REQUIRES(tasks_mutex);
    void dropControllerIfIdleLocked(std::unordered_map<String, WorkerQueryControllerPtr>::iterator controller) TSA_REQUIRES(tasks_mutex);

    /// Schedules a registered task in `thread_pool`; `trace_context_` is the dispatch request's.
    void scheduleTask(const String & unique_task_id, TaskStatePtr task_state, TaskPromisePtr task_promise,
        DistributedQueryTaskDescription task_description_, const String & unique_temp_file_path, OpenTelemetry::TracingContext trace_context_);

    /// The most queries with tasks on this worker; a query holds its slot while it has a `WorkerQueryController`.
    const size_t max_queries;
    /// A thread for every task, as many as the global thread pool has: a task waiting for its peers holds only its own.
    ThreadPool thread_pool;

    std::unordered_map<String, TaskStatePtr> tasks TSA_GUARDED_BY(tasks_mutex);
    std::unordered_map<String, WorkerQueryControllerPtr> query_controllers TSA_GUARDED_BY(tasks_mutex);
    std::mutex tasks_mutex;
};

class StatelessTaskExecutor::StatusWait
{
public:
    /// Returns true once a task has ended or is unknown, or has reported a failure it had not reported when the wait began.
    [[nodiscard]] bool changed() const;
    /// Returns the status of each task, in the order of the request, and drains their logs and progress.
    [[nodiscard]] std::vector<TaskStatus> collect() const;

private:
    friend class StatelessTaskExecutor;

    struct Polled
    {
        /// Null for an unknown task.
        TaskStatePtr state;
        std::shared_future<std::optional<TaskFailure>> completion_future;
        /// Its failure was reported before the wait began, so an earlier reply carried it.
        bool failing_before = false;
    };
    std::vector<Polled> polled;
};

/// Fills the reply for one task; false when the executor returned a result that is no task status.
bool makeTaskStatusReply(StatelessTaskExecutor::TaskStatus status, DistributedQueryTaskStatus & reply);
/// Returns the entries of a `get_statuses` reply, `statuses` being those of `task_ids` in the same order.
std::vector<DistributedQueryTaskStatusEntry> makeTaskStatusEntries(const Strings & task_ids, std::vector<StatelessTaskExecutor::TaskStatus> statuses);

}
