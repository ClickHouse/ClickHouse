#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ClientInfo.h>
#include <Parsers/ASTSelectQuery.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Core/Block.h>
#include <Common/Exception.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/FailPoint.h>
#include <Common/SipHash.h>
#include <Common/QueryScope.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Columns/IColumn.h>
#include <exception>
#include <mutex>

namespace CurrentMetrics
{
    extern const Metric StatelessWorkerThreads;
    extern const Metric StatelessWorkerThreadsActive;
    extern const Metric StatelessWorkerThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace FailPoints
{
    extern const char distributed_plan_delay_root_cause_report[];
    extern const char distributed_plan_delay_failed_task_release[];
    extern const char stateless_worker_pause_task_before_setup[];
}

namespace Setting
{
    extern const SettingsLogsLevel send_logs_level;
    extern const SettingsString send_logs_source_regexp;
    extern const SettingsUInt64 distributed_plan_max_buffered_log_rows;
    extern const SettingsMaxThreads max_threads;
}

/// TODO: move
std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context);

namespace
{

/// The in-flight exception as the initiator reports it. Not `what()`: for a Poco exception that is
/// only the exception's name.
StatelessTaskExecutor::TaskFailure currentTaskFailure()
{
    return {
        getCurrentExceptionCode(),
        getCurrentExceptionMessage(/*with_stacktrace=*/ false, /*check_embedded_stacktrace=*/ false, /*with_extra_info=*/ false, /*with_version=*/ false)};
}

}

void StatelessTaskExecutor::FailureSlot::set(TaskFailure failure_)
{
    std::lock_guard lock(mutex);
    failure = std::move(failure_);
}

std::optional<StatelessTaskExecutor::TaskFailure> StatelessTaskExecutor::FailureSlot::get() const
{
    std::lock_guard lock(mutex);
    return failure;
}

void StatelessTaskExecutor::TaskCancellation::cancel()
{
    std::shared_ptr<QueryStatus> running_query;
    {
        std::lock_guard lock(mutex);
        cancelled = true;
        running_query = query.lock();
    }
    /// The way `KILL QUERY` cancels: it reaches the task's executors at once.
    if (running_query)
        running_query->cancelQuery(CancelReason::CANCELLED_BY_USER);
}

bool StatelessTaskExecutor::TaskCancellation::setQuery(std::weak_ptr<QueryStatus> query_)
{
    std::lock_guard lock(mutex);
    query = std::move(query_);
    return !cancelled;
}

void StatelessTaskExecutor::LogsQueueSlot::set(InternalTextLogsQueuePtr logs_queue_)
{
    std::lock_guard lock(mutex);
    logs_queue = std::move(logs_queue_);
}

InternalTextLogsQueuePtr StatelessTaskExecutor::LogsQueueSlot::get() const
{
    std::lock_guard lock(mutex);
    return logs_queue;
}

StatelessTaskExecutor::StatelessTaskExecutor(size_t max_queries_)
    : max_queries(max_queries_)
    , thread_pool(
        CurrentMetrics::StatelessWorkerThreads,
        CurrentMetrics::StatelessWorkerThreadsActive,
        CurrentMetrics::StatelessWorkerThreadsScheduled,
        GlobalThreadPool::instance().getMaxThreads(), /*max_free_threads=*/ 0, /*queue_size=*/ 0)
{
}

StatelessTaskExecutor::~StatelessTaskExecutor()
{
    /// Before the members are destroyed: a task calls `onTaskEnded` after its promise.
    try
    {
        thread_pool.wait();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("StatelessTaskExecutor"));
    }
}

struct StatelessTaskExecutor::TaskSetup
{
    ContextMutablePtr query_context;
    ObjectStoragePtr object_storage;
    String object_storage_path;
    InternalTextLogsQueuePtr logs_queue;
    LogsLevel client_logs_level = LogsLevel::none;
};

StatelessTaskExecutor::TaskSetup StatelessTaskExecutor::setupTask(const String & unique_task_id, const DistributedQueryTaskDescription & task_description, const String & unique_temp_file_path, const TaskCollectors & collectors, const OpenTelemetry::TracingContext & trace_context)
{
    ContextPtr global_context = Context::getGlobalContextInstance();
    ContextMutablePtr query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    {
        /// Start from the context's client info rather than a default-constructed one:
        /// `makeQueryContext` filled the zero client version with this server's own version,
        /// and a fragment can still read a `Distributed` table, in which case
        /// `RemoteQueryExecutor` refuses to forward an unknown (zero) initiator version.
        ClientInfo client_info = query_context->getClientInfo();
        client_info.current_query_id = unique_task_id;
        client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        client_info.distributed_depth = 1;
        client_info.initial_query_id = task_description.initial_query_id;
        client_info.client_trace_context = trace_context;
        query_context->setClientInfo(client_info);
    }

    /// Apply the initiator's settings so the worker honors query limits and execution-affecting
    /// settings. Limits (e.g. max_rows_to_read, max_rows_in_join) are enforced per task, so each
    /// fragment stays under the limit but the whole query may exceed it by up to the bucket count.
    /// Force make_distributed_plan off: the worker runs an already-split local fragment.
    query_context->applySettingsChanges(task_description.settings_changes);
    query_context->setSetting("make_distributed_plan", false);
    query_context->setSetting("enable_cascades_optimizer", false);

    /// The initiator sends `max_threads` only as the user's cap, which may still exceed this worker's cores.
    if (const UInt64 worker_cores = getNumberOfCPUCoresToUse(); query_context->getSettingsRef()[Setting::max_threads] > worker_cores)
        query_context->setSetting("max_threads", worker_cores);

    TaskSetup setup;
    std::tie(setup.object_storage, setup.object_storage_path) = getObjectStorageForTemporaryFiles(unique_temp_file_path, query_context);

    /// Collect logs only when the coordinator asked for them and `send_logs_level` is not `none`.
    setup.client_logs_level = query_context->getSettingsRef()[Setting::send_logs_level];
    if (collectors.logs && setup.client_logs_level != LogsLevel::none)
    {
        /// Bound the buffer so a stalled or slow status poll cannot grow it without limit; 0 means
        /// unbounded (the default capacity is never reached in practice).
        const UInt64 max_buffered_log_rows = query_context->getSettingsRef()[Setting::distributed_plan_max_buffered_log_rows];
        setup.logs_queue = max_buffered_log_rows != 0
            ? std::make_shared<InternalTextLogsQueue>(max_buffered_log_rows)
            : std::make_shared<InternalTextLogsQueue>(); /// default capacity: effectively unbounded
        setup.logs_queue->max_priority = Poco::Logger::parseLevel(query_context->getSettingsRef()[Setting::send_logs_level].toString());
        setup.logs_queue->setSourceRegexp(query_context->getSettingsRef()[Setting::send_logs_source_regexp]);
    }
    setup.query_context = std::move(query_context);
    return setup;
}

bool StatelessTaskExecutor::admitQuery(const String & unique_query_id)
{
    std::lock_guard lock(tasks_mutex);
    auto controller = query_controllers.find(unique_query_id);
    if (controller == query_controllers.end())
    {
        if (query_controllers.size() >= max_queries)
        {
            LOG_INFO(getLogger("StatelessTaskExecutor"), "Refusing the tasks of {}: {} queries run already", unique_query_id, max_queries);
            return false;
        }
        controller = query_controllers.emplace(unique_query_id, std::make_shared<WorkerQueryController>(unique_query_id)).first;
        LOG_DEBUG(getLogger("StatelessTaskExecutor"), "Created the query controller of {}", unique_query_id);
    }
    controller->second->admit();
    return true;
}

void StatelessTaskExecutor::releaseQuery(const String & unique_query_id)
{
    std::lock_guard lock(tasks_mutex);
    auto controller = query_controllers.find(unique_query_id);
    if (controller == query_controllers.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Releasing the query {}, which has no admission", unique_query_id);
    controller->second->release();
    dropControllerIfIdleLocked(controller);
}

bool StatelessTaskExecutor::startTasks(const String & unique_query_id, DistributedQueryTaskBatch batch, const String & unique_temp_file_path, const TaskCollectors & collectors,
    const StatusChangeNotifierPtr & changes)
{
    const OpenTelemetry::TracingContext & trace_context = OpenTelemetry::CurrentContext();
    size_t task_count = 0;
    for (const auto & stage : batch.stages)
        task_count += stage.tasks.size();
    LOG_DEBUG(getLogger("StatelessTaskExecutor"), "Starting a batch of {} tasks of {}", task_count, unique_query_id);

    if (!admitQuery(unique_query_id))
        return false;
    /// The query keeps its slot after the batch only through the tasks registered here.
    SCOPE_EXIT(releaseQuery(unique_query_id));

    for (auto & stage : batch.stages)
    {
        for (auto & [task, stream_sources] : stage.tasks)
        {
            DistributedQueryTaskDescription task_description;
            task_description.initial_query_id = batch.initial_query_id;
            task_description.serialized_query_plan = stage.serialized_query_plan;
            task_description.exchanges = batch.exchanges;
            task_description.settings_changes = batch.settings_changes;
            task_description.serialization_version = batch.serialization_version;
            task_description.exchange_stream_sources = std::move(stream_sources);
            task_description.task = std::move(task);
            const String unique_task_id = makeUniqueTaskId(unique_query_id, task_description.task.task_id);

            auto task_promise = std::make_shared<std::promise<std::optional<TaskFailure>>>();
            auto task_state = std::make_shared<TaskState>();
            task_state->completion_future = task_promise->get_future();
            task_state->collectors = collectors;
            task_state->unique_query_id = unique_query_id;
            task_state->changes = changes;

            {
                std::lock_guard lock(tasks_mutex);
                if (!tryRegisterTaskLocked(unique_task_id, task_state))
                    continue;
                /// The admission keeps the controller.
                query_controllers.at(unique_query_id)->addTask(unique_task_id);
            }

            scheduleTask(unique_task_id, std::move(task_state), std::move(task_promise), std::move(task_description), unique_temp_file_path, trace_context);
        }
    }
    return true;
}

void StatelessTaskExecutor::scheduleTask(const String & unique_task_id, TaskStatePtr task_state, TaskPromisePtr task_promise,
    DistributedQueryTaskDescription task_description_, const String & unique_temp_file_path, OpenTelemetry::TracingContext trace_context_)
{
    auto update_progress = [task_progress = task_state->progress](const Progress & progress)
    {
        task_progress->incrementPiecewiseAtomically(progress);
    };

    /// The coordinator learns the cause without waiting until the task frees its resources.
    auto report_failure = [failure_slot = task_state->failure, changes = task_state->changes]
    {
        auto failure = currentTaskFailure();
        fiu_do_on(FailPoints::distributed_plan_delay_root_cause_report,
        {
            if (!DistributedQueryCancellation::isConsequence(failure.code))
                sleepForMilliseconds(1000);
        });
        failure_slot->set(std::move(failure));
        changes->notify();
    };

    auto task_function = [this, unique_task_id, task_description = std::move(task_description_), distributed_query_id = unique_temp_file_path,
        collectors = task_state->collectors, trace_context = std::move(trace_context_), logs_queue_slot = task_state->logs_queue,
        cancellation = task_state->cancellation, changes = task_state->changes, task_state, task_promise, update_progress, report_failure] mutable
    {
        /// The promise must be fulfilled strictly after the last log line of this task:
        /// `currentStatus` treats a ready future as a terminal state
        std::optional<TaskFailure> task_failure;
        try
        {
            FailPointInjection::pauseFailPoint(FailPoints::stateless_worker_pause_task_before_setup);
            /// Set up here rather than before the dispatch reply, so the reply waits only for the description.
            /// Setup and QueryScope creation can throw, hence the outer try-catch.
            auto [query_context, object_storage, object_storage_path, logs_queue, client_logs_level]
                = setupTask(unique_task_id, task_description, distributed_query_id, collectors, trace_context);
            logs_queue_slot->set(logs_queue);

            auto query_scope = QueryScope::create(query_context);

            if (logs_queue)
                CurrentThread::attachInternalTextLogsQueue(logs_queue, client_logs_level);
            try
            {
                Stopwatch start_watch(CLOCK_MONOTONIC);
                ASTSelectQuery ast_stub; /// FIXME: this is only used to populate query_kind
                auto query_plan_hash = sipHash64(*task_description.serialized_query_plan);
                /// Process-list insertion can throw (query limit); keep it inside the try.
                auto process_list_entry = query_context->getProcessList().insert(task_description.task.task_id, query_plan_hash, &ast_stub, query_context, start_watch.getStart(), false);
                query_context->setProcessListElement(process_list_entry->getQueryStatus());
                if (!cancellation->setQuery(process_list_entry->getQueryStatus()))
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Task {} was cancelled before it started", task_description.task.task_id);

                /// A dispatched task is by definition not the initiator's in-process execution, so its
                /// exchanges use the streaming/persisted transports rather than in-memory queues.
                doExecuteTask(task_description, object_storage, object_storage_path, distributed_query_id, query_context,
                    /*execute_locally=*/false, /*is_cancelled=*/ {}, update_progress, report_failure);
            }
            catch (...)
            {
                /// Log while still attached to the thread group, so the failure summary reaches
                /// the client's log stream along with the exception context the query machinery
                /// already logged from inside doExecuteTask.
                tryLogCurrentException(getLogger("StatelessTaskExecutor"),
                    fmt::format("Task {} failed", task_description.task.task_id));

                task_failure = currentTaskFailure();
                /// Models a task that frees its resources long after its exchange connections close.
                if (!DistributedQueryCancellation::isConsequence(task_failure->code))
                    fiu_do_on(FailPoints::distributed_plan_delay_failed_task_release, { sleepForMilliseconds(5000); });
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            task_failure = currentTaskFailure();
        }
        task_promise->set_value(task_failure);
        changes->notify();
        onTaskEnded(unique_task_id, task_state);
    };

    try
    {
        thread_pool.scheduleOrThrow(std::move(task_function));
    }
    catch (...)
    {
        /// The pool refused the task (e.g. saturated or shutting down). Drop the
        /// half-published entry so a status request doesn't report "running" forever
        /// for a task no thread is executing, and complete the promise with the
        /// scheduling exception so future waiters fail fast.
        task_promise->set_value(currentTaskFailure());
        std::lock_guard lock(tasks_mutex);
        eraseTaskLocked(unique_task_id);
        throw;
    }
}

namespace
{

/// Drains the queue. `begin_offset` is the number of lines handed to earlier replies; unset when logs are not collected.
std::optional<TaskLogsPayload> drainLogs(const InternalTextLogsQueuePtr & logs_queue, StatelessTaskExecutor::ForwardedLogsCounter & forwarded_logs)
{
    if (!logs_queue)
        return std::nullopt;

    std::lock_guard lock(forwarded_logs.mutex);
    const auto logs = logs_queue->drainAll();
    MutableColumns columns = InternalTextLogsQueue::getSampleColumns();
    for (const auto & log_line : logs)
    {
        for (std::size_t col_id{0}; col_id < columns.size(); ++col_id)
        {
            const auto & col = log_line[col_id];
            columns[col_id]->insertRangeFrom(*col, 0, col->size());
        }
    }

    TaskLogsPayload result;
    result.rows = InternalTextLogsQueue::getSampleBlock();
    result.rows.setColumns(std::move(columns));

    result.begin_offset = forwarded_logs.count;
    forwarded_logs.count += result.rows.rows();
    result.dropped_total = logs_queue->dropped_logs.load(std::memory_order_relaxed);
    return result;
}

}

StatelessTaskExecutor::TaskStatus StatelessTaskExecutor::currentStatus(
    const TaskState & task_state, const std::shared_future<std::optional<TaskFailure>> & completion_future)
{
    /// The task sets its promise after its last log line, so a ready future seen first means every line is drained.
    const bool ended = !completion_future.valid() || completion_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    auto logs = drainLogs(task_state.logs_queue->get(), *task_state.forwarded_logs);
    Progress progress_delta = task_state.progress->fetchAndResetPiecewiseAtomically();

    if (!ended)
    {
        if (auto failure = task_state.failure->get())
            return TaskStatus{Result::TaskFailing, failure->message, std::move(progress_delta), failure->code, std::move(logs), task_state.collectors};
        return TaskStatus{Result::TaskRunnig, "", std::move(progress_delta), 0, std::move(logs), task_state.collectors};
    }

    const auto & failure = completion_future.get();
    if (!failure)
        return TaskStatus{Result::TaskFinished, "", std::move(progress_delta), 0, std::move(logs), task_state.collectors};
    return TaskStatus{Result::TaskFailed, failure->message, std::move(progress_delta), failure->code, std::move(logs), task_state.collectors};
}

StatelessTaskExecutor::StatusWait StatelessTaskExecutor::beginStatusWait(const Strings & task_ids)
{
    StatusWait wait;
    wait.polled.resize(task_ids.size());
    {
        std::lock_guard lock(tasks_mutex);
        for (size_t i = 0; i < task_ids.size(); ++i)
        {
            if (auto it = tasks.find(task_ids[i]); it != tasks.end())
            {
                wait.polled[i].state = it->second;
                wait.polled[i].completion_future = it->second->completion_future;
            }
        }
    }
    for (auto & task : wait.polled)
        task.failing_before = task.state && task.state->failure->get().has_value();
    return wait;
}

bool StatelessTaskExecutor::StatusWait::changed() const
{
    for (const auto & task : polled)
    {
        if (!task.state || !task.completion_future.valid()
            || task.completion_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
            return true;
        if (!task.failing_before && task.state->failure->get())
            return true;
    }
    return false;
}

std::vector<StatelessTaskExecutor::TaskStatus> StatelessTaskExecutor::StatusWait::collect() const
{
    std::vector<TaskStatus> statuses;
    statuses.reserve(polled.size());
    for (const auto & task : polled)
    {
        if (task.state)
            statuses.push_back(currentStatus(*task.state, task.completion_future));
        else
            statuses.push_back(TaskStatus{Result::UnknownTaskId, "", {}, 0, std::nullopt, {}});
    }
    return statuses;
}

void StatelessTaskExecutor::cancelTasks(const Strings & task_ids)
{
    std::lock_guard lock(tasks_mutex);
    for (const auto & task_id : task_ids)
        if (auto it = tasks.find(task_id); it != tasks.end())
            it->second->cancellation->cancel();
}

void StatelessTaskExecutor::forgetTasks(const Strings & task_ids)
{
    std::lock_guard lock(tasks_mutex);
    for (const auto & task_id : task_ids)
        eraseTaskLocked(task_id);
}

void StatelessTaskExecutor::forgetTasksWhenEnded(const Strings & task_ids)
{
    std::lock_guard lock(tasks_mutex);
    for (const auto & task_id : task_ids)
    {
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            continue;
        /// A task that ends after this check sees the flag, as it takes the lock after its promise.
        if (it->second->completion_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
            eraseTaskLocked(task_id);
        else
            it->second->forget_when_ended = true;
    }
}

void StatelessTaskExecutor::onTaskEnded(const String & unique_task_id, const TaskStatePtr & task_state) noexcept
{
    try
    {
        std::lock_guard lock(tasks_mutex);
        auto it = tasks.find(unique_task_id);
        if (it != tasks.end() && it->second == task_state && task_state->forget_when_ended)
            eraseTaskLocked(unique_task_id);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("StatelessTaskExecutor"), "Cannot forget an ended task");
    }
}

bool StatelessTaskExecutor::tryRegisterTaskLocked(const String & unique_task_id, const TaskStatePtr & task_state)
{
    /// `unique_task_id` is unique per task, so a repeated start (e.g. a coordinator retry) is the same
    /// task. Running it twice would double-write exchanges and temp files and orphan the original from
    /// cancel/forget, so treat a duplicate start as a no-op.
    if (!tasks.try_emplace(unique_task_id, task_state).second)
    {
        LOG_WARNING(getLogger("StatelessTaskExecutor"), "Ignoring duplicate start for already running task {}", unique_task_id);
        return false;
    }
    return true;
}

bool StatelessTaskExecutor::eraseTaskLocked(const String & unique_task_id)
{
    auto it = tasks.find(unique_task_id);
    if (it == tasks.end())
        return false;

    auto controller = query_controllers.find(it->second->unique_query_id);
    if (controller != query_controllers.end())
    {
        controller->second->removeTask(unique_task_id);
        dropControllerIfIdleLocked(controller);
    }
    tasks.erase(it);
    return true;
}

void StatelessTaskExecutor::dropControllerIfIdleLocked(std::unordered_map<String, WorkerQueryControllerPtr>::iterator controller)
{
    if (!controller->second->isIdle())
        return;
    LOG_DEBUG(getLogger("StatelessTaskExecutor"), "Dropped the query controller of {}", controller->first);
    query_controllers.erase(controller);
}

bool makeTaskStatusReply(StatelessTaskExecutor::TaskStatus status, DistributedQueryTaskStatus & reply)
{
    reply.progress = std::move(status.progress);
    reply.logs = std::move(status.logs);
    switch (status.result)
    {
        case StatelessTaskExecutor::TaskRunnig:
            reply.status = "Running";
            return true;
        case StatelessTaskExecutor::TaskFailing:
            reply.status = "Failing";
            reply.error_message = status.message;
            reply.error_code = status.error_code;
            return true;
        case StatelessTaskExecutor::TaskFinished:
            reply.status = "Finished";
            return true;
        case StatelessTaskExecutor::TaskCancelled:
            reply.status = "Cancelled";
            return true;
        case StatelessTaskExecutor::TaskFailed:
            reply.status = "Failed";
            reply.error_message = status.message;
            reply.error_code = status.error_code;
            return true;
        case StatelessTaskExecutor::UnknownTaskId:
            /// A gone task is a normal status answer, not a transport error: it has already finished and been reclaimed, or
            /// was never started. The coordinator treats "Unknown task" as terminal.
            reply.status = "Unknown task";
            return true;
        default:
            reply.status = fmt::format("Unexpected task status: {}", static_cast<int>(status.result));
            return false;
    }
}

std::vector<DistributedQueryTaskStatusEntry> makeTaskStatusEntries(const Strings & task_ids, std::vector<StatelessTaskExecutor::TaskStatus> statuses)
{
    if (statuses.size() != task_ids.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} statuses for {} tasks", statuses.size(), task_ids.size());
    std::vector<DistributedQueryTaskStatusEntry> entries(statuses.size());
    for (size_t i = 0; i < statuses.size(); ++i)
    {
        entries[i].task_id = task_ids[i];
        entries[i].collectors = statuses[i].collectors;
        makeTaskStatusReply(std::move(statuses[i]), entries[i].status);
    }
    return entries;
}

void StatelessTaskExecutor::shutdown()
{
    std::lock_guard lock(tasks_mutex);
    for (auto & [task_id, task_state] : tasks)
        task_state->cancellation->cancel();

    for (auto & [task_id, task_state] : tasks)
        task_state->completion_future.wait();
}

}
