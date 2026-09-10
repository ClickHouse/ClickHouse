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
#include <Common/FailPoint.h>
#include <Common/SipHash.h>
#include <Common/QueryScope.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <base/sleep.h>
#include <Common/CurrentThread.h>
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

namespace FailPoints
{
    extern const char distributed_plan_delay_root_cause_report[];
}

namespace Setting
{
    extern const SettingsLogsLevel send_logs_level;
    extern const SettingsString send_logs_source_regexp;
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

StatelessTaskExecutor::StatelessTaskExecutor(size_t max_threads, size_t max_free_threads, size_t queue_size)
    : thread_pool(
        CurrentMetrics::StatelessWorkerThreads,
        CurrentMetrics::StatelessWorkerThreadsActive,
        CurrentMetrics::StatelessWorkerThreadsScheduled,
        max_threads, max_free_threads, queue_size)
{
}

StatelessTaskExecutor::Result StatelessTaskExecutor::startTask(const String & unique_task_id, const DistributedQueryTaskDescription & task_description, const String & unique_temp_file_path)
{
    /// `unique_task_id` is unique per task, so a repeated start (e.g. a coordinator retry) is the same
    /// task. Running it twice would double-write exchanges and temp files and orphan the original from
    /// cancel/forget, so treat a duplicate start as a no-op.
    {
        std::lock_guard lock(tasks_mutex);
        if (tasks.contains(unique_task_id))
        {
            LOG_WARNING(getLogger("StatelessTaskExecutor"), "Ignoring duplicate start for already running task {}", unique_task_id);
            return Result::Ok;
        }
    }

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
        client_info.initial_query_id = task_description.initial_query_id;
        query_context->setClientInfo(client_info);
    }

    /// Apply the initiator's settings so the worker honors query limits and execution-affecting
    /// settings. Limits (e.g. max_rows_to_read, max_rows_in_join) are enforced per task, so each
    /// fragment stays under the limit but the whole query may exceed it by up to the bucket count.
    /// Force make_distributed_plan off: the worker runs an already-split local fragment.
    query_context->applySettingsChanges(task_description.settings_changes);
    query_context->setSetting("make_distributed_plan", false);
    query_context->setSetting("enable_cascades_optimizer", false);

    auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(unique_temp_file_path, query_context);

    auto task_promise = std::make_shared<std::promise<std::optional<TaskFailure>>>();
    auto task_state = std::make_shared<TaskState>();
    task_state->completion_future = task_promise->get_future();

    /// Collect this task's logs for forwarding to the coordinator when the initiator asked for them
    const LogsLevel client_logs_level = query_context->getSettingsRef()[Setting::send_logs_level];
    if (client_logs_level != LogsLevel::none)
    {
        task_state->logs_queue = std::make_shared<InternalTextLogsQueue>();
        task_state->logs_queue->max_priority = Poco::Logger::parseLevel(query_context->getSettingsRef()[Setting::send_logs_level].toString());
        task_state->logs_queue->setSourceRegexp(query_context->getSettingsRef()[Setting::send_logs_source_regexp]);
    }

    {
        std::lock_guard lock(tasks_mutex);
        /// If two starts of the same id race, keep the first; overwriting would orphan the running task.
        if (!tasks.try_emplace(unique_task_id, task_state).second)
        {
            LOG_WARNING(getLogger("StatelessTaskExecutor"), "Ignoring duplicate start for already running task {}", unique_task_id);
            return Result::Ok;
        }
    }

    /// Callback for periodic cancellation check
    auto is_task_cancelled = [cancelled = task_state->cancelled]() -> bool
    {
        return *cancelled;
    };

    auto update_progress = [task_progress = task_state->progress](const Progress & progress)
    {
        task_progress->incrementPiecewiseAtomically(progress);
    };

    auto task_function = [task_description, object_storage, object_storage_path, distributed_query_id = unique_temp_file_path, query_context, task_promise, is_task_cancelled, update_progress,
        logs_queue = task_state->logs_queue, client_logs_level] mutable
    {
        /// The promise must be fulfilled strictly after the last log line of this task:
        /// `getStatus` treats a ready future as a terminal state
        std::optional<TaskFailure> task_failure;
        try
        {
            /// QueryScope creation can throw, hence outer try-catch
            auto query_scope = QueryScope::create(query_context);

            if (logs_queue)
                CurrentThread::attachInternalTextLogsQueue(logs_queue, client_logs_level);
            try
            {
                Stopwatch start_watch(CLOCK_MONOTONIC);
                ASTSelectQuery ast_stub; /// FIXME: this is only used to populate query_kind
                auto query_plan_hash = sipHash64(task_description.serialized_query_plan);
                /// Process-list insertion can throw (query limit); keep it inside the try.
                auto process_list_entry = query_context->getProcessList().insert(task_description.task.task_id, query_plan_hash, &ast_stub, query_context, start_watch.getStart(), false);
                query_context->setProcessListElement(process_list_entry->getQueryStatus());

                /// A dispatched task is by definition not the initiator's in-process execution, so its
                /// exchanges use the streaming/persisted transports rather than in-memory queues.
                doExecuteTask(task_description, object_storage, object_storage_path, distributed_query_id, query_context,
                    /*execute_locally=*/false, is_task_cancelled, update_progress);
            }
            catch (...)
            {
                /// Log while still attached to the thread group, so the failure summary reaches
                /// the client's log stream along with the exception context the query machinery
                /// already logged from inside doExecuteTask.
                tryLogCurrentException(getLogger("StatelessTaskExecutor"),
                    fmt::format("Task {} failed", task_description.task.task_id));

                task_failure = currentTaskFailure();
                fiu_do_on(FailPoints::distributed_plan_delay_root_cause_report,
                {
                    if (!DistributedQueryCancellation::isConsequence(task_failure->code))
                        sleepForMilliseconds(1000);
                });
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            task_failure = currentTaskFailure();
        }
        task_promise->set_value(task_failure);
    };

    try
    {
        thread_pool.scheduleOrThrow(std::move(task_function));
    }
    catch (...)
    {
        /// The pool refused the task (e.g. saturated or shutting down). Drop the
        /// half-published entry so `get_status` doesn't report "running" forever
        /// for a task no thread is executing, and complete the promise with the
        /// scheduling exception so future waiters fail fast.
        task_promise->set_value(currentTaskFailure());
        std::lock_guard lock(tasks_mutex);
        tasks.erase(unique_task_id);
        throw;
    }

    return Result::Ok;
}

namespace
{

// Pops everything held in log currently
Block drainLogs(const InternalTextLogsQueuePtr & logs_queue)
{
    if (!logs_queue)
        return {};

    const auto logs = logs_queue->drainAll();
    MutableColumns columns = InternalTextLogsQueue::getSampleColumns();
    for (const auto &log_line: logs)
    {
        for (std::size_t col_id{0}; col_id < columns.size(); ++col_id)
        {
            const auto &col = log_line[col_id];
            columns[col_id]->insertRangeFrom(*col, 0, col->size());
        }
    }

    Block block = InternalTextLogsQueue::getSampleBlock();
    block.setColumns(std::move(columns));
    return block;
}

}

StatelessTaskExecutor::TaskStatus StatelessTaskExecutor::getStatus(const String & task_id, UInt64 wait_milliseconds)
{
    /// Make a copy of task completion future to wait for it outside of the lock
    std::shared_future<std::optional<TaskFailure>> completion_future;
    std::shared_ptr<Progress> progress;
    InternalTextLogsQueuePtr logs_queue;
    {
        std::lock_guard lock(tasks_mutex);
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return TaskStatus{Result::UnknownTaskId, "", {}, {}, {}};
        completion_future = it->second->completion_future;
        progress = it->second->progress;
        logs_queue = it->second->logs_queue;
    }

    if (completion_future.valid() && completion_future.wait_for(std::chrono::milliseconds(wait_milliseconds)) == std::future_status::timeout)
    {
        Progress progress_delta = progress->fetchAndResetPiecewiseAtomically();
        return TaskStatus{Result::TaskRunnig, "", std::move(progress_delta), 0, drainLogs(logs_queue)};
    }

    auto error_message = completion_future.get();
    /// Drain only after the future is ready
    Block logs = drainLogs(logs_queue);
    Progress progress_delta = progress->fetchAndResetPiecewiseAtomically();
    const auto & failure = completion_future.get();

    if (!failure)
        return TaskStatus{Result::TaskFinished, "", std::move(progress_delta), {}, std::move(logs)};
    else
        return TaskStatus{Result::TaskFailed, failure->message, std::move(progress_delta), failure->code, std::move(logs)};
}

StatelessTaskExecutor::Result StatelessTaskExecutor::cancelTask(const String & task_id)
{
    std::lock_guard lock(tasks_mutex);
    auto it = tasks.find(task_id);
    if (it == tasks.end())
        return Result::UnknownTaskId;
    *it->second->cancelled = true;

    return Result::Ok;
}

StatelessTaskExecutor::Result StatelessTaskExecutor::forgetTask(const String & task_id)
{
    std::lock_guard lock(tasks_mutex);
    auto it = tasks.find(task_id);
    if (it == tasks.end())
        return Result::UnknownTaskId;

    tasks.erase(it);
    return Result::Ok;
}

void StatelessTaskExecutor::shutdown()
{
    std::lock_guard lock(tasks_mutex);
    for (auto & [task_id, task_state] : tasks)
        *task_state->cancelled = true;

    for (auto & [task_id, task_state] : tasks)
        task_state->completion_future.wait();
}

}
