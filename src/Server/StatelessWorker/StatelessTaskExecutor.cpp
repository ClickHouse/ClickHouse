#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ClientInfo.h>
#include <Parsers/ASTSelectQuery.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Core/Block.h>
#include <Common/SipHash.h>
#include <Common/Stopwatch.h>
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

/// TODO: move
std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context);

StatelessTaskExecutor::StatelessTaskExecutor()
    : thread_pool(
        CurrentMetrics::StatelessWorkerThreads,
        CurrentMetrics::StatelessWorkerThreadsActive,
        CurrentMetrics::StatelessWorkerThreadsScheduled,
        1000, 100, 3000)
{
}

StatelessTaskExecutor::Result StatelessTaskExecutor::startTask(const String & unique_task_id, const DistributedQueryTaskDescription & task_description, const String & unique_temp_file_path)
{
    ContextPtr global_context = Context::getGlobalContextInstance();
    ContextMutablePtr query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    {
        ClientInfo client_info;
        client_info.current_query_id = unique_task_id;
        client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
        client_info.initial_query_id = task_description.initial_query_id;
        query_context->setClientInfo(client_info);
    }

    auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(unique_temp_file_path, query_context);

    std::shared_ptr<std::promise<void>> task_promise = std::make_shared<std::promise<void>>();
    auto task_state = std::make_shared<TaskState>();
    task_state->completion_future = task_promise->get_future();

    {
        std::lock_guard lock(tasks_mutex);
        tasks[unique_task_id] = task_state;
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

    auto task_function = [task_description, object_storage, object_storage_path, query_context, task_promise, is_task_cancelled, update_progress]() mutable
    {
        auto query_scope = CurrentThread::QueryScope::create(query_context);

        Stopwatch start_watch(CLOCK_MONOTONIC);
        ASTSelectQuery ast_stub; /// FIXME: this is only used to populate query_kind
        auto query_plan_hash = sipHash64(task_description.serialized_query_plan);
        auto process_list_entry = query_context->getProcessList().insert(task_description.task.task_id, query_plan_hash, &ast_stub, query_context, start_watch.getStart(), false);
        query_context->setProcessListElement(process_list_entry->getQueryStatus());

        try
        {
            doExecuteTask(task_description, object_storage, object_storage_path, query_context, is_task_cancelled, update_progress);
            task_promise->set_value("");
        }
        catch (std::exception & e)
        {
            tryLogCurrentException(getLogger("StatelessTaskExecutor"),
                fmt::format("Task {} failed", task_description.task.task_id));
            task_promise->set_value(e.what());
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            task_promise->set_value("unknown exception");
        }
    };

    thread_pool.scheduleOrThrow(std::move(task_function));

    return Result::Ok;
}

StatelessTaskExecutor::TaskStatus StatelessTaskExecutor::getStatus(const String & task_id, UInt64 wait_milliseconds)
{
    /// Make a copy of task completion future to wait for it outside of the lock
    std::shared_future<String> completion_future;
    std::shared_ptr<Progress> progress;
    {
        std::lock_guard lock(tasks_mutex);
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return TaskStatus{Result::UnknownTaskId, "", {}};
        completion_future = it->second->completion_future;
        progress = it->second->progress;
    }

    if (completion_future.valid() && completion_future.wait_for(std::chrono::milliseconds(wait_milliseconds)) == std::future_status::timeout)
    {
        Progress progress_delta = progress->fetchAndResetPiecewiseAtomically();
        return TaskStatus{Result::TaskRunnig, "", std::move(progress_delta)};
    }

    Progress progress_delta = progress->fetchAndResetPiecewiseAtomically();
    auto error_message = completion_future.get();
    if (error_message.empty())
        return TaskStatus{Result::TaskFinished, "", std::move(progress_delta)};
    else
        return TaskStatus{Result::TaskFailed, error_message, std::move(progress_delta)};
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
