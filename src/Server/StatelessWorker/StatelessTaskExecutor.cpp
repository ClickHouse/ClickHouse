#include <Server/StatelessWorker/StatelessTaskExecutor.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Core/Block.h>
#include <mutex>

namespace DB
{

/// TODO: move
std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(const String & unique_temp_file_path, ContextPtr context);


StatelessTaskExecutor::Result StatelessTaskExecutor::startTask(const String & unique_task_id, const String & serialized_query_plan, const DistributedQueryTask & task, const String & unique_temp_file_path)
{
    ContextPtr context = Context::getGlobalContextInstance();
    auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(unique_temp_file_path, context);

    std::shared_ptr<std::promise<void>> task_promise = std::make_shared<std::promise<void>>();
    auto task_state = std::make_shared<TaskState>();
    task_state->completion_future = task_promise->get_future();

    {
        std::lock_guard lock(tasks_mutex);
        tasks[unique_task_id] = task_state;
    }

    auto task_function = [serialized_query_plan, task, object_storage, object_storage_path, context, task_promise]() mutable
    {
        doExecuteTask(serialized_query_plan, task, object_storage, object_storage_path, context);
        task_promise->set_value();
    };

    // TODO: use special pool
    Context::getGlobalContextInstance()->getPrefetchThreadpool().scheduleOrThrow(std::move(task_function));

    return Result::Ok;
}

StatelessTaskExecutor::TaskStatus StatelessTaskExecutor::getStatus(const String & task_id, UInt64 wait_milliseconds)
{
    /// Make a copy of task completion future to wait for it outside of the lock
    std::shared_future<void> completion_future;
    {
        std::lock_guard lock(tasks_mutex);
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return TaskStatus{Result::UnknownTaskId, ""};
        completion_future = it->second->completion_future;
    }

    if (completion_future.valid() && completion_future.wait_for(std::chrono::milliseconds(wait_milliseconds)) == std::future_status::timeout)
        return TaskStatus{Result::TaskRunnig, ""};

    return TaskStatus{Result::TaskFinished, ""};
}

StatelessTaskExecutor::Result StatelessTaskExecutor::cancelTask(const String & task_id)
{
    std::lock_guard lock(tasks_mutex);
    auto it = tasks.find(task_id);
    if (it == tasks.end())
        return Result::UnknownTaskId;
    it->second->cancelled = true;

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
        task_state->cancelled = true;

    for (auto & [task_id, task_state] : tasks)
        task_state->completion_future.wait();
}

}
