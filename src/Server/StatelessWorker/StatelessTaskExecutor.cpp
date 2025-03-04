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
std::pair<ObjectStoragePtr, String> getObjectStorageForTemporaryFiles(ContextPtr context);

StatelessTaskExecutor::Result StatelessTaskExecutor::startTask(const String & serialized_query_plan, const DistributedQueryTask & task)
{
    {
        ContextPtr context = Context::getGlobalContextInstance();

        auto [object_storage, object_storage_path] = getObjectStorageForTemporaryFiles(context);


        /// TODO: start the task asynchronously

        doExecuteTask(serialized_query_plan, task, object_storage, object_storage_path, context);
    }


    std::lock_guard lock(tasks_mutex);

    auto task_state = std::make_shared<TaskState>();
    task_state->completion_future = std::future<void>();
    tasks[task.task_id] = task_state;

    /// TODO: start task in pool
    (void)serialized_query_plan;

    return Result::Ok;
}

StatelessTaskExecutor::TaskStatus StatelessTaskExecutor::getStatus(const String & task_id)
{
    TaskStatePtr task_state;
    {
        std::lock_guard lock(tasks_mutex);
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return TaskStatus{Result::UnknownTaskId, ""};
        task_state = it->second;
    }

    if (task_state->completion_future.valid() && task_state->completion_future.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout)
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
