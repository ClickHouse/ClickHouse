#include <Processors/Executors/ExecutionThreadContext.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXPIRED;
    extern const int QUERY_WAS_CANCELLED;
}

bool StoppingPipelineTask::executeTask(StoppingPipelineTask::Data & data)
{
    StoppingPipelineTask * desired = this;
    StoppingPipelineTask * expected = nullptr;

    while (!data.task.compare_exchange_strong(expected, desired))
    {
        if (!expected->executeTask(data, true))
            return false;

        expected = nullptr;
    }

    return desired->executeTask(data, true);
}

void StoppingPipelineTask::enterConcurrentReadSection(Data & data)
{
    ++data.num_processing_executors;
    while (auto * task = data.task.load())
        task->executeTask(data, true);
}

void StoppingPipelineTask::exitConcurrentReadSection(Data & data)
{
    --data.num_processing_executors;
    while (auto * task = data.task.load())
        task->executeTask(data, false);
}

bool StoppingPipelineTask::executeTask(StoppingPipelineTask::Data & data, bool on_enter)
{
    std::unique_lock lock(mutex);

    if (on_enter)
        ++num_waiting_processing_threads;

    condvar.wait(lock, [&]()
    {
        return num_waiting_processing_threads >= data.num_processing_executors || data.task != this;
    });

    /// After condvar.wait() task may point to trash. Can change it only if it is still in expand_pipeline_task.
    if (data.task == this)
    {
        result = callback();

        data.task = nullptr;

        lock.unlock();
        condvar.notify_all();
    }

    return result;
}

void ExecutionThreadContext::wait(std::atomic_bool & finished)
{
    std::unique_lock lock(mutex);

    condvar.wait(lock, [&]
    {
        return finished || wake_flag;
    });

    wake_flag = false;
}

void ExecutionThreadContext::wakeUp()
{
    std::lock_guard guard(mutex);
    wake_flag = true;
    condvar.notify_one();
}

static bool checkCanAddAdditionalInfoToException(const DB::Exception & exception)
{
    /// Don't add additional info to limits and quota exceptions, and in case of kill query (to pass tests).
    return exception.code() != ErrorCodes::TOO_MANY_ROWS_OR_BYTES
           && exception.code() != ErrorCodes::QUOTA_EXPIRED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED;
}

static void executeJob(IProcessor * processor)
{
    try
    {
        processor->work();
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + processor->getName());
        throw;
    }
}

bool ExecutionThreadContext::executeTask()
{
#ifndef NDEBUG
    Stopwatch execution_time_watch;
#endif

    try
    {
        executeJob(node->processor);

        ++node->num_executed_jobs;
    }
    catch (...)
    {
        node->exception = std::current_exception();
    }

#ifndef NDEBUG
    execution_time_ns += execution_time_watch.elapsed();
#endif

    return node->exception == nullptr;
}

void ExecutionThreadContext::rethrowExceptionIfHas()
{
    if (exception)
        std::rethrow_exception(exception);
}

ExecutingGraph::Node * ExecutionThreadContext::tryPopAsyncTask()
{
    ExecutingGraph::Node * task = nullptr;

    if (!async_tasks.empty())
    {
        task = async_tasks.front();
        async_tasks.pop();

        if (async_tasks.empty())
            has_async_tasks = false;
    }

    return task;
}

void ExecutionThreadContext::pushAsyncTask(ExecutingGraph::Node * async_task)
{
    async_tasks.push(async_task);
    has_async_tasks = true;
}

}
