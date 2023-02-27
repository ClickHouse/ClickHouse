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
