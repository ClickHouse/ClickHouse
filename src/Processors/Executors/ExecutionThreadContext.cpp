#include <Processors/Executors/ExecutionThreadContext.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/Stopwatch.h>
#include <Interpreters/OpenTelemetrySpanLog.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXCEEDED;
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
           && exception.code() != ErrorCodes::QUOTA_EXCEEDED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED;
}

static void executeJob(ExecutingGraph::Node * node, ReadProgressCallback * read_progress_callback)
{
    try
    {
        node->processor->work();

        /// Update read progress only for source nodes.
        bool is_source = node->back_edges.empty();

        if (is_source && read_progress_callback)
        {
            if (auto read_progress = node->processor->getReadProgress())
            {
                if (read_progress->counters.total_rows_approx)
                    read_progress_callback->addTotalRowsApprox(read_progress->counters.total_rows_approx);

                if (!read_progress_callback->onProgress(read_progress->counters.read_rows, read_progress->counters.read_bytes, read_progress->limits))
                    node->processor->cancel();
            }
        }
    }
    catch (Exception & exception)
    {
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + node->processor->getName());
        throw;
    }
}

bool ExecutionThreadContext::executeTask()
{
    OpenTelemetrySpanHolder span("ExecutionThreadContext::executeTask() " + node->processor->getName());
    std::optional<Stopwatch> execution_time_watch;

#ifndef NDEBUG
    execution_time_watch.emplace();
#else
    if (profile_processors)
        execution_time_watch.emplace();
#endif

    try
    {
        executeJob(node, read_progress_callback);
        ++node->num_executed_jobs;
    }
    catch (...)
    {
        node->exception = std::current_exception();
    }

    if (profile_processors)
    {
         UInt64 elapsed_microseconds =  execution_time_watch->elapsedMicroseconds();
         node->processor->elapsed_us += elapsed_microseconds;
         span.addAttribute("execution_time_ms", elapsed_microseconds);
     }
#ifndef NDEBUG
    execution_time_ns += execution_time_watch->elapsed();
    span.addAttribute("execution_time_ns", execution_time_watch->elapsed());
#endif

    span.addAttribute("thread_number", thread_number);

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
