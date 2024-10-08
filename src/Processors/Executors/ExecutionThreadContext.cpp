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

                if (read_progress->counters.total_bytes)
                    read_progress_callback->addTotalBytes(read_progress->counters.total_bytes);

                if (!read_progress_callback->onProgress(read_progress->counters.read_rows, read_progress->counters.read_bytes, read_progress->limits))
                    node->processor->cancel();
            }
        }
    }
    catch (Exception exception) /// NOLINT
    {
        /// Copy exception before modifying it because multiple threads can rethrow the same exception
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + node->processor->getName());
        throw exception;
    }
}

bool ExecutionThreadContext::executeTask()
{
    std::unique_ptr<OpenTelemetry::SpanHolder> span;

    if (trace_processors)
    {
        span = std::make_unique<OpenTelemetry::SpanHolder>(node->processor->getName());
        span->addAttribute("thread_number", thread_number);
    }
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
        UInt64 elapsed_ns = execution_time_watch->elapsedNanoseconds();
        node->processor->elapsed_ns += elapsed_ns;
        if (trace_processors)
            span->addAttribute("execution_time_ms", elapsed_ns / 1000U);
    }
#ifndef NDEBUG
    execution_time_ns += execution_time_watch->elapsed();
    if (trace_processors)
        span->addAttribute("execution_time_ns", execution_time_watch->elapsed());
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
