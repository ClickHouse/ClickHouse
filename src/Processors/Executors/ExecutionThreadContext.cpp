#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Processors/Executors/ExecutionThreadContext.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/Stopwatch.h>
#include <Common/VariableContext.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
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
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED
           && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT;
}

static std::optional<Int64> getCurrentThreadMemoryUsage()
{
    auto * memory_tracker = CurrentThread::getMemoryTracker();
    if (!memory_tracker)
        return std::nullopt;

    Int64 result = memory_tracker->get();
    /// Do not flush untracked memory here. Keep this profiling path read-only and count only memory
    /// that would be charged to the query-thread tracker when eventually flushed.
    if (current_thread && current_thread->untracked_memory_blocker_level == VariableContext::Max)
        result += current_thread->untracked_memory;

    return result;
}

void ExecutionThreadContext::executeJob()
{
    try
    {
        auto & processor = *node->processor();

        if (processor.isSpillable())
        {
            if (auto group = CurrentThread::getGroup())
                group->memory_spill_scheduler->checkAndSpill(node->processor());
        }

        std::optional<Int64> memory_usage_before;
        if (profile_processors)
            memory_usage_before = getCurrentThreadMemoryUsage();

        auto add_memory_usage = [&]
        {
            if (!memory_usage_before)
                return;

            if (auto memory_usage_after = getCurrentThreadMemoryUsage())
                processor.memory_usage_delta += *memory_usage_after - *memory_usage_before;
        };

        try
        {
            processor.work();
        }
        catch (...)
        {
            add_memory_usage();
            throw;
        }

        add_memory_usage();

        /// Update read progress only for source nodes.
        bool is_source = node->back_edges.empty();

        if (is_source && read_progress_callback)
        {
            if (auto read_progress = processor.getReadProgress())
            {
                if (read_progress->counters.total_rows_approx)
                    read_progress_callback->addTotalRowsApprox(read_progress->counters.total_rows_approx);

                if (read_progress->counters.total_bytes)
                    read_progress_callback->addTotalBytes(read_progress->counters.total_bytes);

                if (!read_progress_callback->onProgress(read_progress->counters.read_rows, read_progress->counters.read_bytes, read_progress->limits))
                    processor.cancel();
            }
        }
    }
    catch (Exception exception) /// NOLINT
    {
        /// Copy exception before modifying it because multiple threads can rethrow the same exception
        if (checkCanAddAdditionalInfoToException(exception))
            exception.addMessage("While executing " + node->processor()->getName());
        throw exception;
    }
}

bool ExecutionThreadContext::executeTask()
{
    std::unique_ptr<OpenTelemetry::SpanHolder> span;

    if (trace_processors)
    {
        span = std::make_unique<OpenTelemetry::SpanHolder>(node->processor()->getUniqID());
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
        executeJob();
        ++node->num_executed_jobs;
    }
    catch (...)
    {
        node->exception = std::current_exception();
    }

    if (profile_processors)
    {
        UInt64 elapsed_ns = execution_time_watch->elapsedNanoseconds();
        node->processor()->elapsed_ns += elapsed_ns;
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

}
