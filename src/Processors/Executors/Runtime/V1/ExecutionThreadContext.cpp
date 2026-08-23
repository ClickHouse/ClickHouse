#include <ctime>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Processors/Executors/Runtime/V1/ExecutionThreadContext.h>
#include <Processors/IProcessor.h>
#include <Processors/ISpillable.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <base/types.h>
#include <base/defines.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

namespace Runtime::V1
{

ExecutionThreadContext::ExecutionThreadContext(size_t thread_number_, bool profile_processors_, bool trace_processors_, ReadProgressCallback * callback, StepProfiler * step_profiler_)
    : read_progress_callback(callback)
    , step_profiler(step_profiler_)
    , thread_number(thread_number_)
    , profile_processors(profile_processors_)
    , trace_processors(trace_processors_)
    , collect_work_intervals(step_profiler && step_profiler->needCollectWorkIntervals())
{
    if (collect_work_intervals)
        work_intervals.reserve(1024ul);
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

static void executeJob(IProcessor & processor, ReadProgressCallback * read_progress_callback)
{
    try
    {
        if (auto * spillable = processor.getSpillable(); spillable && CurrentThread::getGroup())
            CurrentThread::getGroup()->memory_spill_scheduler->checkAndSpill(spillable);

        processor.work();

        /// Update read progress only for source nodes.
        bool is_source = processor.getInputs().empty();

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
    catch (Exception & exception)
    {
        /// The same exception can be rethrown by several threads, so it must not be modified in
        /// place: copy it before adding anything. The copy slices the exception to `Exception`, so
        /// rethrow the original when there is nothing to add - the callers which recognize an
        /// exception of their own by its type, such as `StorageURLSource::generate`, then still can.
        if (!checkCanAddAdditionalInfoToException(exception))
            throw;

        Exception annotated = exception; /// NOLINT
        annotated.addMessage("While executing " + processor.getName());
        throw annotated; /// NOLINT
    }
}

bool ExecutionThreadContext::executeTask()
{
    std::unique_ptr<OpenTelemetry::SpanHolder> span;

    if (trace_processors)
    {
        span = std::make_unique<OpenTelemetry::SpanHolder>(processor->getUniqID());
        span->addAttribute("thread_number", thread_number);
    }

    std::optional<Stopwatch> execution_time_watch;

    const size_t group = processor->getQueryPlanStepGroup();

    /// Some processors are pipeline "plumbing" (resize, converting, output format, etc.)
    /// and are not attributed to any query plan step, so there is no clock for them.
    const auto * step = processor->getQueryPlanStep();

    StepWallClock * clock = nullptr;
    if (step_profiler && step)
    {
        auto & cached_clock = processor->query_plan_step_wall_clock_ptr;
        if (!cached_clock)
            cached_clock = step_profiler->findClockForStep(step, group);

        clock = cached_clock;
        if (clock)
            clock->onEnter();
    }

#ifndef NDEBUG
    execution_time_watch.emplace();
#else
    if (profile_processors || step_profiler)
        execution_time_watch.emplace();
#endif

    bool success = true;
    try
    {
        executeJob(*processor, read_progress_callback);
        ++processor->num_executed_jobs;
    }
    catch (...)
    {
        setException(std::current_exception());
        success = false;
    }

    UInt64 elapsed_ns = 0;

    if (profile_processors || step_profiler)
    {
        elapsed_ns = execution_time_watch->elapsedNanoseconds();
        processor->elapsed_ns += elapsed_ns;
        if (trace_processors)
            span->addAttribute("execution_time_ms", elapsed_ns / 1000U);
    }

    if (clock)
        clock->onLeave();

    if (collect_work_intervals)
        work_intervals.emplace_back(execution_time_watch->getStart(), elapsed_ns, step);

#ifndef NDEBUG
    execution_time_ns += execution_time_watch->elapsed();
    if (trace_processors)
        span->addAttribute("execution_time_ns", execution_time_watch->elapsed());
#endif
    return success;
}

void ExecutionThreadContext::setException(std::exception_ptr exception_)
{
    if (!exception)
        exception = std::move(exception_);
}

std::exception_ptr ExecutionThreadContext::getException()
{
    return exception;
}

void ExecutionThreadContext::rethrowExceptionIfHas()
{
    if (exception)
        std::rethrow_exception(exception);
}

void ExecutionThreadContext::flushWorkIntervals()
{
    if (step_profiler)
        step_profiler->addWorkIntervals(std::move(work_intervals));
}

}

}
