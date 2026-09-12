#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Processors/Executors/Runtime/ExecutionThreadContext.h>
#include <Interpreters/ProcessList.h>
#include <Processors/ISpillable.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/StepWallClock.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <base/defines.h>
#include <base/numeric.h>
#include <Common/Logger.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/Stopwatch.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event MemoryReservationSpilledBytes;
    extern const Event MemoryReservationSpillingMicroseconds;
}

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

static void executeJob(ExecutingGraph::Node * node, ReadProgressCallback * read_progress_callback)
{
    try
    {
        const auto & processor = node->processor();

        processor->work();

        if (auto * spillable = processor->getSpillable())
        {
            auto memory = spillable->getMemoryStats();
            QueryStatusPtr process_list_element = read_progress_callback ? read_progress_callback->getProcessListElement() : nullptr;
            auto * reservation = process_list_element ? process_list_element->getMemoryReservation() : nullptr;
            if (reservation)
            {
                reservation->updateReclaimable(spillable, memory.spillable_memory_bytes);
                if (memory.spillable_memory_bytes > 0)
                {
                    if (auto spill_request = reservation->takeSpillRequest(spillable, memory.spillable_memory_bytes))
                    {
                        auto * memory_tracker = process_list_element->getMemoryTracker();
                        const auto & logger = getLogger("Scheduler");

                        LOG_TRACE(logger, "Spilling {}, of {} (tracked {})",
                            formatReadableSizeWithBinarySuffix(spill_request),
                            formatReadableSizeWithBinarySuffix(memory.spillable_memory_bytes),
                            formatReadableSizeWithBinarySuffix(memory_tracker->get()));

                        Stopwatch watch;
                        size_t spilled = spillable->spill(spill_request);
                        auto new_spillable_memory_bytes = spillable->getMemoryStats().spillable_memory_bytes;
                        reservation->finishSpill(spillable, spill_request, new_spillable_memory_bytes, memory_tracker);

                        LOG_TRACE(logger, "Spilled {}, remaining {}, tracked {} (took {} ms)",
                            formatReadableSizeWithBinarySuffix(spilled),
                            formatReadableSizeWithBinarySuffix(new_spillable_memory_bytes),
                            formatReadableSizeWithBinarySuffix(memory_tracker->get()),
                            watch.elapsedMilliseconds());
                        ProfileEvents::increment(ProfileEvents::MemoryReservationSpilledBytes, spilled);
                        ProfileEvents::increment(ProfileEvents::MemoryReservationSpillingMicroseconds, watch.elapsedMicroseconds());
                    }
                }
            }
            else if (memory.spillable_memory_bytes > 0 && CurrentThread::getGroup())
                CurrentThread::getGroup()->memory_spill_scheduler->checkAndSpill(spillable);
        }

        /// Update read progress only for source nodes.
        bool is_source = node->back_edges.empty();

        if (is_source && read_progress_callback)
        {
            if (auto read_progress = node->processor()->getReadProgress())
            {
                if (read_progress->counters.total_rows_approx)
                    read_progress_callback->addTotalRowsApprox(read_progress->counters.total_rows_approx);

                if (read_progress->counters.total_bytes)
                    read_progress_callback->addTotalBytes(read_progress->counters.total_bytes);

                if (!read_progress_callback->onProgress(read_progress->counters.read_rows, read_progress->counters.read_bytes, read_progress->limits))
                    node->processor()->cancel();
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

    const size_t group = node->processor()->getQueryPlanStepGroup();

    StepWallClock * clock = nullptr;
    if (step_to_wall_clock_registry)
    {
        /// Some processors are pipeline "plumbing" (resize, converting, output format, etc.)
        /// and are not attributed to any query plan step, so there is no clock for them.
        if (const auto * step = node->processor()->getQueryPlanStep())
        {
            auto & cached_clock = node->processor()->query_plan_step_wall_clock_ptr;
            /// We will search in the registry only initially or when the group of the processor changed
            if (!cached_clock)
                cached_clock = step_to_wall_clock_registry->find(step, group);

            clock = cached_clock;
            chassert(clock);
            if (clock)
                clock->onEnter();
        }
    }

#ifndef NDEBUG
    execution_time_watch.emplace();
#else
    if (profile_processors || step_to_wall_clock_registry)
        execution_time_watch.emplace();
#endif

    bool success = true;
    try
    {
        executeJob(node, read_progress_callback);
        ++node->processor()->num_executed_jobs;
    }
    catch (...)
    {
        setException(std::current_exception());
        success = false;
    }

    if (profile_processors || step_to_wall_clock_registry)
    {
        UInt64 elapsed_ns = execution_time_watch->elapsedNanoseconds();
        node->processor()->elapsed_ns += elapsed_ns;
        if (trace_processors)
            span->addAttribute("execution_time_ms", elapsed_ns / 1000U);
    }

    if (clock)
    {
        clock->onLeave();
    }

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

}
