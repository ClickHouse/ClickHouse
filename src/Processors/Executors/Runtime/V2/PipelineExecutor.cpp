#include <Processors/Executors/Runtime/V2/PipelineExecutor.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <QueryPipeline/printPipeline.h>
#include <Common/Exception.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/Scheduler/MemoryReservation.h>

namespace DB
{

namespace Runtime::V2
{

namespace
{

bool checkTimeLimit(const QueryStatusPtr & process_list_element)
{
    return !process_list_element || process_list_element->checkTimeLimit();
}

void syncMemoryReservation(const QueryStatusPtr & process_list_element)
{
    if (!process_list_element)
        return;

    auto * reservation = process_list_element->getMemoryReservation();
    auto * tracker = process_list_element->getMemoryTracker();
    if (reservation && tracker)
        reservation->syncWithMemoryTracker(tracker);
}

}

PipelineExecutor::PipelineExecutor(std::shared_ptr<Processors> processors_, QueryStatusPtr elem)
    : processors(std::move(processors_))
    , process_list_element(std::move(elem))
    , options(makeExecutionOptions(process_list_element))
{
    Wyrd::setName(mutex, "executor.mutex");
    if (process_list_element)
        process_list_element->addPipelineExecutor(this);
}

PipelineExecutor::~PipelineExecutor()
{
    if (process_list_element)
        process_list_element->removePipelineExecutor(this);
}

void PipelineExecutor::start(size_t num_threads, bool concurrency_control)
{
    std::lock_guard lock(mutex);

    try
    {
        pipeline.emplace(processors);
        pool.emplace(*pipeline, options, num_threads, concurrency_control);
    }
    catch (Exception & exception)
    {
        WriteBufferFromOwnString buf;
        printPipeline(*processors, buf, /* with_profile = */ false, /* with_addresses = */ true);
        buf.finalize();
        exception.addMessage("Query pipeline:\n" + buf.str());
        throw;
    }

    if (cancel_before_start)
        pipeline->cancel(*cancel_before_start);

    if (pipeline->cancelled())
        pool->stop();
}

void PipelineExecutor::finalize()
{
    if (!checkTimeLimit(process_list_element))
        cancel(IProcessor::CancelReason::CancelledByTimeout);

    switch (pipeline->cancelReason())
    {
        case IProcessor::CancelReason::NotCancelled:
        case IProcessor::CancelReason::PartialResult:
            if (!pipeline->allProcessorsFinished())
                Wyrd::abort("Pipeline stuck. Current state:\n{}", pipeline->dump());
            break;
        case IProcessor::CancelReason::CancelledByTimeout:
            break;
        case IProcessor::CancelReason::Unknown:
        case IProcessor::CancelReason::CancelledByUser:
        case IProcessor::CancelReason::Exception:
            return;
    }

    if (read_progress_callback)
        pipeline->reportReadProgress(*read_progress_callback);

    syncMemoryReservation(process_list_element);
    pipeline->finish();
}

void PipelineExecutor::execute(size_t num_threads, bool concurrency_control)
{
    if (!checkTimeLimit(process_list_element))
        cancel(IProcessor::CancelReason::CancelledByTimeout);

    num_threads = std::max<size_t>(num_threads, 1);

    OpenTelemetry::SpanHolder span("PipelineExecutor::execute()");
    span.addAttribute("clickhouse.thread_num", num_threads);

    try
    {
        start(num_threads, concurrency_control);
        pool->run();
        pool->join();
        pool->rethrowIfFailed();
        finalize();
    }
    catch (...)
    {
        span.addAttribute(ExecutionStatus::fromCurrentException());
        throw;
    }
}

bool PipelineExecutor::executeUntil(std::atomic_bool * yield_flag)
{
    if (!pool)
        start(1, true);

    pool->runUntil(yield_flag);

    if (!pool->isStopped())
        return true;

    pool->join();
    pool->rethrowIfFailed();
    finalize();
    return false;
}

void PipelineExecutor::cancel(IProcessor::CancelReason reason)
{
    std::lock_guard lock(mutex);

    if (!pipeline)
    {
        if (!cancel_before_start || *cancel_before_start == IProcessor::CancelReason::PartialResult)
            cancel_before_start = reason;
        return;
    }

    pipeline->cancel(reason);
    if (pool)
        pool->stop();
}

void PipelineExecutor::cancelReading()
{
    std::lock_guard lock(mutex);

    if (!pipeline)
    {
        if (!cancel_before_start)
            cancel_before_start = IProcessor::CancelReason::PartialResult;
        return;
    }

    pipeline->cancel(IProcessor::CancelReason::PartialResult);
}

void PipelineExecutor::setReadProgressCallback(ReadProgressCallbackPtr callback)
{
    std::lock_guard lock(mutex);

    read_progress_callback = std::move(callback);
    options.read_progress_callback = read_progress_callback.get();
}

void PipelineExecutor::setStepProfiler(StepProfilerPtr step_profiler_)
{
    std::lock_guard lock(mutex);

    step_profiler = std::move(step_profiler_);
    options.step_profiler = step_profiler.get();
}

PoolMetrics PipelineExecutor::getMetrics() const
{
    Wyrd::verify(pool.has_value(), "The metrics of the executor are available only after it has run");
    return pool->getMetrics();
}

}

}
