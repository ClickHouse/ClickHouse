#include <Processors/Executors/Runtime/Executor.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <QueryPipeline/printPipeline.h>
#include <Common/Exception.h>
#include <Common/OpenTelemetryTraceContext.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

bool checkTimeLimit(const QueryStatusPtr & process_list_element)
{
    return !process_list_element || process_list_element->checkTimeLimit();
}

}

Executor::Executor(std::shared_ptr<Processors> processors_, QueryStatusPtr elem, const StepWallClockRegistry * registry_)
    : processors(std::move(processors_))
    , process_list_element(std::move(elem))
    , registry(registry_)
{
    if (process_list_element)
        process_list_element->addExecutor(this);
}

Executor::~Executor()
{
    if (process_list_element)
        process_list_element->removeExecutor(this);
}

void Executor::start(size_t num_threads, bool concurrency_control)
{
    std::lock_guard lock(mutex);

    try
    {
        pipeline.emplace(processors, process_list_element, registry);
        pipeline->read_progress_callback = read_progress_callback.get();

        poller.emplace();
        scheduler.emplace(*poller, num_threads);

        coordinator.emplace(*scheduler, *poller, num_threads);
        pool.emplace(*scheduler, *coordinator, *pipeline, num_threads, concurrency_control);
    }
    catch (Exception & exception)
    {
        WriteBufferFromOwnString buf;
        printPipeline(*processors, buf, /* with_profile = */ false, /* with_addresses = */ true);
        buf.finalize();
        exception.addMessage("Query pipeline:\n" + buf.str());
        throw;
    }

    pushInitialTasks();

    if (cancel_before_start)
        pipeline->cancel(*cancel_before_start);

    if (pipeline->cancelled())
        coordinator->stop();
}

void Executor::pushInitialTasks()
{
    for (auto * sink : pipeline->sinks())
    {
        auto round_lock = sink->lock.lockRound();
        if (sink->lock.status() != ProcessorLock::Status::Idle)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} is locked before the execution started", sink->processor->getName());

        sink->lock.setExecuting();
        scheduler->push(Task{.state = sink, .kind = Task::Kind::Prepare});
    }
}

void Executor::finalize()
{
    if (!checkTimeLimit(process_list_element))
        cancel(IProcessor::CancelReason::CancelledByTimeout);

    if (pipeline->cancelled())
        return;

    if (!pipeline->allFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline stuck. Current state:\n{}", pipeline->dump());

    if (read_progress_callback)
        pipeline->reportReadProgress(*read_progress_callback);
}

void Executor::execute(size_t num_threads, bool concurrency_control)
{
    if (!checkTimeLimit(process_list_element))
        cancel(IProcessor::CancelReason::CancelledByTimeout);

    num_threads = std::max<size_t>(num_threads, 1);

    OpenTelemetry::SpanHolder span("Executor::execute()");
    span.addAttribute("clickhouse.thread_num", num_threads);

    try
    {
        start(num_threads, concurrency_control);
        pool->run();
        pool->stop();

        if (pipeline->exception)
            std::rethrow_exception(pipeline->exception);

        finalize();
    }
    catch (...)
    {
        span.addAttribute(ExecutionStatus::fromCurrentException());
        throw;
    }
}

bool Executor::executeUntil(std::atomic_bool * yield_flag)
{
    if (!pool)
        start(1, true);

    pool->runUntil(yield_flag);

    if (!coordinator->stopped())
        return true;

    pool->stop();

    if (pipeline->exception)
        std::rethrow_exception(pipeline->exception);

    finalize();
    return false;
}

void Executor::cancel(IProcessor::CancelReason reason)
{
    std::lock_guard lock(mutex);

    if (!pipeline)
    {
        if (!cancel_before_start || *cancel_before_start == IProcessor::CancelReason::PartialResult)
            cancel_before_start = reason;
        return;
    }

    pipeline->cancel(reason);
    coordinator->stop();
}

void Executor::cancelReading()
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

void Executor::setReadProgressCallback(ReadProgressCallbackPtr callback)
{
    std::lock_guard lock(mutex);

    read_progress_callback = std::move(callback);
    if (pipeline)
        pipeline->read_progress_callback = read_progress_callback.get();
}

}
