#include <Processors/Executors/Runtime/V2/Pipeline/ProcessorRunner.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/CurrentThread.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Abort.h>
#include <Common/Exception.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>
#include <base/scope_guard.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Runtime::V2
{

void ProcessorRunner::runPrepare(ExecutingProcessor & executing)
{
    pending_inputs.clear();
    pending_outputs.clear();

    {
        auto round_lock = executing.lockRound();
        executing.status.store(ExecutingProcessor::Status::Idle);
        prepareRound(executing, std::move(round_lock));
    }

    size_t processed_inputs = 0;
    size_t processed_outputs = 0;
    while (processed_outputs < pending_outputs.size() || processed_inputs < pending_inputs.size())
    {
        for (; processed_outputs < pending_outputs.size(); ++processed_outputs)
            visitNeighbour(*pending_outputs[processed_outputs]);

        for (; processed_inputs < pending_inputs.size(); ++processed_inputs)
            visitNeighbour(*pending_inputs[processed_inputs]);
    }
}

void ProcessorRunner::runWork(ExecutingProcessor & executing)
{
    IProcessor & processor = *executing.processor;

    std::unique_ptr<OpenTelemetry::SpanHolder> span;
    if (options.trace_processors)
    {
        span = std::make_unique<OpenTelemetry::SpanHolder>(processor.getUniqID());
        span->addAttribute("thread_number", worker_id);
    }

    const auto * step = processor.getQueryPlanStep();
    StepWallClock * clock = nullptr;
    if (options.step_profiler && step)
    {
        auto & cached_clock = processor.query_plan_step_wall_clock_ptr;
        if (!cached_clock)
            cached_clock = options.step_profiler->findClockForStep(step, processor.getQueryPlanStepGroup());

        clock = cached_clock;
        if (clock)
            clock->onEnter();
    }

    std::optional<Stopwatch> execution_time_watch;
    if (options.profile_processors || options.trace_processors || options.step_profiler)
        execution_time_watch.emplace();

    SCOPE_EXIT({
        if (execution_time_watch)
        {
            const UInt64 elapsed_ns = execution_time_watch->elapsedNanoseconds();
            processor.elapsed_ns += elapsed_ns;
            if (span)
                span->addAttribute("execution_time_ms", elapsed_ns / 1000U);
            if (collect_work_intervals)
                work_intervals.emplace_back(execution_time_watch->getStart(), elapsed_ns, step);
        }

        if (clock)
            clock->onLeave();
    });

    if (processor.isSpillable() && CurrentThread::getGroup())
        CurrentThread::getGroup()->memory_spill_scheduler->checkAndSpill(&processor);

    processor.work();
    ++processor.num_executed_jobs;

    const bool is_source = processor.getInputs().empty();
    if (is_source && options.read_progress_callback)
    {
        if (auto progress = processor.getReadProgress())
        {
            if (progress->counters.total_rows_approx)
                options.read_progress_callback->addTotalRowsApprox(progress->counters.total_rows_approx);

            if (progress->counters.total_bytes)
                options.read_progress_callback->addTotalBytes(progress->counters.total_bytes);

            if (!options.read_progress_callback->onProgress(progress->counters.read_rows, progress->counters.read_bytes, progress->limits))
                processor.cancel();
        }
    }
}

void ProcessorRunner::runAsyncReady(ExecutingProcessor & executing)
{
    executing.processor->onAsyncJobReady();
    executing.status.store(ExecutingProcessor::Status::Work);
}

void ProcessorRunner::runUpdatePipeline(ExecutingProcessor & executing)
{
    IProcessor & processor = *executing.processor;
    IProcessor::PipelineUpdate update = processor.updatePipeline();

    /// TODO: move to callers (do that in processor implementations).
    for (const auto & added : update.to_add)
        if (!added->getQueryPlanStep())
            added->inheritQueryPlanStepFromParent(processor, processor.getQueryPlanStepGroup());

    auto updated = pipeline.update(executing, update.to_add, update.to_reconnect);
    if (!update.to_remove.empty())
        pipeline.submitForRemoval(update.to_remove);

    for (auto * updated_executing : updated)
        notifyOwner(*updated_executing);
}

template <class PortT>
void ProcessorRunner::visitNeighbour(PortT & neighbour)
{
    ExecutingProcessor & owner = neighbour.getRoundUpdatesChannel().getOwner();
    if (owner.status.load() == ExecutingProcessor::Status::Finished)
        return;

    auto round_lock = owner.lockRound();
    owner.incoming_updates.push(neighbour);

    switch (owner.status.load())
    {
        case ExecutingProcessor::Status::Idle:
            prepareRound(owner, std::move(round_lock));
            return;
        case ExecutingProcessor::Status::Prepare:
        case ExecutingProcessor::Status::Work:
        case ExecutingProcessor::Status::Async:
        case ExecutingProcessor::Status::AsyncReady:
        case ExecutingProcessor::Status::UpdatePipeline:
            owner.processor->onUpdatePorts();
            return;
        case ExecutingProcessor::Status::Finished:
            return;
    }
}

void ProcessorRunner::notifyOwner(ExecutingProcessor & executing)
{
    auto round_lock = executing.lockRound();
    switch (executing.status.load())
    {
        case ExecutingProcessor::Status::Idle:
            executing.status.store(ExecutingProcessor::Status::Prepare);
            runnable.push_back(&executing);
            return;
        case ExecutingProcessor::Status::Prepare:
        case ExecutingProcessor::Status::Work:
        case ExecutingProcessor::Status::Async:
        case ExecutingProcessor::Status::AsyncReady:
        case ExecutingProcessor::Status::UpdatePipeline:
            executing.processor->onUpdatePorts();
            return;
        case ExecutingProcessor::Status::Finished:
            return;
    }
}

void ProcessorRunner::prepareRound(ExecutingProcessor & executing, std::unique_lock<Wyrd::Mutex>)
{
    IProcessor & processor = *executing.processor;

    executing.incoming_updates.drainLifo(hint_inputs, hint_outputs);
    const auto last_status = executing.last_status;
    const auto new_status = processor.prepare(hint_inputs, hint_outputs);
    executing.last_status = new_status;

    if (options.profile_processors)
        profileWaits(processor, last_status, new_status);

    /// TODO: Fix lifo here.
    executing.round_updates.drainLifo(changed_inputs, changed_outputs);
    for (auto * input : changed_inputs)
        pending_outputs.push_back(&input->getOutputPort());
    for (auto * output : changed_outputs)
        pending_inputs.push_back(&output->getInputPort());

    switch (new_status)
    {
        case IProcessor::Status::NeedData:
        case IProcessor::Status::PortFull:
            return;

        case IProcessor::Status::Finished:
            if (CurrentThread::getGroup())
                CurrentThread::getGroup()->memory_spill_scheduler->remove(&processor);
            executing.status.store(ExecutingProcessor::Status::Finished);
            finished_processors.push_back(&processor);
            return;

        case IProcessor::Status::Ready:
            executing.status.store(ExecutingProcessor::Status::Work);
            runnable.push_back(&executing);
            return;

        case IProcessor::Status::Async:
        {
#if defined(OS_LINUX) || defined(OS_DARWIN)
            auto [fd, events, timeout_ms] = processor.scheduleForEvent();
            executing.status.store(ExecutingProcessor::Status::Async);
            poll_requests.push_back(PollRequest{.processor = &executing, .fd = fd, .events = events, .timeout_ms = timeout_ms});
            return;
#else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Asynchronous processors are not supported on this platform");
#endif
        }

        case IProcessor::Status::UpdatePipeline:
            executing.status.store(ExecutingProcessor::Status::UpdatePipeline);
            runnable.push_back(&executing);
            return;
    }
}

void ProcessorRunner::profileWaits(IProcessor & processor, std::optional<IProcessor::Status> last_status, IProcessor::Status status) const
{
    if (last_status != IProcessor::Status::NeedData && status == IProcessor::Status::NeedData)
        processor.input_wait_watch.restart();
    else if (last_status == IProcessor::Status::NeedData && status != IProcessor::Status::NeedData)
        processor.input_wait_elapsed_ns += processor.input_wait_watch.elapsedNanoseconds();

    if (last_status != IProcessor::Status::PortFull && status == IProcessor::Status::PortFull)
        processor.output_wait_watch.restart();
    else if (last_status == IProcessor::Status::PortFull && status != IProcessor::Status::PortFull)
        processor.output_wait_elapsed_ns += processor.output_wait_watch.elapsedNanoseconds();
}

ProcessorRunner::ProcessorRunner(ExecutingPipeline & pipeline_, const ExecutionOptions & options_, size_t worker_id_)
    : pipeline(pipeline_)
    , options(options_)
    , worker_id(worker_id_)
    , collect_work_intervals(options.step_profiler && options.step_profiler->needCollectWorkIntervals())
{
}

void ProcessorRunner::run(ExecutingProcessor & executing)
{
    runnable.clear();
    poll_requests.clear();
    finished_processors.clear();

    switch (executing.status.load())
    {
        case ExecutingProcessor::Status::Prepare:
            runPrepare(executing);
            return;
        case ExecutingProcessor::Status::Work:
            runWork(executing);
            runPrepare(executing);
            return;
        case ExecutingProcessor::Status::AsyncReady:
            runAsyncReady(executing);
            runWork(executing);
            runPrepare(executing);
            return;
        case ExecutingProcessor::Status::UpdatePipeline:
            runUpdatePipeline(executing);
            runPrepare(executing);
            return;
        case ExecutingProcessor::Status::Idle:
        case ExecutingProcessor::Status::Async:
        case ExecutingProcessor::Status::Finished:
            Wyrd::abort("Processor {} was picked while not scheduled", executing.processor->getName());
    }
}

const std::vector<ExecutingProcessor *> & ProcessorRunner::getRunnable() const
{
    return runnable;
}

const std::vector<PollRequest> & ProcessorRunner::getPollRequests() const
{
    return poll_requests;
}

const std::vector<IProcessor *> & ProcessorRunner::getFinishedProcessors() const
{
    return finished_processors;
}

WorkIntervals ProcessorRunner::extractWorkIntervals()
{
    return std::exchange(work_intervals, {});
}

}

}
