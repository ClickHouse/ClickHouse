#include <Processors/Executors/Runtime/V2/Pipeline/ProcessorRunner.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ProcessorRunOrder.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/CurrentThread.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Clock.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>
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

void ProcessorRunner::runPrepare(ExecutingProcessor & picked)
{
    pending_inputs.clear();
    pending_outputs.clear();

    {
        auto round_lock = picked.lockRound();
        picked.status.store(ExecutingProcessor::Status::Idle);
        prepareRound(picked, std::move(round_lock));
    }

    while (!pending_outputs.empty() || !pending_inputs.empty())
    {
        while (!pending_outputs.empty())
        {
            OutputPort * upstream = pending_outputs.back();
            pending_outputs.pop_back();
            visitNeighbour(*upstream);
        }

        while (!pending_inputs.empty())
        {
            InputPort * downstream = pending_inputs.back();
            pending_inputs.pop_back();
            visitNeighbour(*downstream);
        }
    }
}

void ProcessorRunner::runWork(ExecutingProcessor & picked)
{
    IProcessor & processor = *picked.processor;

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

    const UInt64 work_started_ns = Wyrd::monotonicNanoseconds();

    SCOPE_EXIT({
        const UInt64 elapsed_ns = Wyrd::monotonicNanoseconds() - work_started_ns;
        picked.work_ns = picked.work_ns == 0 ? elapsed_ns : (picked.work_ns * 3 + elapsed_ns) / 4;

        if (options.profile_processors || options.trace_processors || options.step_profiler)
        {
            processor.elapsed_ns += elapsed_ns;
            if (span)
                span->addAttribute("execution_time_ms", elapsed_ns / 1000U);
            if (collect_work_intervals)
                work_intervals.emplace_back(work_started_ns, elapsed_ns, step);
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

void ProcessorRunner::runAsyncReady(ExecutingProcessor & picked)
{
    picked.processor->onAsyncJobReady();
    picked.status.store(ExecutingProcessor::Status::Work);
}

void ProcessorRunner::runUpdatePipeline(ExecutingProcessor & picked)
{
    IProcessor & processor = *picked.processor;
    IProcessor::PipelineUpdate update = processor.updatePipeline();

    /// TODO: move to callers (do that in processor implementations).
    for (const auto & added : update.to_add)
        if (!added->getQueryPlanStep())
            added->inheritQueryPlanStepFromParent(processor, processor.getQueryPlanStepGroup());

    auto updated = pipeline.update(picked, update.to_add, update.to_reconnect);
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

void ProcessorRunner::notifyOwner(ExecutingProcessor & owner)
{
    auto round_lock = owner.lockRound();
    switch (owner.status.load())
    {
        case ExecutingProcessor::Status::Idle:
            owner.status.store(ExecutingProcessor::Status::Prepare);
            runnable.push_back(&owner);
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

void ProcessorRunner::prepareRound(ExecutingProcessor & owner, std::unique_lock<Wyrd::Mutex>)
{
    IProcessor & processor = *owner.processor;

    owner.incoming_updates.drain(hint_inputs, hint_outputs);
    const auto last_status = owner.last_status;
    const auto new_status = processor.prepare(hint_inputs, hint_outputs);
    owner.last_status = new_status;

    if (options.profile_processors)
        profileWaits(processor, last_status, new_status);

    owner.round_updates.drain(changed_inputs, changed_outputs);
    for (auto * output : changed_outputs)
        pending_inputs.push_back(&output->getInputPort());
    for (auto * input : changed_inputs)
        pending_outputs.push_back(&input->getOutputPort());

    switch (new_status)
    {
        case IProcessor::Status::NeedData:
        case IProcessor::Status::PortFull:
            return;

        case IProcessor::Status::Finished:
            if (CurrentThread::getGroup())
                CurrentThread::getGroup()->memory_spill_scheduler->remove(&processor);
            owner.status.store(ExecutingProcessor::Status::Finished);
            finished_processors.push_back(&processor);
            return;

        case IProcessor::Status::Ready:
            owner.status.store(ExecutingProcessor::Status::Work);
            runnable.push_back(&owner);
            return;

        case IProcessor::Status::Async:
        {
#if defined(OS_LINUX) || defined(OS_DARWIN)
            auto [fd, events, timeout_ms] = processor.scheduleForEvent();
            owner.status.store(ExecutingProcessor::Status::Async);
            poll_requests.push_back(PollRequest{.processor = &owner, .fd = fd, .events = events, .timeout_ms = timeout_ms});
            return;
#else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Asynchronous processors are not supported on this platform");
#endif
        }

        case IProcessor::Status::UpdatePipeline:
            owner.status.store(ExecutingProcessor::Status::UpdatePipeline);
            runnable.push_back(&owner);
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

void ProcessorRunner::run(ExecutingProcessor & picked)
{
    runnable.clear();
    poll_requests.clear();
    finished_processors.clear();

    switch (picked.status.load())
    {
        case ExecutingProcessor::Status::Prepare:
            runPrepare(picked);
            break;
        case ExecutingProcessor::Status::Work:
            runWork(picked);
            runPrepare(picked);
            break;
        case ExecutingProcessor::Status::AsyncReady:
            runAsyncReady(picked);
            runWork(picked);
            runPrepare(picked);
            break;
        case ExecutingProcessor::Status::UpdatePipeline:
            runUpdatePipeline(picked);
            runPrepare(picked);
            break;
        case ExecutingProcessor::Status::Idle:
        case ExecutingProcessor::Status::Async:
        case ExecutingProcessor::Status::Finished:
            Wyrd::abort("Processor {} was picked while not scheduled", picked.processor->getName());
    }

    sortRunnable(runnable, picked);
}

const VectorWithMemoryTracking<ExecutingProcessor *> & ProcessorRunner::getRunnable() const
{
    return runnable;
}

const VectorWithMemoryTracking<PollRequest> & ProcessorRunner::getPollRequests() const
{
    return poll_requests;
}

const VectorWithMemoryTracking<IProcessor *> & ProcessorRunner::getFinishedProcessors() const
{
    return finished_processors;
}

WorkIntervals ProcessorRunner::extractWorkIntervals()
{
    return std::exchange(work_intervals, {});
}

}

}
