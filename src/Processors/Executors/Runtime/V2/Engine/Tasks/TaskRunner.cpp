#include <Processors/Executors/Runtime/V2/Engine/Tasks/TaskRunner.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>
#include <Processors/QueryPlan/Profiling/Execution/StepWallClock.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Common/CurrentThread.h>
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

void TaskRunner::runPrepare(ProcessorState & state)
{
    pending_inputs.clear();
    pending_outputs.clear();
    finished_processors.clear();

    {
        auto round_lock = state.lock.lockRound();
        state.lock.setIdle();
        prepareRound(state, std::move(round_lock));
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

    /// TODO: add batch api
    for (auto * processor : finished_processors)
        pipeline.recordAsFinished(*processor);
}

void TaskRunner::runWork(ProcessorState & state) /// NOLINT
{
    IProcessor & processor = *state.processor;

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

void TaskRunner::runAsyncReady(ProcessorState & state)
{
    state.processor->onAsyncJobReady();
    tasks_found.push_back(Task::create(state, Task::Step::Work));
}

void TaskRunner::runUpdatePipeline(ProcessorState & state)
{
    IProcessor::PipelineUpdate update = state.processor->updatePipeline();

    /// TODO: move to callers.
    for (const auto & processor : update.to_add)
        if (!processor->getQueryPlanStep())
            processor->inheritQueryPlanStepFromParent(*state.processor, state.processor->getQueryPlanStepGroup());

    auto updated = pipeline.updateProcessors(state, update.to_add, update.to_reconnect);
    if (!update.to_remove.empty())
        pipeline.submitForRemoval(std::move(update.to_remove));

    if (pipeline.cancelled())
        return;

    tasks_found.push_back(Task::create(state, Task::Step::Prepare));
    for (auto * updated_state : updated)
        notifyOwner(*updated_state);
}

template <class PortT>
void TaskRunner::visitNeighbour(PortT & neighbour)
{
    ProcessorState & owner = neighbour.getRoundUpdatesChannel().getOwner();
    if (owner.lock.isFinished())
        return;

    auto round_lock = owner.lock.lockRound();
    owner.incoming_updates.push(neighbour);

    switch (owner.lock.status())
    {
        case ProcessorLock::Status::Idle:
            prepareRound(owner, std::move(round_lock));
            return;
        case ProcessorLock::Status::Executing:
            owner.processor->onUpdatePorts();
            return;
        case ProcessorLock::Status::Finished:
            return;
    }
}

void TaskRunner::notifyOwner(ProcessorState & state)
{
    auto round_lock = state.lock.lockRound();
    switch (state.lock.status())
    {
        case ProcessorLock::Status::Idle:
            state.lock.setExecuting();
            tasks_found.push_back(Task::create(state, Task::Step::Prepare));
            return;
        case ProcessorLock::Status::Executing:
            state.processor->onUpdatePorts();
            return;
        case ProcessorLock::Status::Finished:
            return;
    }
}

void TaskRunner::prepareRound(ProcessorState & state, std::unique_lock<Wyrd::Mutex>)
{
    IProcessor & processor = *state.processor;

    state.incoming_updates.drain(hint_inputs, hint_outputs);
    const auto last_status = state.last_status;
    const auto new_status = processor.prepare(hint_inputs, hint_outputs);
    state.last_status = new_status;

    if (options.profile_processors)
        profileWaits(processor, last_status, new_status);

    state.round_updates.drain(changed_inputs, changed_outputs);
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
            state.lock.finish();
            finished_processors.push_back(&processor);
            return;

        case IProcessor::Status::Ready:
            state.lock.setExecuting();
            tasks_found.push_back(Task::create(state, Task::Step::Work));
            return;

        case IProcessor::Status::Async:
        {
#if defined(OS_LINUX) || defined(OS_DARWIN)
            auto [fd, events, timeout_ms] = processor.scheduleForEvent();
            state.lock.setExecuting();
            poll_requests_found.push_back(PollRequest{.state = &state, .fd = fd, .events = events, .timeout_ms = timeout_ms});
            return;
#else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Asynchronous processors are not supported on this platform");
#endif
        }

        case IProcessor::Status::UpdatePipeline:
            state.lock.setExecuting();
            tasks_found.push_back(Task::create(state, Task::Step::UpdatePipeline));
            return;
    }
}

void TaskRunner::profileWaits(IProcessor & processor, std::optional<IProcessor::Status> last_status, IProcessor::Status status) const
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

TaskRunner::TaskRunner(ExecutingPipeline & pipeline_, const RunOptions & options_, size_t worker_id_)
    : pipeline(pipeline_)
    , options(options_)
    , worker_id(worker_id_)
    , collect_work_intervals(options.step_profiler && options.step_profiler->needCollectWorkIntervals())
{
}

void TaskRunner::run(Task task)
{
    tasks_found.clear();
    poll_requests_found.clear();

    switch (task.step())
    {
        case Task::Step::Prepare:
            runPrepare(task.state());
            break;
        case Task::Step::Work:
            runWork(task.state());
            runPrepare(task.state());
            break;
        case Task::Step::AsyncReady:
            runAsyncReady(task.state());
            break;
        case Task::Step::UpdatePipeline:
            runUpdatePipeline(task.state());
            break;
    }
}

const std::vector<Task> & TaskRunner::getFoundTasks() const
{
    return tasks_found;
}

const std::vector<PollRequest> & TaskRunner::getFoundPollRequests() const
{
    return poll_requests_found;
}

WorkIntervals TaskRunner::takeWorkIntervals()
{
    return std::exchange(work_intervals, {});
}

}

}
