#include <Processors/Executors/Runtime/Engine/Worker.h>
#include <Processors/Port.h>
#include <Processors/StepWallClock.h>
#include <Processors/StepWallClockRegistry.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Interpreters/ProcessList.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemorySpillScheduler.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_MANY_ROWS_OR_BYTES;
    extern const int QUOTA_EXCEEDED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

namespace
{

bool canAddInfoToException(const Exception & exception)
{
    return exception.code() != ErrorCodes::TOO_MANY_ROWS_OR_BYTES
        && exception.code() != ErrorCodes::QUOTA_EXCEEDED
        && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED
        && exception.code() != ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT;
}

}

Worker::Worker(size_t worker_id_, TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_)
    : worker_id(worker_id_)
    , scheduler(scheduler_)
    , coordinator(coordinator_)
    , pipeline(pipeline_)
{
}

void Worker::profileWaits(IProcessor & processor, std::optional<IProcessor::Status> last_status, IProcessor::Status status) const
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

void Worker::run(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (auto task = pickTask())
    {
        runTask(*task);

        if (scheduler.size() > 1)
            coordinator.wakeOne();

        if (pipeline.hasReadyForRemoval())
            pipeline.removeReady();

        if (pipeline.process_list_element && !pipeline.process_list_element->checkTimeLimitSoft())
        {
            pipeline.cancel(IProcessor::CancelReason::CancelledByTimeout);
            coordinator.stop();
            break;
        }

        if (!slot.keepGoing())
            break;

        if (yield_flag && yield_flag->load())
            break;
    }
}

std::optional<Task> Worker::pickTask()
{
    while (true)
    {
        if (auto task = scheduler.tryPop(worker_id))
            return task;

        if (!coordinator.wait(worker_id))
            return std::nullopt;
    }
}

void Worker::runTask(Task task)
{
    switch (task.kind)
    {
        case Task::Kind::Prepare:
            runPrepare(*task.state);
            break;
        case Task::Kind::Work:
            runWork(*task.state);
            break;
        case Task::Kind::AsyncReady:
            runAsyncReady(*task.state);
            break;
        case Task::Kind::UpdatePipeline:
            runUpdatePipeline(*task.state);
            break;
    }
}

template <class PortT>
void Worker::notifyNeighbour(PortT & neighbour)
{
    ProcessorState & owner = neighbour.getUpdateChannel().getOwner();
    if (owner.lock.isFinished())
        return;

    owner.incoming_updates.push(neighbour);
    owner.lock.notify();

    if (owner.lock.tryLock())
        scheduler.push(Task{.state = &owner, .kind = Task::Kind::Prepare}, worker_id);
    else
        owner.processor->onUpdatePorts();
}

void Worker::runPrepare(ProcessorState & state)
{
    thread_local IProcessor::UpdatedInputPorts hint_inputs;
    thread_local IProcessor::UpdatedOutputPorts hint_outputs;
    thread_local IProcessor::UpdatedInputPorts changed_inputs;
    thread_local IProcessor::UpdatedOutputPorts changed_outputs;

    IProcessor & processor = *state.processor;

    while (true)
    {
        const uint64_t snapshot = state.lock.snapshot();
        state.incoming_updates.drain(hint_inputs, hint_outputs);

        const auto last_status = state.last_status;
        const auto new_status = processor.prepare(hint_inputs, hint_outputs);
        state.last_status = new_status;

        if (pipeline.profile_processors)
            profileWaits(processor, last_status, new_status);

        state.round_updates.drain(changed_inputs, changed_outputs);
        for (auto * input : changed_inputs)
            notifyNeighbour(input->getOutputPort());
        for (auto * output : changed_outputs)
            notifyNeighbour(output->getInputPort());

        switch (new_status)
        {
            case IProcessor::Status::NeedData:
            case IProcessor::Status::PortFull:
                if (state.lock.tryUnlock(snapshot))
                    return;
                continue;

            case IProcessor::Status::Finished:
                state.lock.finish();
                pipeline.recordAsFinished(processor);
                if (CurrentThread::getGroup())
                    CurrentThread::getGroup()->memory_spill_scheduler->remove(&processor);
                return;

            case IProcessor::Status::Ready:
                scheduler.push(Task{.state = &state, .kind = Task::Kind::Work}, worker_id);
                return;

            case IProcessor::Status::Async:
            {
#if defined(OS_LINUX) || defined(OS_DARWIN)
                auto [fd, events, timeout_ms] = processor.scheduleForEvent();
                scheduler.push(AsyncTask{.state = &state, .fd = fd, .events = events, .timeout_ms = timeout_ms});
                return;
#else
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Asynchronous processors are not supported on this platform");
#endif
            }

            case IProcessor::Status::UpdatePipeline:
                scheduler.push(Task{.state = &state, .kind = Task::Kind::UpdatePipeline}, worker_id);
                return;
        }
    }
}

void Worker::runWork(ProcessorState & state) /// NOLINT
{
    IProcessor & processor = *state.processor;

    std::unique_ptr<OpenTelemetry::SpanHolder> span;
    if (pipeline.trace_processors)
    {
        span = std::make_unique<OpenTelemetry::SpanHolder>(processor.getUniqID());
        span->addAttribute("thread_number", worker_id);
    }

    StepWallClock * clock = nullptr;
    if (pipeline.wall_clocks)
    {
        if (const auto * step = processor.getQueryPlanStep())
        {
            auto & cached_clock = processor.query_plan_step_wall_clock_ptr;
            if (!cached_clock)
                cached_clock = pipeline.wall_clocks->find(step, processor.getQueryPlanStepGroup());

            clock = cached_clock;
            if (clock)
                clock->onEnter();
        }
    }

    std::optional<Stopwatch> execution_time_watch;
    if (pipeline.profile_processors || pipeline.trace_processors || clock)
        execution_time_watch.emplace();

    SCOPE_EXIT({
        if (execution_time_watch)
        {
            const UInt64 elapsed_ns = execution_time_watch->elapsedNanoseconds();
            processor.elapsed_ns += elapsed_ns;
            if (span)
                span->addAttribute("execution_time_ms", elapsed_ns / 1000U);
        }

        if (clock)
            clock->onLeave();
    });

    try
    {
        if (processor.isSpillable() && CurrentThread::getGroup())
            CurrentThread::getGroup()->memory_spill_scheduler->checkAndSpill(&processor);

        processor.work();
        ++processor.num_executed_jobs;

        const bool is_source = processor.getInputs().empty();
        if (is_source && pipeline.read_progress_callback)
        {
            if (auto progress = processor.getReadProgress())
            {
                if (progress->counters.total_rows_approx)
                    pipeline.read_progress_callback->addTotalRowsApprox(progress->counters.total_rows_approx);

                if (progress->counters.total_bytes)
                    pipeline.read_progress_callback->addTotalBytes(progress->counters.total_bytes);

                if (!pipeline.read_progress_callback->onProgress(progress->counters.read_rows, progress->counters.read_bytes, progress->limits))
                    processor.cancel();
            }
        }
    }
    catch (Exception exception) /// NOLINT
    {
        if (canAddInfoToException(exception))
            exception.addMessage("While executing " + processor.getName());
        throw exception;
    }

    scheduler.push(Task{.state = &state, .kind = Task::Kind::Prepare}, worker_id);
}

void Worker::runAsyncReady(ProcessorState & state)
{
    state.processor->onAsyncJobReady();
}

void Worker::runUpdatePipeline(ProcessorState & requester)
{
    IProcessor::PipelineUpdate update = requester.processor->updatePipeline();

    auto added = pipeline.addProcessors(requester, update.to_add);
    if (!update.to_remove.empty())
        pipeline.submitForRemoval(std::move(update.to_remove));

    if (pipeline.cancelled())
        return;

    for (auto * state : added)
    {
        if (!state->lock.tryLock())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} added by {} is already locked", state->processor->getName(), requester.processor->getName());

        scheduler.push(Task{.state = state, .kind = Task::Kind::Prepare}, worker_id);
    }

    scheduler.push(Task{.state = &requester, .kind = Task::Kind::Prepare}, worker_id);
}

}
