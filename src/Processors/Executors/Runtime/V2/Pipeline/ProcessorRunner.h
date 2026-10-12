#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/PollRequest.h>
#include <Processors/Executors/Runtime/V2/ExecutionOptions.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>
#include <Common/VectorWithMemoryTracking.h>

#include <mutex>
#include <optional>

namespace DB
{

namespace Runtime::V2
{

class ProcessorRunner
{
    /// Processor syscalls.
    void runPrepare(ExecutingProcessor & picked);
    void runWork(ExecutingProcessor & picked);
    void runAsyncReady(ExecutingProcessor & picked);
    void runUpdatePipeline(ExecutingProcessor & picked);

    /// Helper methods.
    template <class PortT>
    void visitNeighbour(PortT & neighbour);
    void notifyOwner(ExecutingProcessor & owner);
    void prepareRound(ExecutingProcessor & owner, std::unique_lock<Wyrd::Mutex> round_lock);
    void profileWaits(IProcessor & processor, std::optional<IProcessor::Status> last_status, IProcessor::Status status) const;

public:
    ProcessorRunner(ExecutingPipeline & pipeline_, const ExecutionOptions & options_, size_t worker_id_);

    void run(ExecutingProcessor & picked);
    const VectorWithMemoryTracking<ExecutingProcessor *> & getRunnable() const;
    const VectorWithMemoryTracking<PollRequest> & getPollRequests() const;
    const VectorWithMemoryTracking<IProcessor *> & getFinishedProcessors() const;
    WorkIntervals extractWorkIntervals();

private:
    ExecutingPipeline & pipeline;
    const ExecutionOptions & options;
    const size_t worker_id;
    const bool collect_work_intervals;
    WorkIntervals work_intervals;

    /// Graph prepare temporary data.
    VectorWithMemoryTracking<ExecutingProcessor *> runnable;
    VectorWithMemoryTracking<PollRequest> poll_requests;
    VectorWithMemoryTracking<IProcessor *> finished_processors;
    VectorWithMemoryTracking<InputPort *> pending_inputs;
    VectorWithMemoryTracking<OutputPort *> pending_outputs;

    /// Single processor prepare temporary data.
    IProcessor::UpdatedInputPorts hint_inputs;
    IProcessor::UpdatedOutputPorts hint_outputs;
    IProcessor::UpdatedInputPorts changed_inputs;
    IProcessor::UpdatedOutputPorts changed_outputs;
};

}

}
