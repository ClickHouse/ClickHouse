#pragma once

#include <Processors/Executors/Runtime/V2/Engine/Polling/PollRequest.h>
#include <Processors/Executors/Runtime/V2/Engine/RunOptions.h>
#include <Processors/Executors/Runtime/V2/Engine/Tasks/Task.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>

#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

namespace Runtime::V2
{

class TaskRunner
{
    /// Processor syscalls.
    void runPrepare(ProcessorState & state);
    void runWork(ProcessorState & state);
    void runAsyncReady(ProcessorState & state);
    void runUpdatePipeline(ProcessorState & state);

    /// Helper methods.
    template <class PortT>
    void visitNeighbour(PortT & neighbour);
    void notifyOwner(ProcessorState & state);
    void prepareRound(ProcessorState & state, std::unique_lock<Wyrd::Mutex> round_lock);
    void profileWaits(IProcessor & processor, std::optional<IProcessor::Status> last_status, IProcessor::Status status) const;

public:
    TaskRunner(ExecutingPipeline & pipeline_, const RunOptions & options_, size_t worker_id_);

    void run(Task task);
    const std::vector<Task> & getFoundTasks() const;
    const std::vector<PollRequest> & getFoundPollRequests() const;
    WorkIntervals takeWorkIntervals();

private:
    ExecutingPipeline & pipeline;
    const RunOptions & options;
    const size_t worker_id;
    const bool collect_work_intervals;
    WorkIntervals work_intervals;

    /// Graph prepare temporary data.
    std::vector<Task> tasks_found;
    std::vector<PollRequest> poll_requests_found;
    std::vector<InputPort *> pending_inputs;
    std::vector<OutputPort *> pending_outputs;
    std::vector<IProcessor *> finished_processors;

    /// Single processor prepare temporary data.
    IProcessor::UpdatedInputPorts hint_inputs;
    IProcessor::UpdatedOutputPorts hint_outputs;
    IProcessor::UpdatedInputPorts changed_inputs;
    IProcessor::UpdatedOutputPorts changed_outputs;
};

}

}
