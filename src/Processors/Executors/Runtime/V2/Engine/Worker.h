#pragma once

#include <Processors/Executors/Runtime/V2/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/QueryPlan/Profiling/Execution/WorkInterval.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

namespace Runtime::V2
{

class Worker
{
    std::optional<Task> pickTask();
    void wakeOrSpawn(size_t threads_needed);
    size_t runTask(Task task);
    void runPrepare(ProcessorState & state);
    void prepareRound(ProcessorState & state, std::unique_lock<std::mutex> round_lock);
    void runWork(ProcessorState & state);
    void runAsyncReady(ProcessorState & state);
    void runUpdatePipeline(ProcessorState & requester);
    void profileWaits(IProcessor & processor, std::optional<IProcessor::Status> last_status, IProcessor::Status status) const;

    template <class PortT>
    void visitNeighbour(PortT & neighbour);
    void notifyOwner(ProcessorState & owner);

public:
    Worker(size_t worker_id_, TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_, WorkerPool & pool_);

    void run(WorkerSlot & slot, std::atomic_bool * yield_flag);

private:
    const size_t worker_id;
    TaskScheduler & scheduler;
    WorkersCoordinator & coordinator;
    ExecutingPipeline & pipeline;
    WorkerPool & pool;
    const bool collect_work_intervals;
    WorkIntervals work_intervals;

    /// Graph prepare temporary data.
    std::vector<Task> tasks_found;
    std::vector<AsyncTask> async_tasks_found;
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
