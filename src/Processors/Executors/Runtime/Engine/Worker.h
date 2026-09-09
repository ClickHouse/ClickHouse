#pragma once

#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/Engine/WorkerSlot.h>
#include <Processors/Executors/Runtime/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/Pipeline/ExecutingPipeline.h>

#include <atomic>
#include <mutex>
#include <optional>

namespace DB
{

class Worker
{
    std::optional<Task> pickTask();
    void runTask(Task task);
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
};

}
