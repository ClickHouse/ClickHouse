#pragma once

#include <Processors/Executors/Runtime/Engine/TaskScheduler.h>
#include <Processors/Executors/Runtime/Engine/WorkersCoordinator.h>
#include <Processors/Executors/Runtime/Pipeline/ExecutingPipeline.h>

#include <atomic>
#include <functional>
#include <optional>

namespace DB
{

class Worker
{
    std::optional<Task> pickTask();
    void runTask(Task task);
    void runPrepare(ProcessorState & state);
    void runWork(ProcessorState & state);
    void runAsyncReady(ProcessorState & state);
    void runUpdatePipeline(ProcessorState & requester);
    void profileWaits(IProcessor & processor, std::optional<IProcessor::Status> last_status, IProcessor::Status status) const;

    template <class PortT>
    void notifyNeighbour(PortT & neighbour);

public:
    using KeepGoing = std::function<bool()>;

    Worker(size_t worker_id_, TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_);

    void run(const KeepGoing & keep_going, std::atomic_bool * yield_flag);

private:
    const size_t worker_id;
    TaskScheduler & scheduler;
    WorkersCoordinator & coordinator;
    ExecutingPipeline & pipeline;
};

}
