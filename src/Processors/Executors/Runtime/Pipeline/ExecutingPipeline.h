#pragma once

#include <Processors/Executors/Runtime/Pipeline/ProcessorStates.h>
#include <Processors/Executors/Runtime/Pipeline/RemovalCoordinator.h>
#include <Processors/IProcessor.h>

#include <atomic>
#include <exception>
#include <memory>
#include <vector>

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ReadProgressCallback;
class StepWallClockRegistry;

struct ExecutingPipeline
{
    const bool profile_processors;
    const bool trace_processors;
    const QueryStatusPtr process_list_element;
    const StepWallClockRegistry * const wall_clocks;

    ReadProgressCallback * read_progress_callback = nullptr;

    std::atomic<IProcessor::CancelReason> cancel_reason = IProcessor::CancelReason::NotCancelled;
    std::exception_ptr exception;

public:
    ExecutingPipeline(std::shared_ptr<Processors> processors_, QueryStatusPtr process_list_element_, const StepWallClockRegistry * wall_clocks_);

    std::vector<ProcessorState *> sinks();
    std::vector<ProcessorState *> updateProcessors(ProcessorState & requester, const Processors & to_add, const Processors & to_reconnect);

    void submitForRemoval(Processors group);
    void recordAsFinished(IProcessor & processor);
    bool hasReadyForRemoval() const;
    void removeReady();

    void cancel(IProcessor::CancelReason reason);
    void fail(std::exception_ptr exception);

    bool cancelled() const;
    bool allFinished();
    void reportReadProgress(ReadProgressCallback & callback);
    String dump() const;

private:
    ProcessorStates states;
    RemovalCoordinator removals;
    std::atomic_bool failed = false;
};

}
