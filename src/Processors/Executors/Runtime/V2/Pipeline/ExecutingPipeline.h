#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/State/ProcessorStates.h>
#include <Processors/Executors/Runtime/V2/Pipeline/State/RemovalGroups.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/IProcessor.h>

#include <memory>
#include <vector>

namespace DB
{

class ReadProgressCallback;

namespace Runtime::V2
{

class ExecutingPipeline
{
public:
    explicit ExecutingPipeline(std::shared_ptr<Processors> processors_);

    std::vector<ProcessorState *> sinks();
    std::vector<ProcessorState *> updateProcessors(ProcessorState & requester, const Processors & to_add, const Processors & to_reconnect);

    void submitForRemoval(Processors group);
    void recordAsFinished(IProcessor & processor);
    bool hasReadyForRemoval() const;
    void removeReady();

    void cancel(IProcessor::CancelReason reason);
    bool cancelled() const;
    bool allProcessorsFinished();
    void reportReadProgress(ReadProgressCallback & callback);
    String dump() const;

private:
    ProcessorStates states;
    RemovalGroups removals;
    Wyrd::Atomic<IProcessor::CancelReason> cancel_reason = IProcessor::CancelReason::NotCancelled;
};

}

}
