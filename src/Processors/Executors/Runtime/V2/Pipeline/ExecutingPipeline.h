#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/V2/Pipeline/RemovalGroups.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/IProcessor.h>

#include <memory>
#include <unordered_map>
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

    std::vector<ExecutingProcessor *> sinks();
    std::vector<ExecutingProcessor *> update(ExecutingProcessor & requester, const Processors & to_add, const Processors & to_reconnect);

    void submitForRemoval(const Processors & group);
    void recordAsFinished(const std::vector<IProcessor *> & finished);
    bool hasReadyForRemoval() const;
    void removeReady();

    void cancel(IProcessor::CancelReason reason);
    bool cancelled() const;
    IProcessor::CancelReason cancelReason() const;

    void finish();
    bool allProcessorsFinished() const;
    void reportReadProgress(ReadProgressCallback & callback);

    String dump() const;

private:
    mutable Wyrd::Mutex mutex;
    std::shared_ptr<Processors> processors;
    std::unordered_map<const IProcessor *, ExecutingProcessor> records;
    RemovalGroups removals;
    Wyrd::Atomic<IProcessor::CancelReason> cancel_reason = IProcessor::CancelReason::NotCancelled;
};

}

}
