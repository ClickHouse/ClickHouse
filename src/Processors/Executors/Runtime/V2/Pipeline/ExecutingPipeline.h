#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingProcessor.h>
#include <Processors/Executors/Runtime/V2/Pipeline/RemovalGroups.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Mutex.h>
#include <Processors/IProcessor.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <memory>

namespace DB
{

class ReadProgressCallback;

namespace Runtime::V2
{

class ExecutingPipeline
{
public:
    explicit ExecutingPipeline(std::shared_ptr<Processors> processors_);

    VectorWithMemoryTracking<ExecutingProcessor *> sinks();
    VectorWithMemoryTracking<ExecutingProcessor *> update(ExecutingProcessor & requester, const Processors & to_add, const Processors & to_reconnect);

    void submitForRemoval(const Processors & group);
    void recordAsFinished(const VectorWithMemoryTracking<IProcessor *> & finished);
    bool hasReadyForRemoval() const;
    void removeReady();

    void cancel(IProcessor::CancelReason reason);
    bool cancelled() const;
    IProcessor::CancelReason cancelReason() const;

    void finish();
    bool allProcessorsFinished() const;
    void reportReadProgress(ReadProgressCallback & callback);

    size_t size() const;
    String dump() const;

private:
    mutable Wyrd::Mutex mutex;
    std::shared_ptr<Processors> processors;
    UnorderedMapWithMemoryTracking<const IProcessor *, ExecutingProcessor> records;
    RemovalGroups removals;
    Wyrd::Atomic<IProcessor::CancelReason> cancel_reason = IProcessor::CancelReason::NotCancelled;
};

}

}
