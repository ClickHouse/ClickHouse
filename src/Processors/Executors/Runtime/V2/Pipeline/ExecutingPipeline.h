#pragma once

#include <Processors/Executors/Runtime/V2/Pipeline/State/ProcessorStates.h>
#include <Processors/Executors/Runtime/V2/Pipeline/State/RemovalGroups.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Atomic.h>
#include <Processors/IProcessor.h>

#include <exception>
#include <memory>
#include <vector>

namespace DB
{

class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class ReadProgressCallback;
class StepProfiler;

namespace Runtime::V2
{

struct ExecutingPipeline
{
    const bool profile_processors;
    const bool trace_processors;
    const QueryStatusPtr process_list_element;

    ReadProgressCallback * read_progress_callback = nullptr;
    StepProfiler * step_profiler = nullptr;

    Wyrd::Atomic<IProcessor::CancelReason> cancel_reason = IProcessor::CancelReason::NotCancelled;
    std::exception_ptr exception;

public:
    ExecutingPipeline(std::shared_ptr<Processors> processors_, QueryStatusPtr process_list_element_);

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
    RemovalGroups removals;
    Wyrd::Atomic<bool> failed = false;
};

}

}
