#pragma once

#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Processors/IProcessor.h>

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

class ExecutionGraph
{
public:
    explicit ExecutionGraph(std::shared_ptr<Processors> processors_);

    void add(ProcessorState & requester, const Processors & added);
    void remove(const ProcessorPtr & processor);
    ProcessorState & getState(const IProcessor & processor);

    void forEachProcessor(const std::function<void(IProcessor &)> & f);

    bool allFinished() const;
    String dump() const;

private:
    mutable std::mutex mutex;
    std::shared_ptr<Processors> processors;
    std::unordered_map<const IProcessor *, ProcessorState> states;
};

}
