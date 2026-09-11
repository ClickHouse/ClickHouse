#pragma once

#include <Processors/Executors/Runtime/Pipeline/ProcessorState.h>
#include <Processors/IProcessor.h>

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

class ProcessorStates
{
public:
    explicit ProcessorStates(std::shared_ptr<Processors> processors_);

    ProcessorState & get(const IProcessor & processor);
    void forEachProcessor(const std::function<void(IProcessor &, ProcessorState &)> & f);

    std::vector<ProcessorState *> update(ProcessorState & requester, const Processors & to_add, const Processors & to_reconnect);
    void remove(const Processors & to_remove);

    String dump() const;

private:
    mutable std::mutex mutex;
    std::shared_ptr<Processors> processors;
    std::unordered_map<const IProcessor *, ProcessorState> states;
};

}
