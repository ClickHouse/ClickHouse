#include <Processors/Executors/Runtime/Topology/ExecutionGraph.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ExecutionGraph::ExecutionGraph(std::shared_ptr<Processors> processors_)
    : processors(std::move(processors_))
{
}

void ExecutionGraph::addProcessor(ProcessorPtr processor)
{
    std::lock_guard lock(mutex);
    processors->push_back(std::move(processor));
}

void ExecutionGraph::removeProcessor(const ProcessorPtr & processor)
{
    std::lock_guard lock(mutex);

    auto it = std::find(processors->begin(), processors->end(), processor);
    if (it == processors->end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} does not exist in pipeline", processor->getName());

    processors->erase(it);
}

const Processors & ExecutionGraph::getProcessors() const
{
    return *processors;
}

}
