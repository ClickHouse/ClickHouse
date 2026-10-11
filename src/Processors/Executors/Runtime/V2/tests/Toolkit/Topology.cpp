#include <Processors/Executors/Runtime/V2/tests/Toolkit/Topology.h>
#include <Processors/Executors/Runtime/V2/tests/Toolkit/Scripted.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Port.h>

#include <ranges>

namespace DB::Runtime::V2
{

void chain(const VectorWithMemoryTracking<ProcessorPtr> & stages)
{
    for (size_t index = 1; index < stages.size(); ++index)
        connect(stages[index - 1]->getOutputs().front(), stages[index]->getInputs().front());
}

ProcessorPtr fanIn(const VectorWithMemoryTracking<ProcessorPtr> & sources, const ProcessorPtr & sink)
{
    auto resize = std::make_shared<ResizeProcessor>(makeHeader(), sources.size(), 1);

    connect(resize->getOutputs().front(), sink->getInputs().front());
    for (auto [source, input] : std::views::zip(sources, resize->getInputs()))
        connect(source->getOutputs().front(), input);

    return resize;
}

ProcessorPtr fanOut(const ProcessorPtr & source, const VectorWithMemoryTracking<ProcessorPtr> & sinks)
{
    auto resize = std::make_shared<ResizeProcessor>(makeHeader(), 1, sinks.size());

    connect(source->getOutputs().front(), resize->getInputs().front());
    for (auto [output, sink] : std::views::zip(resize->getOutputs(), sinks))
        connect(output, sink->getInputs().front());

    return resize;
}

}
