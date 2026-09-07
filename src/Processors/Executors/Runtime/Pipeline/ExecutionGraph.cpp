#include <Processors/Executors/Runtime/Pipeline/ExecutionGraph.h>
#include <Processors/Port.h>
#include <QueryPipeline/printPipeline.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

String describeProcessor(const IProcessor * processor)
{
    return fmt::format("{} at {}", processor->getUniqID(), static_cast<const void *>(processor));
}

ProcessorState & addState(std::unordered_map<const IProcessor *, ProcessorState> & states, IProcessor * processor)
{
    auto [it, inserted] = states.try_emplace(processor);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} was already added to pipeline", processor->getName());

    it->second.processor = processor;
    return it->second;
}

void checkPeerIsKnown(const std::unordered_map<const IProcessor *, ProcessorState> & states, const ProcessorState & state, const Port & peer, const char * found_as)
{
    const IProcessor * to = &peer.getProcessor();
    if (!states.contains(to))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Processor {} was found as {} for processor {}, but not found in list of processors",
            describeProcessor(to),
            found_as,
            describeProcessor(state.processor));
}

void connectPorts(const std::unordered_map<const IProcessor *, ProcessorState> & states, ProcessorState & state)
{
    for (auto & input : state.processor->getInputs())
    {
        if (!input.isConnected() || input.getUpdateChannel().isConnected())
            continue;

        checkPeerIsKnown(states, state, input.getOutputPort(), "input");
        input.getUpdateChannel().connect(state, input);
        state.incoming_updates.push(input);
    }

    for (auto & output : state.processor->getOutputs())
    {
        if (!output.isConnected() || output.getUpdateChannel().isConnected())
            continue;

        checkPeerIsKnown(states, state, output.getInputPort(), "output");
        output.getUpdateChannel().connect(state, output);
        state.incoming_updates.push(output);
    }
}

}

ExecutionGraph::ExecutionGraph(std::shared_ptr<Processors> processors_)
    : processors(std::move(processors_))
{
    for (const auto & processor : *processors)
        addState(states, processor.get());

    for (const auto & processor : *processors)
        connectPorts(states, states.at(processor.get()));
}

void ExecutionGraph::add(ProcessorState & requester, const Processors & added)
{
    std::lock_guard lock(mutex);

    for (const auto & processor : added)
    {
        addState(states, processor.get());
        processors->push_back(processor);
    }

    connectPorts(states, requester);
    for (const auto & processor : added)
        connectPorts(states, states.at(processor.get()));
}

void ExecutionGraph::remove(const ProcessorPtr & processor)
{
    std::lock_guard lock(mutex);

    auto state_it = states.find(processor.get());
    if (state_it == states.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} does not exist in pipeline", processor->getName());

    if (state_it->second.last_status != IProcessor::Status::Finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to remove not finished processor {}", processor->getName());

    for (auto & input : processor->getInputs())
        input.getUpdateChannel().disconnect();

    for (auto & output : processor->getOutputs())
        output.getUpdateChannel().disconnect();

    states.erase(state_it);
    processors->remove(processor);
}

ProcessorState & ExecutionGraph::getState(const IProcessor & processor)
{
    std::lock_guard lock(mutex);

    auto it = states.find(&processor);
    if (it == states.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} does not exist in pipeline", processor.getName());

    return it->second;
}

void ExecutionGraph::forEachProcessor(const std::function<void(IProcessor &)> & f)
{
    std::lock_guard lock(mutex);
    for (const auto & processor : *processors)
        f(*processor);
}

bool ExecutionGraph::allFinished() const
{
    std::lock_guard lock(mutex);
    return std::ranges::all_of(states, [](const auto & entry) { return entry.second.lock.isFinished(); });
}

String ExecutionGraph::dump() const
{
    std::lock_guard lock(mutex);

    std::vector<std::optional<IProcessor::Status>> statuses;
    statuses.reserve(processors->size());

    for (const auto & processor : *processors)
    {
        WriteBufferFromOwnString buffer;
        buffer << "(" << processor->getNumExecutedJobs() << " jobs)";
        processor->setDescription(buffer.str());

        statuses.emplace_back(states.at(processor.get()).last_status);
    }

    WriteBufferFromOwnString out;
    printPipeline(*processors, statuses, out);
    out.finalize();

    return out.str();
}

}
