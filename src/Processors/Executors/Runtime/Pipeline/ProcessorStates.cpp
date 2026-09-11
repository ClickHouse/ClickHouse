#include <Processors/Executors/Runtime/Pipeline/ProcessorStates.h>
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

void checkNeighbourIsKnown(const std::unordered_map<const IProcessor *, ProcessorState> & states, const ProcessorState & state, const Port & connected_port, const char * found_as)
{
    const IProcessor * to = &connected_port.getProcessor();
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

        checkNeighbourIsKnown(states, state, input.getOutputPort(), "input");
        input.getUpdateChannel().connect(state, input);
        state.incoming_updates.push(input);
    }

    for (auto & output : state.processor->getOutputs())
    {
        if (!output.isConnected() || output.getUpdateChannel().isConnected())
            continue;

        checkNeighbourIsKnown(states, state, output.getInputPort(), "output");
        output.getUpdateChannel().connect(state, output);
        state.incoming_updates.push(output);
    }
}

#ifdef DEBUG_OR_SANITIZER_BUILD
void checkNeighboursHaveChannels(ProcessorState & state)
{
    for (auto & input : state.processor->getInputs())
        if (input.isConnected() && !input.getOutputPort().getUpdateChannel().isConnected())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} got a connected output port from a pipeline update that did not list it in to_reconnect", describeProcessor(&input.getOutputPort().getProcessor()));

    for (auto & output : state.processor->getOutputs())
        if (output.isConnected() && !output.getInputPort().getUpdateChannel().isConnected())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} got a connected input port from a pipeline update that did not list it in to_reconnect", describeProcessor(&output.getInputPort().getProcessor()));
}
#endif

}

ProcessorStates::ProcessorStates(std::shared_ptr<Processors> processors_)
    : processors(std::move(processors_))
{
    for (const auto & processor : *processors)
        addState(states, processor.get());

    for (const auto & processor : *processors)
        connectPorts(states, states.at(processor.get()));
}

ProcessorState & ProcessorStates::get(const IProcessor & processor)
{
    std::lock_guard lock(mutex);

    auto it = states.find(&processor);
    if (it == states.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} does not exist in pipeline", processor.getName());

    return it->second;
}

void ProcessorStates::forEachProcessor(const std::function<void(IProcessor &, ProcessorState &)> & f)
{
    std::lock_guard lock(mutex);
    for (const auto & processor : *processors)
        f(*processor, states.at(processor.get()));
}

std::vector<ProcessorState *> ProcessorStates::update(ProcessorState & requester, const Processors & to_add, const Processors & to_reconnect)
{
    std::lock_guard lock(mutex);

    std::vector<ProcessorState *> updated;
    for (const auto & processor : to_reconnect)
    {
        auto it = states.find(processor.get());
        if (it == states.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} listed in to_reconnect does not exist in pipeline", processor->getName());

        updated.push_back(&it->second);
    }

    for (const auto & processor : to_add)
    {
        updated.push_back(&addState(states, processor.get()));
        processors->push_back(processor);
    }

    connectPorts(states, requester);
    for (auto * state : updated)
        connectPorts(states, *state);

#ifdef DEBUG_OR_SANITIZER_BUILD
    checkNeighboursHaveChannels(requester);
    for (auto * state : updated)
        checkNeighboursHaveChannels(*state);
#endif

    return updated;
}

void ProcessorStates::remove(const Processors & to_remove)
{
    std::lock_guard lock(mutex);

    for (const auto & processor : to_remove)
    {
        auto state_it = states.find(processor.get());
        if (state_it == states.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} does not exist in pipeline", processor->getName());

        if (state_it->second.last_status != IProcessor::Status::Finished)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to remove not finished processor {}", processor->getName());
    }

    for (const auto & processor : to_remove)
    {
        for (auto & input : processor->getInputs())
            input.getUpdateChannel().disconnect();

        for (auto & output : processor->getOutputs())
            output.getUpdateChannel().disconnect();

        states.erase(processor.get());
        processors->erase(std::find(processors->begin(), processors->end(), processor));
    }
}

String ProcessorStates::dump() const
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
