#include <Processors/Executors/Runtime/V2/Pipeline/ExecutingPipeline.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Invariants.h>
#include <Processors/Port.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <QueryPipeline/printPipeline.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <fmt/format.h>

#include <algorithm>

namespace DB
{

namespace Runtime::V2
{

namespace
{

using Records = UnorderedMapWithMemoryTracking<const IProcessor *, ExecutingProcessor>;

String describeProcessor(const IProcessor * processor)
{
    return fmt::format("{} at {}", processor->getUniqID(), fmt::ptr(processor));
}

void checkNeighbourIsKnown(const Records & records, const ExecutingProcessor & executing, const Port & connected_port, const char * found_as)
{
    const IProcessor * to = &connected_port.getProcessor();
    if (!records.contains(to))
        Wyrd::abort(
            "Processor {} was found as {} for processor {}, but not found in list of processors",
            describeProcessor(to),
            found_as,
            describeProcessor(executing.processor));
}

void checkNeighboursHaveChannels(ExecutingProcessor & executing)
{
    for (auto & input : executing.processor->getInputs())
        if (input.isConnected() && !input.getOutputPort().getRoundUpdatesChannel().isConnected())
            Wyrd::abort("Processor {} got a connected output port from a pipeline update that did not list it in to_reconnect", describeProcessor(&input.getOutputPort().getProcessor()));

    for (auto & output : executing.processor->getOutputs())
        if (output.isConnected() && !output.getInputPort().getRoundUpdatesChannel().isConnected())
            Wyrd::abort("Processor {} got a connected input port from a pipeline update that did not list it in to_reconnect", describeProcessor(&output.getInputPort().getProcessor()));
}

void checkConnectedOnlyInsideGroup(const Processors & group)
{
    UnorderedSetWithMemoryTracking<const IProcessor *> members;
    for (const auto & processor : group)
        members.insert(processor.get());

    auto check = [&](const IProcessor & processor, const IProcessor & neighbour)
    {
        if (!members.contains(&neighbour))
            Wyrd::abort(
                "Cannot remove processor {}: it is still connected to {}, which stays in the pipeline",
                processor.getName(),
                neighbour.getName());
    };

    for (const auto & processor : group)
    {
        for (const auto & input : processor->getInputs())
            if (input.isConnected())
                check(*processor, input.getOutputPort().getProcessor());

        for (const auto & output : processor->getOutputs())
            if (output.isConnected())
                check(*processor, output.getInputPort().getProcessor());
    }
}

void countSiblings(const Processors & processors, const Processors & counted, Records & records)
{
    MapWithMemoryTracking<std::pair<const IQueryPlanStep *, size_t>, size_t> instances;
    for (const auto & processor : processors)
        if (processor->getQueryPlanStep())
            ++instances[{processor->getQueryPlanStep(), processor->getQueryPlanStepGroup()}];

    for (const auto & processor : counted)
    {
        const auto * step = processor->getQueryPlanStep();
        records.at(processor.get()).siblings = step ? instances[{step, processor->getQueryPlanStepGroup()}] : 1;
    }
}

ExecutingProcessor & addRecord(Records & records, IProcessor * processor)
{
    auto [it, inserted] = records.try_emplace(processor);
    if (!inserted)
        Wyrd::abort("Processor {} was already added to pipeline", processor->getName());

    it->second.setProcessor(*processor);
    return it->second;
}

void connectPorts(const Records & records, ExecutingProcessor & executing)
{
    for (auto & input : executing.processor->getInputs())
    {
        if (!input.isConnected() || input.getRoundUpdatesChannel().isConnected())
            continue;

        checkNeighbourIsKnown(records, executing, input.getOutputPort(), "input");
        input.getRoundUpdatesChannel().connect(executing, input);
        executing.incoming_updates.push(input);
    }

    for (auto & output : executing.processor->getOutputs())
    {
        if (!output.isConnected() || output.getRoundUpdatesChannel().isConnected())
            continue;

        checkNeighbourIsKnown(records, executing, output.getInputPort(), "output");
        output.getRoundUpdatesChannel().connect(executing, output);
        executing.incoming_updates.push(output);
    }
}

void disconnectPorts(IProcessor & processor)
{
    for (auto & input : processor.getInputs())
        input.getRoundUpdatesChannel().disconnect();

    for (auto & output : processor.getOutputs())
        output.getRoundUpdatesChannel().disconnect();
}

}

ExecutingPipeline::ExecutingPipeline(std::shared_ptr<Processors> processors_)
    : processors(std::move(processors_))
{
    Wyrd::setName(mutex, "pipeline.mutex");
    Wyrd::setName(cancel_reason, "pipeline.cancel_reason");

    for (const auto & processor : *processors)
        addRecord(records, processor.get());
    countSiblings(*processors, *processors, records);

    for (const auto & processor : *processors)
        connectPorts(records, records.at(processor.get()));
}

VectorWithMemoryTracking<ExecutingProcessor *> ExecutingPipeline::sinks()
{
    std::lock_guard lock(mutex);

    VectorWithMemoryTracking<ExecutingProcessor *> result;
    for (const auto & processor : *processors)
    {
        const bool has_connected_output = std::ranges::any_of(processor->getOutputs(), [](const OutputPort & output) { return output.isConnected(); });
        if (!has_connected_output)
            result.push_back(&records.at(processor.get()));
    }

    std::ranges::reverse(result);
    return result;
}

VectorWithMemoryTracking<ExecutingProcessor *> ExecutingPipeline::update(ExecutingProcessor & requester, const Processors & to_add, const Processors & to_reconnect)
{
    std::lock_guard lock(mutex);

    VectorWithMemoryTracking<ExecutingProcessor *> updated;
    for (const auto & processor : to_reconnect)
    {
        auto it = records.find(processor.get());
        if (it == records.end())
            Wyrd::abort("Processor {} listed in to_reconnect does not exist in pipeline", processor->getName());

        updated.push_back(&it->second);
    }

    for (const auto & processor : to_add)
        updated.push_back(&addRecord(records, processor.get()));

    processors->insert(processors->end(), to_add.begin(), to_add.end());
    countSiblings(*processors, to_add, records);

    const auto reason = cancel_reason.load();
    if (reason != IProcessor::CancelReason::NotCancelled)
        for (const auto & processor : to_add)
            processor->cancel(reason);

    connectPorts(records, requester);
    for (auto * executing : updated)
        connectPorts(records, *executing);

    checkNeighboursHaveChannels(requester);
    for (auto * executing : updated)
        checkNeighboursHaveChannels(*executing);

    return updated;
}

void ExecutingPipeline::submitForRemoval(const Processors & group)
{
    removals.submitForRemoval(group);
}

void ExecutingPipeline::recordAsFinished(const VectorWithMemoryTracking<IProcessor *> & finished)
{
    removals.markFinished(finished);
}

bool ExecutingPipeline::hasReadyForRemoval() const
{
    return removals.hasReadyForRemoval();
}

void ExecutingPipeline::removeReady()
{
    const Processors ready = removals.takeReadyForRemoval();
    checkConnectedOnlyInsideGroup(ready);

    std::lock_guard lock(mutex);
    for (const auto & processor : ready)
    {
        auto it = records.find(processor.get());
        if (it == records.end())
            Wyrd::abort("Processor {} submitted for removal does not exist in pipeline", processor->getName());

        if (it->second.status.load() != ExecutingProcessor::Status::Finished)
            Wyrd::abort("Trying to remove not finished processor {}", processor->getName());

        disconnectPorts(*processor);
        records.erase(it);
        processors->remove(processor);
    }
}

void ExecutingPipeline::cancel(IProcessor::CancelReason reason)
{
    Wyrd::SpinWait spin_wait;
    auto current = cancel_reason.load();
    while (current == IProcessor::CancelReason::NotCancelled || (current == IProcessor::CancelReason::PartialResult && reason != IProcessor::CancelReason::PartialResult))
    {
        if (cancel_reason.compare_exchange_weak(current, reason))
            break;

        spin_wait.spin();
    }

    std::lock_guard lock(mutex);
    for (const auto & processor : *processors)
        processor->cancel(cancel_reason.load());
}

bool ExecutingPipeline::cancelled() const
{
    const auto reason = cancel_reason.load();
    return reason != IProcessor::CancelReason::NotCancelled && reason != IProcessor::CancelReason::PartialResult;
}

IProcessor::CancelReason ExecutingPipeline::cancelReason() const
{
    return cancel_reason.load();
}

void ExecutingPipeline::finish()
{
    std::lock_guard lock(mutex);
    for (const auto & processor : *processors)
        processor->onPipelineFinished();
}

bool ExecutingPipeline::allProcessorsFinished() const
{
    std::lock_guard lock(mutex);
    return removals.finishedCount() == processors->size();
}

void ExecutingPipeline::reportReadProgress(ReadProgressCallback & callback)
{
    std::lock_guard lock(mutex);
    for (const auto & processor : *processors)
    {
        auto progress = processor->getReadProgress();
        if (!progress)
            continue;

        if (progress->counters.total_rows_approx)
            callback.addTotalRowsApprox(progress->counters.total_rows_approx);

        if (progress->counters.total_bytes)
            callback.addTotalBytes(progress->counters.total_bytes);

        if (progress->counters.read_rows || progress->counters.read_bytes)
            callback.onProgress(progress->counters.read_rows, progress->counters.read_bytes, progress->limits);
    }
}

size_t ExecutingPipeline::size() const
{
    std::lock_guard lock(mutex);
    return processors->size();
}

String ExecutingPipeline::dump() const
{
    std::lock_guard lock(mutex);

    VectorWithMemoryTracking<std::optional<IProcessor::Status>> statuses;
    statuses.reserve(processors->size());

    for (const auto & processor : *processors)
    {
        WriteBufferFromOwnString buffer;
        buffer << "(" << processor->getNumExecutedJobs() << " jobs)";
        processor->setDescription(buffer.str());

        statuses.emplace_back(records.at(processor.get()).last_status);
    }

    WriteBufferFromOwnString out;
    printPipeline(*processors, statuses, out);
    out.finalize();

    return out.str();
}

}

}
