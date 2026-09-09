#include <Processors/Executors/Runtime/Pipeline/ExecutingPipeline.h>
#include <Processors/Port.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Core/Settings.h>
#include <Common/Exception.h>

#include <algorithm>
#include <unordered_set>

namespace DB
{

namespace Setting
{
    extern const SettingsBool log_processors_profiles;
    extern const SettingsBool opentelemetry_trace_processors;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

bool profileProcessors(const QueryStatusPtr & process_list_element)
{
    if (!process_list_element)
        return false;

    const auto & context = process_list_element->getContext();
    return context->getSettingsRef()[Setting::log_processors_profiles] && context->getProcessorsProfileLog();
}

bool traceProcessors(const QueryStatusPtr & process_list_element)
{
    return process_list_element && process_list_element->getContext()->getSettingsRef()[Setting::opentelemetry_trace_processors];
}

bool supersedes(IProcessor::CancelReason current, IProcessor::CancelReason reason)
{
    if (current == IProcessor::CancelReason::NotCancelled)
        return true;

    return current == IProcessor::CancelReason::PartialResult && reason != IProcessor::CancelReason::PartialResult;
}

void checkConnectedOnlyInside(const Processors & group)
{
    std::unordered_set<const IProcessor *> members;
    for (const auto & processor : group)
        members.insert(processor.get());

    auto check = [&](const IProcessor & processor, const IProcessor & neighbour)
    {
        if (!members.contains(&neighbour))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
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

}

ExecutingPipeline::ExecutingPipeline(std::shared_ptr<Processors> processors_, QueryStatusPtr process_list_element_, const StepWallClockRegistry * wall_clocks_)
    : profile_processors(profileProcessors(process_list_element_))
    , trace_processors(traceProcessors(process_list_element_))
    , process_list_element(std::move(process_list_element_))
    , wall_clocks(wall_clocks_)
    , states(std::move(processors_))
{
}

std::vector<ProcessorState *> ExecutingPipeline::sinks()
{
    std::vector<ProcessorState *> result;
    states.forEachProcessor([&](IProcessor & processor, ProcessorState & state)
    {
        const bool has_connected_output = std::ranges::any_of(processor.getOutputs(), [](const OutputPort & output) { return output.isConnected(); });
        if (!has_connected_output)
            result.push_back(&state);
    });

    std::ranges::reverse(result);
    return result;
}

std::vector<ProcessorState *> ExecutingPipeline::updateProcessors(ProcessorState & requester, const Processors & to_add, const Processors & to_reconnect)
{
    auto updated = states.update(requester, to_add, to_reconnect);

    const auto reason = cancel_reason.load();
    if (reason != IProcessor::CancelReason::NotCancelled)
        for (const auto & processor : to_add)
            processor->cancel(reason);

    return updated;
}

void ExecutingPipeline::submitForRemoval(Processors group)
{
    removals.submit(std::move(group));
}

void ExecutingPipeline::recordAsFinished(IProcessor & processor)
{
    removals.onFinished(&processor);
}

bool ExecutingPipeline::hasReadyForRemoval() const
{
    return removals.hasReady();
}

void ExecutingPipeline::removeReady()
{
    Processors ready = removals.takeReadyForRemoval();
    checkConnectedOnlyInside(ready);
    states.remove(ready);
}

void ExecutingPipeline::cancel(IProcessor::CancelReason reason)
{
    auto current = cancel_reason.load();
    while (supersedes(current, reason) && !cancel_reason.compare_exchange_weak(current, reason))
    {
    }

    const auto reason_in_force = cancel_reason.load();
    states.forEachProcessor([&](IProcessor & processor, ProcessorState &) { processor.cancel(reason_in_force); });
}

void ExecutingPipeline::fail(std::exception_ptr exception_)
{
    if (!failed.exchange(true))
        exception = std::move(exception_);

    cancel(IProcessor::CancelReason::Exception);
}

bool ExecutingPipeline::cancelled() const
{
    const auto reason = cancel_reason.load();
    return reason != IProcessor::CancelReason::NotCancelled && reason != IProcessor::CancelReason::PartialResult;
}

bool ExecutingPipeline::allFinished()
{
    bool all_finished = true;
    states.forEachProcessor([&](IProcessor &, ProcessorState & state) { all_finished = all_finished && state.lock.isFinished(); });
    return all_finished;
}

void ExecutingPipeline::reportReadProgress(ReadProgressCallback & callback)
{
    states.forEachProcessor([&](IProcessor & processor, ProcessorState &)
    {
        auto progress = processor.getReadProgress();
        if (!progress)
            return;

        if (progress->counters.total_rows_approx)
            callback.addTotalRowsApprox(progress->counters.total_rows_approx);

        if (progress->counters.total_bytes)
            callback.addTotalBytes(progress->counters.total_bytes);

        if (progress->counters.read_rows || progress->counters.read_bytes)
            callback.onProgress(progress->counters.read_rows, progress->counters.read_bytes, progress->limits);
    });
}

String ExecutingPipeline::dump() const
{
    return states.dump();
}

}
