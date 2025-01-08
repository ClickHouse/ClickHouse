#include <iostream>
#include <Processors/IProcessor.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

IProcessor::IProcessor()
{
    processor_index = CurrentThread::isInitialized() ? CurrentThread::get().getNextPipelineProcessorIndex() : 0;
}

IProcessor::IProcessor(InputPorts inputs_, OutputPorts outputs_) : inputs(std::move(inputs_)), outputs(std::move(outputs_))
{
    for (auto & port : inputs)
        port.processor = this;
    for (auto & port : outputs)
        port.processor = this;
    processor_index = CurrentThread::isInitialized() ? CurrentThread::get().getNextPipelineProcessorIndex() : 0;
}

void IProcessor::setQueryPlanStep(IQueryPlanStep * step, size_t group)
{
    query_plan_step = step;
    query_plan_step_group = group;
    if (step)
    {
        plan_step_name = step->getName();
        plan_step_description = step->getStepDescription();
        step_uniq_id = step->getUniqID();
    }
}

void IProcessor::cancel() noexcept
{

    bool already_cancelled = is_cancelled.exchange(true, std::memory_order_acq_rel);
    if (already_cancelled)
        return;

    onCancel();
}

String IProcessor::debug() const
{
    WriteBufferFromOwnString buf;
    writeString(getName(), buf);
    buf.write('\n');

    writeString("inputs (hasData, isFinished):\n", buf);
    for (const auto & port : inputs)
    {
        buf.write('\t');
        writeBoolText(port.hasData(), buf);
        buf.write(' ');
        writeBoolText(port.isFinished(), buf);
        buf.write('\n');
    }

    writeString("outputs (hasData, isNeeded):\n", buf);
    for (const auto & port : outputs)
    {
        buf.write('\t');
        writeBoolText(port.hasData(), buf);
        buf.write(' ');
        writeBoolText(port.isNeeded(), buf);
        buf.write('\n');
    }

    buf.finalize();
    return buf.str();
}

void IProcessor::dump() const
{
    std::cerr << debug();
}


std::string IProcessor::statusToName(std::optional<Status> status)
{
    if (status == std::nullopt)
        return "NotStarted";

    switch (*status)
    {
        case Status::NeedData:
            return "NeedData";
        case Status::PortFull:
            return "PortFull";
        case Status::Finished:
            return "Finished";
        case Status::Ready:
            return "Ready";
        case Status::Async:
            return "Async";
        case Status::ExpandPipeline:
            return "ExpandPipeline";
    }
}

}
