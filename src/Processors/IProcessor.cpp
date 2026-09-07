#include <Processors/IProcessor.h>

#include <iostream>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/CurrentThread.h>

#ifdef OS_LINUX
#include <sys/epoll.h>
#endif


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

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

IProcessor::Status IProcessor::prepare()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'prepare' is not implemented for {} processor", getName());
}

void IProcessor::work()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'work' is not implemented for {} processor", getName());
}

int IProcessor::schedule()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'schedule' is not implemented for {} processor", getName());
}

#ifdef OS_LINUX
std::pair<int, uint32_t> IProcessor::scheduleForEvent()
{
    return {schedule(), EPOLLIN | EPOLLERR};
}
#endif

Processors IProcessor::expandPipeline()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'expandPipeline' is not implemented for {} processor", getName());
}

void IProcessor::cancel() noexcept
{

    bool already_cancelled = is_cancelled.exchange(true, std::memory_order_acq_rel);
    if (already_cancelled)
        return;

    onCancel();
}

UInt64 IProcessor::getInputPortNumber(const InputPort * input_port) const
{
    UInt64 number = 0;
    for (const auto & port : inputs)
    {
        if (&port == input_port)
            return number;

        ++number;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find input port for {} processor", getName());
}

UInt64 IProcessor::getOutputPortNumber(const OutputPort * output_port) const
{
    UInt64 number = 0;
    for (const auto & port : outputs)
    {
        if (&port == output_port)
            return number;

        ++number;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find output port for {} processor", getName());
}

IProcessor::ProcessorDataStats IProcessor::getProcessorDataStats() const
{
    ProcessorDataStats stats;

    for (const auto & input : inputs)
    {
        stats.input_rows += input.rows;
        stats.input_bytes += input.bytes;
    }

    for (const auto & output : outputs)
    {
        stats.output_rows += output.rows;
        stats.output_bytes += output.bytes;
    }

    return stats;
}

IProcessor::ProcessorsProfileLogInfo IProcessor::getProcessorsProfileLogInfo() const
{
    ProcessorsProfileLogInfo info;

    auto get_proc_id = [](const IProcessor & proc) -> UInt64 { return reinterpret_cast<std::uintptr_t>(&proc); };

    info.id = get_proc_id(*this);

    for (const auto & port : outputs)
    {
        if (!port.isConnected())
            continue;
        const IProcessor & next = port.getInputPort().getProcessor();
        info.parent_ids.push_back(get_proc_id(next));
    }

    info.plan_step = reinterpret_cast<std::uintptr_t>(query_plan_step);
    info.plan_step_name = plan_step_name;
    info.plan_step_description = plan_step_description;
    info.plan_group = query_plan_step_group;
    info.processor_uniq_id = getUniqID();
    info.step_uniq_id = step_uniq_id;

    info.processor_name = getName();

    info.elapsed_us = static_cast<UInt64>(elapsed_ns / 1000U);
    info.input_wait_elapsed_us = static_cast<UInt64>(input_wait_elapsed_ns / 1000U);
    info.output_wait_elapsed_us = static_cast<UInt64>(output_wait_elapsed_ns / 1000U);

    auto stats = getProcessorDataStats();
    info.input_rows = stats.input_rows;
    info.input_bytes = stats.input_bytes;
    info.output_rows = stats.output_rows;
    info.output_bytes = stats.output_bytes;

    return info;
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
