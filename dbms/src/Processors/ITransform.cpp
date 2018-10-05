#include <Processors/ITransform.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int PROCESSOR_CANNOT_PROCESS_ALL_DATA;
}

static InputPorts createInputPorts(Blocks && input_headers)
{
    InputPorts ports;
    ports.reserve(input_headers.size());
    for (auto & header : input_headers)
        ports.emplace_back(std::move(header));
    return ports;
}

static OutputPorts createOutputPorts(Blocks && output_headers)
{
    OutputPorts ports;
    ports.reserve(output_headers.size());
    for (auto & header : output_headers)
        ports.emplace_back(std::move(header));
    return ports;
}

ITransform::ITransform(Blocks input_headers, Blocks output_headers)
    : IProcessor(createInputPorts(std::move(input_headers)), createOutputPorts(std::move(output_headers)))
{
}

static bool hasNeededOutput(const OutputPorts & outputs)
{
    for (const auto & output : outputs)
        if (output.isNeeded())
            return true;

    return false;
}

static bool hasFullPort(const OutputPorts & outputs)
{
    for (const auto & output : outputs)
        if (output.isNeeded() && output.hasData())
            return true;

    return false;
}

static bool allHasData(const InputPorts & inputs)
{
    for (const auto & input : inputs)
        if (!input.hasData())
            return false;

    return true;
}

static bool allHasOrNeedData(const InputPorts & inputs)
{
    for (const auto & input : inputs)
        if (input.isFinished() && !input.hasData())
            return false;

    return true;
}

static bool allFinishedAndHasNoData(const InputPorts & inputs)
{
    for (const auto & input : inputs)
        if (!input.isFinished() || input.hasData())
            return false;

    return true;
}

static void pushToOutputPorts(Blocks & blocks, OutputPorts & ports)
{
    size_t num_outputs = blocks.size();
    for (size_t i = 0; i < num_outputs; ++i)
        if (ports[i].isNeeded())
            ports[i].push(std::move(blocks[i]));

    blocks.clear();
}

static void pullFromInputPorts(Blocks & blocks, InputPorts & ports)
{
    size_t num_inputs = ports.size();
    for (size_t i = 0; i < num_inputs; ++i)
        blocks.emplace_back(ports[i].pull());
}

static void setAllNeededIfHasNoData(InputPorts & ports)
{
    for (auto & port : ports)
        if (!port.hasData())
            port.setNeeded();
}

static void setAllFinished(OutputPorts & ports)
{
    for (auto & port : ports)
        port.setFinished();
}

ITransform::Status ITransform::prepare()
{
    if (!hasNeededOutput(outputs))
        return Status::Unneeded;

    if (!input_blocks.empty())
        return Status::Ready;

    if (!output_blocks.empty())
    {
        if (hasFullPort(outputs))
            return Status::PortFull;

        pushToOutputPorts(output_blocks, outputs);
    }

    bool all_has_or_need_data = allHasOrNeedData(inputs);
    bool all_has_data =  allHasData(inputs);
    bool all_finished_and_has_no_data = allFinishedAndHasNoData(inputs);

    if (all_has_data)
    {
        pullFromInputPorts(input_blocks, inputs);
        return Status::Ready;
    }

    /// Has input without data.

    if (all_has_or_need_data)
    {
        setAllNeededIfHasNoData(inputs);
        return Status::NeedData;
    }

    /// Has finished input without data.

    if (all_finished_and_has_no_data)
    {
        setAllFinished(outputs);
        return Status::Finished;
    }

    /// Has finished input without data and input with pending data. Let's throw exception.

    size_t finished_port_without_data = 0;
    size_t port_with_pending_data = 0;
    bool port_with_pending_data_has_data = false;

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (inputs[i].isFinished() && !inputs[i].hasData())
        {
            finished_port_without_data = i;
            continue;
        }

        port_with_pending_data = i;
        port_with_pending_data_has_data = inputs[i].hasData();
    }

    throw Exception(
            "Transform processor cannot transform all data because port "
            + toString(finished_port_without_data) + " is finished and has no data, but port "
            + toString(port_with_pending_data) + (port_with_pending_data_has_data ? " has data" : "is not finished"),
            ErrorCodes::PROCESSOR_CANNOT_PROCESS_ALL_DATA);
}

void ITransform::work()
{
    output_blocks = transform(std::move(input_blocks));
}

}
