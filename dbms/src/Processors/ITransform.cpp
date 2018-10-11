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

static bool allUnneededOutput(const OutputPorts & outputs)
{
    for (const auto & output : outputs)
        if (output.isNeeded())
            return false;

    return true;
}

static bool hasUnneededOutput(const OutputPorts & outputs)
{
    for (const auto & output : outputs)
        if (!output.isNeeded())
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

static bool allFinished(const InputPorts & inputs)
{
    for (const auto & input : inputs)
        if (!input.isFinished())
            return false;

    return true;
}

static bool hasFinished(const InputPorts & inputs)
{
    for (const auto & input : inputs)
        if (input.isFinished())
            return true;

    return false;
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

void throwHasNeededAndUnneededOutputs(const OutputPorts & ports)
{
    size_t needed = 0;
    size_t unneeded = 0;
    for (size_t i = 0; i < ports.size(); ++i)
        (ports[i].isNeeded() ? needed : unneeded) = i;

    throw Exception("Transform processor cannot transform all data because output port "
                    + toString(needed) + " is needed, but output port " + toString(unneeded) + " is not",
                    ErrorCodes::PROCESSOR_CANNOT_PROCESS_ALL_DATA);
}

void throwHasFinishedAndUnfinishedInput(const InputPorts & ports)
{
    size_t finished_port = 0;
    size_t unfinished_port = 0;
    bool has_data = false;

    for (size_t i = 0; i < ports.size(); ++i)
    {
        (ports[i].isFinished() ? finished_port : unfinished_port) = i;
        if (!ports[i].isFinished())
            has_data = ports[i].hasData();
    }

    throw Exception(
            "Transform processor cannot transform all data because port "
            + toString(finished_port) + " is finished, but port " + toString(unfinished_port)
            + (has_data ? " has data" : " needs data"),
            ErrorCodes::PROCESSOR_CANNOT_PROCESS_ALL_DATA);
}

ITransform::Status ITransform::prepare()
{
    if (allUnneededOutput(outputs))
        return Status::Unneeded;

    if (hasUnneededOutput(outputs))
        throwHasNeededAndUnneededOutputs(outputs);

    if (!input_blocks.empty())
        return Status::Ready;

    if (!output_blocks.empty())
    {
        if (hasFullPort(outputs))
            return Status::PortFull;

        pushToOutputPorts(output_blocks, outputs);
    }

    bool all_has_data = allHasData(inputs);
    bool all_finished = allFinished(inputs);
    bool has_finished = hasFinished(inputs);

    if (all_has_data)
    {
        pullFromInputPorts(input_blocks, inputs);
        return Status::Ready;
    }

    if (all_finished)
    {
        setAllFinished(outputs);
        return Status::Finished;
    }

    if (!has_finished)
    {
        setAllNeededIfHasNoData(inputs);
        return Status::NeedData;
    }

    throwHasFinishedAndUnfinishedInput(inputs);
    return Status::Finished; /// Never returns.
}

void ITransform::work()
{
    output_blocks = transform(std::move(input_blocks));
    input_blocks.clear();
}

}
