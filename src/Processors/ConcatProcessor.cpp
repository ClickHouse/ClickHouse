#include <Processors/ConcatProcessor.h>


namespace DB
{

ConcatProcessor::ConcatProcessor(const Block & header, size_t num_inputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts{header}), current_input(inputs.begin())
{
}

void ConcatProcessor::prepareInitializeInputs()
{
    for (auto & input : inputs)
        input.setNeeded();
}

ConcatProcessor::Status ConcatProcessor::prepare()
{
    auto & output = outputs.front();

    /// Check can output.

    if (output.isFinished())
    {
        for (; current_input != inputs.end(); ++current_input)
            current_input->close();

        return Status::Finished;
    }

    if (!output.isNeeded())
    {
        if (current_input != inputs.end())
            current_input->setNotNeeded();

        return Status::PortFull;
    }

    if (!output.canPush())
        return Status::PortFull;

    /// Check can input.

    while (current_input != inputs.end() && current_input->isFinished())
        ++current_input;

    if (current_input == inputs.end())
    {
        output.finish();
        return Status::Finished;
    }

    auto & input = *current_input;

    if (!is_initialized)
    {
        prepareInitializeInputs();
        is_initialized = true;
    }

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    /// Move data.
    output.push(input.pull());

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

}


