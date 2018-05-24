#include <Processors/ConcatProcessor.h>


namespace DB
{

ConcatProcessor::Status ConcatProcessor::prepare()
{
    auto & output = outputs[0];

    if (output.hasData())
        return Status::PortFull;

    if (!output.isNeeded())
    {
        for (auto & input : inputs)
            input.setNotNeeded();

        return Status::Unneeded;
    }

    if (current_input == inputs.end())
        return Status::Finished;

    auto & input = *current_input;

    input.setNeeded();

    if (input.hasData())
    {
        output.push(input.pull());
    }

    if (input.isFinished())
    {
        input.setNotNeeded();

        ++current_input;
        if (current_input == inputs.end())
        {
            output.setFinished();
            return Status::Finished;
        }

        current_input->setNeeded();
    }

    return Status::NeedData;
}

}


