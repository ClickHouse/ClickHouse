#include <Processors/ForkProcessor.h>


namespace DB
{

ForkProcessor::Status ForkProcessor::prepare()
{
    auto & input = inputs[0];

    bool all_outputs_unneeded = true;

    for (const auto & output : outputs)
    {
        if (output.isNeeded())
        {
            all_outputs_unneeded = false;
            if (output.hasData())
                return Status::PortFull;
        }
    }

    if (all_outputs_unneeded)
    {
        input.setNotNeeded();
        return Status::Unneeded;
    }

    input.setNeeded();

    if (!input.hasData())
    {
        if (input.isFinished())
        {
            input.setNotNeeded();
            return Status::Finished;
        }
        else
            return Status::NeedData;
    }

    auto data = input.pull();

    for (auto & output : outputs)
        if (output.isNeeded())
            output.push(data);

    return Status::NeedData;
}

}



