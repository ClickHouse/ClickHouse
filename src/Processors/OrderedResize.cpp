#include <Processors/OrderedResize.h>


namespace DB
{

IProcessor::Status OrderedScatterProcessor::prepare()
{
    auto & input = inputs.front();
    auto & output = *current_output;

    /// The next chunk may go only to `current_output`, so a finished output stops the whole processor.
    if (output.isFinished())
    {
        input.close();
        for (auto & out : outputs)
            out.finish();

        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    input.setNeeded();

    if (!input.hasData())
    {
        if (input.isFinished())
        {
            for (auto & out : outputs)
                out.finish();

            return Status::Finished;
        }

        return Status::NeedData;
    }

    output.push(input.pull());

    if (++current_output == outputs.end())
        current_output = outputs.begin();

    return Status::PortFull;
}

IProcessor::Status OrderedGatherProcessor::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();

        return Status::Finished;
    }

    /// Every input is kept needed even while we cannot push, otherwise only the branch we are
    /// waiting for would be allowed to work.
    for (auto & in : inputs)
        if (!in.isFinished())
            in.setNeeded();

    if (!output.canPush())
        return Status::PortFull;

    auto & input = *current_input;

    if (!input.hasData())
    {
        if (input.isFinished())
        {
            /// Chunks were dispatched in the same order, so the remaining inputs have nothing left either.
            for (auto & in : inputs)
                in.close();

            output.finish();
            return Status::Finished;
        }

        return Status::NeedData;
    }

    output.push(input.pull());

    if (++current_input == inputs.end())
        current_input = inputs.begin();

    return Status::PortFull;
}

}
