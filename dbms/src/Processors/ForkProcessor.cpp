#include <Processors/ForkProcessor.h>


namespace DB
{

ForkProcessor::Status ForkProcessor::prepare()
{
    auto & input = inputs.front();

    /// Check can output.

    bool all_finished = true;
    bool all_can_push = true;

    for (const auto & output : outputs)
    {
        if (!output.isFinished())
        {
            all_finished = false;

            /// The order is important.
            if (!output.canPush())
                all_can_push = false;
        }
    }

    if (all_finished)
    {
        input.close();
        return Status::Finished;
    }

    if (!all_can_push)
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Check can input.

    if (input.isFinished())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    /// Move data.

    auto data = input.pull();

    for (auto & output : outputs)
        if (!output.isFinished())  /// Skip finished outputs.
            output.push(data);  /// Can push because no full or unneeded outputs.

    /// Now, we pulled from input. It must be empty.
    return Status::NeedData;
}

}



