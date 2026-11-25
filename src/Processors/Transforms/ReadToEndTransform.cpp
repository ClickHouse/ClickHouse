#include <Processors/Transforms/ReadToEndTransform.h>

#include <Processors/Port.h>

namespace DB
{

ReadToEndTransform::ReadToEndTransform(SharedHeader header)
    : IProcessor({header}, {header})
{
}

IProcessor::Status ReadToEndTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        /// Nobody needs the data anymore, but the source has to reach its end.
        if (input.isFinished())
            return Status::Finished;

        input.setNeeded();
        if (input.hasData())
            input.pull(/*set_not_needed=*/ false);

        return Status::NeedData;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.hasData())
    {
        output.push(input.pull(/*set_not_needed=*/ true));
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    return Status::NeedData;
}

}
