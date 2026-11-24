#include <Processors/Streaming/ReadToEndTransform.h>

#include <Processors/Port.h>

namespace DB
{

ReadToEndTransform::ReadToEndTransform(SharedHeader header)
    : IProcessor({header}, {header})
    , input(inputs.front())
    , output(outputs.front())
{
}

IProcessor::Status ReadToEndTransform::prepare()
{
    if (output.isFinished())
    {
        /// Nobody needs the rows anymore, but the source has to reach the end of the stream: do not close the input.
        if (input.isFinished())
            return Status::Finished;

        input.setNeeded();
        if (input.hasData())
            input.pull();

        return Status::NeedData;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    output.push(input.pull(/*set_not_needed=*/ true));
    return Status::PortFull;
}

}
