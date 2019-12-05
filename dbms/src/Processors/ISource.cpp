#include <Processors/ISource.h>


namespace DB
{

ISource::ISource(Block header)
    : IProcessor({}, {std::move(header)}), output(outputs.front())
{
}

ISource::Status ISource::prepare()
{
    if (finished)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can output.
    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (!has_input)
        return Status::Ready;

    output.pushData(std::move(current_chunk));
    has_input = false;

    /// Now, we pushed to output, and it must be full.
    return Status::PortFull;
}

void ISource::work()
{
    try
    {
        if (auto chunk = generate())
        {
            current_chunk.chunk = std::move(*chunk);

            if (current_chunk.chunk)
                has_input = true;
            else if (reset()) /// if returned chunk is empty, try to reset source and try again next time.
                has_input = false;
            else
                finished = true;
        }
        else
            finished = true;
    }
    catch (...)
    {
        finished = true;
        throw;
    }
}

}
