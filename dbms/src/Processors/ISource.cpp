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
        output.setFinished();
        return Status::Finished;
    }

    if (output.hasData())
        return Status::PortFull;

    if (!output.isNeeded())
        return Status::Unneeded;

    if (current_block)
        output.push(std::move(current_block));

    return Status::Ready;
}

void ISource::work()
{
    current_block = generate();
    if (!current_block)
        finished = true;
}

}

