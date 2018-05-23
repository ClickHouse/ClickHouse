#include <Processors/ISink.h>


namespace DB
{

ISink::ISink(Block header)
    : IProcessor({std::move(header)}, {}), input(inputs.front())
{
}

ISink::Status ISink::prepare()
{
    if (current_block)
        return Status::Ready;

    if (input.hasData())
    {
        current_block = input.pull();
        return Status::Ready;
    }

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
}

void ISink::work()
{
    consume(std::move(current_block));
}

}

