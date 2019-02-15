#include <Processors/ISink.h>


namespace DB
{

ISink::ISink(Block header)
    : IProcessor({std::move(header)}, {}), input(inputs.front())
{
}

ISink::Status ISink::prepare()
{
    if (has_input)
        return Status::Ready;

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_block = input.pull();
    has_input = true;
    return Status::Ready;
}

void ISink::work()
{
    consume(std::move(current_block));
}

}

