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
    {
        if (!was_on_finish_called)
            return Status::Ready;

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void ISink::work()
{
    if (has_input)
    {
        consume(std::move(current_chunk));
        has_input = false;
    }
    else
    {
        onFinish();
        was_on_finish_called = true;
    }
}

}
