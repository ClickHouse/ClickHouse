#include <Processors/ISink.h>


namespace DB
{

ISink::ISink(Block header)
    : IProcessor({std::move(header)}, {}), input(inputs.front())
{
}

ISink::Status ISink::prepare()
{
    if (!was_on_start_called)
        return Status::Ready;

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
    if (!was_on_start_called)
    {
        was_on_start_called = true;
        onStart();
    }
    else if (has_input)
    {
        has_input = false;
        consume(std::move(current_chunk));
    }
    else if (!was_on_finish_called)
    {
        was_on_finish_called = true;
        onFinish();
    }
}

}
