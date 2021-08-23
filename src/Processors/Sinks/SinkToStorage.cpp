#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{


ExceptionKeepingTransform::ExceptionKeepingTransform(const Block & in_header, const Block & out_header)
    : IProcessor({in_header}, {out_header})
    , input(inputs.front()), output(outputs.front())
{
}

IProcessor::Status ExceptionKeepingTransform::prepare()
{
    if (!was_on_start_called)
        return Status::Ready;

    /// Check can output.

    if (output.isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Output port is finished for {}", getName());

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (ready_output)
    {
        output.pushData(std::move(data));
        ready_output = false;
        return Status::PortFull;
    }

    if (!ready_input)
    {
        if (input.isFinished())
        {
            if (!was_on_finish_called)
                return Status::Ready;

            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        data = input.pullData(true);

        if (data.exception)
        {
            output.pushData(std::move(data));
            return Status::PortFull;
        }

        ready_input = true;
    }

    return Status::Ready;
}

void ExceptionKeepingTransform::work()
{
    if (!was_on_start_called)
    {
        was_on_start_called = true;
        onStart();
    }

    if (ready_input)
    {
        ready_input = false;
        ready_output = true;

        try
        {
            transform(data.chunk);
        }
        catch (...)
        {
            data.chunk.clear();
            data.exception = std::current_exception();
        }
    }
    else if (!was_on_finish_called)
    {
        was_on_finish_called = true;
        try
        {
            onFinish();
        }
        catch (...)
        {
            ready_input = true;
            data.exception = std::current_exception();
        }
    }
}

SinkToStorage::SinkToStorage(const Block & header) : ExceptionKeepingTransform(header, header) {}

void SinkToStorage::transform(Chunk & chunk)
{
    consume(chunk.clone());
}

}
