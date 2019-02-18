#include <Processors/ISimpleTransform.h>


namespace DB
{

ISimpleTransform::ISimpleTransform(Block input_header, Block output_header)
    : IProcessor({std::move(input_header)}, {std::move(output_header)}),
    input(inputs.front()), output(outputs.front())
{
}

ISimpleTransform::Status ISimpleTransform::prepare()
{
    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (transformed)
    {
        output.push(std::move(current_chunk));
        has_input = false;
        transformed = false;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        current_chunk = input.pull();
        has_input = true;
    }

    /// Now transform.
    return Status::Ready;
}

void ISimpleTransform::work()
{
    transform(current_chunk);
    transformed = true;
}

}

