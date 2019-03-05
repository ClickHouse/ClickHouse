#include <Processors/ISimpleTransform.h>


namespace DB
{

ISimpleTransform::ISimpleTransform(Block input_header, Block output_header, bool skip_empty_chunks)
    : IProcessor({std::move(input_header)}, {std::move(output_header)})
    , input(inputs.front())
    , output(outputs.front())
    , skip_empty_chunks(skip_empty_chunks)
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

        if (set_input_not_needed_after_read)
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}

void ISimpleTransform::work()
{
    transform(current_chunk);

    if (!skip_empty_chunks || current_chunk)
        transformed = true;

    if (transformed && !current_chunk)
        /// Support invariant that chunks must have the same number of columns as header.
        current_chunk = Chunk(getOutputPort().getHeader().cloneEmpty().getColumns(), 0);
}

}

