#include <Processors/ISimpleTransform.h>


namespace DB
{

ISimpleTransform::ISimpleTransform(Block input_header_, Block output_header_, bool skip_empty_chunks_)
    : IProcessor({std::move(input_header_)}, {std::move(output_header_)})
    , input(inputs.front())
    , output(outputs.front())
    , skip_empty_chunks(skip_empty_chunks_)
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
        output.pushData(std::move(current_data));
        transformed = false;

        if (!no_more_data_needed)
            return Status::PortFull;

    }

    /// Stop if don't need more data.
    if (no_more_data_needed)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        current_data = input.pullData(true);
        has_input = true;

        if (current_data.exception)
        {
            /// Skip transform in case of exception.
            has_input = false;
            transformed = true;

            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();
        }

        if (set_input_not_needed_after_read)
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}

void ISimpleTransform::work()
{
    if (current_data.exception)
        return;

    try
    {
        transform(current_data.chunk);
    }
    catch (DB::Exception &)
    {
        current_data.exception = std::current_exception();
        transformed = true;
        has_input = false;
        return;
    }

    has_input = !needInputData();

    if (!skip_empty_chunks || current_data.chunk)
        transformed = true;

    if (transformed && !current_data.chunk)
        /// Support invariant that chunks must have the same number of columns as header.
        current_data.chunk = Chunk(getOutputPort().getHeader().cloneEmpty().getColumns(), 0);
}

}

