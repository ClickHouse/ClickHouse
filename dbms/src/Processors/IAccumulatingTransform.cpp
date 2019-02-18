#include <Processors/IAccumulatingTransform.h>


namespace DB
{

IAccumulatingTransform::IAccumulatingTransform(Block input_header, Block output_header)
    : IProcessor({std::move(input_header)}, {std::move(output_header)}),
    input(inputs.front()), output(outputs.front())
{
}

IAccumulatingTransform::Status IAccumulatingTransform::prepare()
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
    if (current_output_chunk)
        output.push(std::move(current_output_chunk));

    if (finished_generate)
    {
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!has_input)
    {
        /// Generate output block.
        if (input.isFinished())
        {
            finished_input = true;
            return Status::Ready;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        current_input_chunk = input.pull();
        has_input = true;
    }

    return Status::Ready;
}

void IAccumulatingTransform::work()
{
    if (!finished_input)
    {
        consume(std::move(current_input_chunk));
        has_input = false;
    }
    else
    {
        current_output_chunk = generate();
        if (!current_output_chunk)
            finished_generate = true;
    }
}

}

