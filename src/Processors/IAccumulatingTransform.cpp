#include <Processors/IAccumulatingTransform.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IAccumulatingTransform::IAccumulatingTransform(Block input_header, Block output_header)
    : IProcessor({std::move(input_header)}, {std::move(output_header)})
    , input(inputs.front())
    , output(outputs.front())
{
}

IAccumulatingTransform::Status IAccumulatingTransform::prepare()
{
    /// Check can output.
    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();

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

    if (input.isFinished())
        finished_input = true;

    if (finished_input)
    {
        /// Close input if flag was set manually.
        input.close();

        /// Read from totals port if has it.
        if (inputs.size() > 1)
        {
            auto & totals_input = inputs.back();
            if (!totals_input.isFinished())
            {
                totals_input.setNeeded();
                if (!totals_input.hasData())
                    return Status::NeedData;

                totals = totals_input.pull();
                totals_input.close();
            }
        }

        /// Generate output block.
        return Status::Ready;
    }

    /// Check can input.
    if (!has_input)
    {
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

void IAccumulatingTransform::setReadyChunk(Chunk chunk)
{
    if (current_output_chunk)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "IAccumulatingTransform already has input. "
                        "Cannot set another chunk. Probably, setReadyChunk method was called twice per consume().");

    current_output_chunk = std::move(chunk);
}

}

