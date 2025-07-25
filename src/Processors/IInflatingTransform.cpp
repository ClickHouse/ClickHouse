#include <Processors/IInflatingTransform.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IInflatingTransform::IInflatingTransform(Block input_header, Block output_header)
    : IProcessor({std::move(input_header)}, {std::move(output_header)})
    , input(inputs.front()), output(outputs.front())
{

}

IInflatingTransform::Status IInflatingTransform::prepare()
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
    if (generated)
    {
        output.push(std::move(current_chunk));
        generated = false;
    }

    if (can_generate)
        return Status::Ready;

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            if (is_finished)
            {
                output.finish();
                return Status::Finished;
            }
            is_finished = true;
            return Status::Ready;
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

void IInflatingTransform::work()
{
    if (can_generate)
    {
        if (generated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IInflatingTransform cannot consume chunk because it already was generated");

        current_chunk = generate();
        generated = true;
        can_generate = canGenerate();
    }
    else if (is_finished)
    {
        if (can_generate || generated || has_input)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IInflatingTransform cannot finish work because it has generated data or has input data");

        current_chunk = getRemaining();
        generated = !current_chunk.empty();
    }
    else
    {
        if (!has_input)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IInflatingTransform cannot consume chunk because it wasn't read");

        consume(std::move(current_chunk));
        has_input = false;
        can_generate = canGenerate();
    }
}

}
