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
    if (!output.isNeeded())
        return Status::Unneeded;

    if (current_input_block)
        return Status::Ready;

    if (current_output_block)
    {
        if (output.hasData())
            return Status::PortFull;
        else
            output.push(std::move(current_output_block));
    }

    if (input.hasData())
    {
        current_input_block = input.pull();
        return Status::Ready;
    }

    if (input.isFinished())
    {
        if (finished)
            return Status::Finished;

        return Status::Ready;
    }

    input.setNeeded();
    return Status::NeedData;
}

void IAccumulatingTransform::work()
{
    if (current_input_block)
    {
        consume(std::move(current_input_block));
    }
    else
    {
        current_output_block = generate();
        if (!current_output_block)
            finished = true;
    }
}

}

