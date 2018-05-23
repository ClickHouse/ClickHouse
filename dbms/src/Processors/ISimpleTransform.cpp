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
    if (!output.isNeeded())
        return Status::Unneeded;

    if (current_block)
    {
        if (!transformed)
            return Status::Ready;
        else if (output.hasData())
            return Status::PortFull;
        else
            output.push(std::move(current_block));
    }

    if (input.hasData())
    {
        current_block = input.pull();
        transformed = false;
        return Status::Ready;
    }

    if (input.isFinished())
        return Status::Finished;

    input.setNeeded();
    return Status::NeedData;
}

void ISimpleTransform::work()
{
    transform(current_block);
    transformed = true;
}

}

