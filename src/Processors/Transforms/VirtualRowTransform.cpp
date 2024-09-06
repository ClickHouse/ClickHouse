#include <Processors/Transforms/VirtualRowTransform.h>
#include "Processors/Chunk.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VirtualRowTransform::VirtualRowTransform(const Block & header)
    : IProcessor({header}, {header})
    , input(inputs.front()), output(outputs.front())
{
}

VirtualRowTransform::Status VirtualRowTransform::prepare()
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
        return Status::PortFull;
    }

    if (can_generate)
        return Status::Ready;

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

        /// Set input port NotNeeded after chunk was pulled.
        current_chunk = input.pull(true);
        has_input = true;
    }

    /// Now transform.
    return Status::Ready;
}

void VirtualRowTransform::work()
{
    if (can_generate)
    {
        if (generated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "VirtualRowTransform cannot consume chunk because it already was generated");

        current_chunk = generate();
        generated = true;
        can_generate = false;
    }
    else
    {
        if (!has_input)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "VirtualRowTransform cannot consume chunk because it wasn't read");

        consume(std::move(current_chunk));
        has_input = false;
        can_generate = true;
    }
}

void VirtualRowTransform::consume(Chunk chunk)
{
    if (!is_first)
    {
        temp_chunk = std::move(chunk);
        return;
    }

    is_first = false;
    temp_chunk = std::move(chunk);
}

Chunk VirtualRowTransform::generate()
{
    if (temp_chunk.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in VirtualRowTransform");

    Chunk result;
    result.swap(temp_chunk);
    return result;
}

}
