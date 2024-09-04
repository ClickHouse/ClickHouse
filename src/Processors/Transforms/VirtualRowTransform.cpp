#include <Processors/Transforms/VirtualRowTransform.h>
#include "Processors/Chunk.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VirtualRowTransform::VirtualRowTransform(const Block & header)
    : IInflatingTransform(header, header)
{
}

IInflatingTransform::Status VirtualRowTransform::prepare()
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

        /// Set input port NotNeeded after chunk was pulled.
        current_chunk = input.pull(true);
        has_input = true;
    }

    /// Now transform.
    return Status::Ready;
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

bool VirtualRowTransform::canGenerate()
{
    return !temp_chunk.empty();
}

}
