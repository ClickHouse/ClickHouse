#include <Processors/Transforms/VirtualRowTransform.h>

#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VirtualRowTransform::VirtualRowTransform(SharedHeader header_, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_)
    : IProcessor({header_}, {header_})
    , input(inputs.front()), output(outputs.front())
    , pk_block(pk_block_)
    , virtual_row_conversions(std::move(virtual_row_conversions_))
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

        generated = true;
        can_generate = false;

        if (!is_first)
        {
            if (current_chunk.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in VirtualRowTransform");
            return;
        }

        is_first = false;

        Columns empty_columns;
        const auto & header = getOutputs().front().getHeader();
        empty_columns.reserve(header.columns());
        for (size_t i = 0; i < header.columns(); ++i)
        {
            const ColumnWithTypeAndName & type_and_name = header.getByPosition(i);
            empty_columns.push_back(type_and_name.type->createColumn()->cloneEmpty());
        }

        current_chunk.setColumns(empty_columns, 0);
        current_chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(0, pk_block, virtual_row_conversions));
    }
    else
    {
        if (!has_input)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "VirtualRowTransform cannot consume chunk because it wasn't read");

        has_input = false;
        can_generate = true;
    }
}

}
