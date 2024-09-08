#include <Processors/Transforms/VirtualRowTransform.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

VirtualRowTransform::VirtualRowTransform(const Block & header_,
    const KeyDescription & primary_key_,
    const IMergeTreeDataPart::Index & index_,
    size_t mark_range_begin_)
    : IProcessor({header_}, {header_})
    , input(inputs.front()), output(outputs.front())
    , header(header_), primary_key(primary_key_)
    , index(index_), mark_range_begin(mark_range_begin_)
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

        /// Reorder the columns according to result_header
        Columns ordered_columns;
        ordered_columns.reserve(header.columns());
        for (size_t i = 0, j = 0; i < header.columns(); ++i)
        {
            const ColumnWithTypeAndName & type_and_name = header.getByPosition(i);
            ColumnPtr current_column = type_and_name.type->createColumn();
            // ordered_columns.push_back(current_column->cloneResized(1));

            if (j < index->size() && type_and_name.name == primary_key.column_names[j] 
                && type_and_name.type == primary_key.data_types[j])
            {
                auto column = current_column->cloneEmpty();
                column->insert((*(*index)[j])[mark_range_begin]);
                ordered_columns.push_back(std::move(column));
                ++j;
            }
            else
                ordered_columns.push_back(current_column->cloneResized(1));
        }

        current_chunk.setColumns(ordered_columns, 1);
        current_chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(0, true));
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
