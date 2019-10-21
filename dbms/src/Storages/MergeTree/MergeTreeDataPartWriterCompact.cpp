#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>

namespace DB
{

size_t MergeTreeDataPartWriterCompact::write(const Block & block, size_t from_mark, size_t index_offset,
    const MergeTreeIndexGranularity & index_granularity,
    const Block & primary_key_block, const Block & skip_indexes_block)
{
    size_t total_rows = block.rows();  
    size_t current_mark = from_mark;
    size_t current_row = 0;

    ColumnsWithTypeAndName columns_to_write(columns_list.size());
    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        if (permutation)
        {
            if (primary_key_block.has(it->name))
                columns_to_write[i] = primary_key_block.getByName(it->name);
            else if (skip_indexes_block.has(it->name))
                columns_to_write[i] = skip_indexes_block.getByName(it->name);
            else
            {
                columns_to_write[i] = block.getByName(it->name);
                columns_to_write[i].column = columns_to_write[i].column->permute(*permutation, 0);
            }
        }
        else
            columns_to_write[i] = block.getByName(it->name);
    }

    while (current_row < total_rows)
    {
        bool write_marks = true;
        size_t rows_to_write;
        if (current_row == 0 && index_offset != 0)
        {
            rows_to_write = index_offset;
            write_marks = false;
        }
        else
        {
            rows_to_write = index_granularity.getMarkRows(current_mark);
        }

        if (write_marks)
        {
            writeIntBinary(rows_to_write, stream->marks);
            for (size_t i = 0; i < columns_to_write.size(); ++i)
            {
                writeIntBinary(stream->plain_hashing.count(), stream->marks);
                writeIntBinary(stream->compressed.offset(), stream->marks);
                current_row = writeColumnSingleGranule(columns_to_write[i], current_row, rows_to_write);
            }
            ++current_mark;
        }
        else
        {
            for (size_t i = 0; i < columns_to_write.size(); ++i)
                current_row = writeColumnSingleGranule(columns_to_write[i], current_row, rows_to_write);
        }
    }

    /// We always write end granule for block in Compact parts.
    return 0;
}

size_t MergeTreeDataPartWriterCompact::writeColumnSingleGranule(const ColumnWithTypeAndName & column, size_t from_row, size_t number_of_rows)
{
    IDataType::SerializeBinaryBulkStatePtr state;
    IDataType::SerializeBinaryBulkSettings serialize_settings;

    serialize_settings.getter = [this](IDataType::SubstreamPath) -> WriteBuffer * { return &stream->compressed; };
    serialize_settings.position_independent_encoding = false;
    serialize_settings.low_cardinality_max_dictionary_size = 0;

    column.type->serializeBinaryBulkStatePrefix(serialize_settings, state);
    column.type->serializeBinaryBulkWithMultipleStreams(*column.column, from_row, number_of_rows, serialize_settings, state);
    column.type->serializeBinaryBulkStateSuffix(serialize_settings, state);

    return from_row + number_of_rows;
}

}
