#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>

namespace DB
{

size_t MergeTreeDataPartWriterCompact::writeColumnSingleGranule(
    const ColumnWithTypeAndName & column,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    IDataType::SerializeBinaryBulkSettings & serialize_settings,
    size_t from_row,
    size_t number_of_rows)
{
}


size_t MergeTreeDataPartWriterCompact::write(const Block & block, size_t from_mark, size_t offset,
    const Block & primary_key_block, const Block & skip_indexes_block)
{
    if (!started)
        start();

    size_t total_rows = block.rows();  
    size_t current_mark = from_mark;
    size_t current_row = 0;


    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = [&ostr](IDataType::SubstreamPath) -> WriteBuffer * { return &ostr; };
    serialize_settings.position_independent_encoding = false;
    serialize_settings.low_cardinality_max_dictionary_size = 0;

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
        if (current_row == 0 && offset != 0)
        {
            rows_to_write = offset;
            write_marks = false;
        }
        else
        {
            rows_to_write = index_granularity->getMarkRows(current_mark);
        } 

        for (size_t i = 0; i < columns_to_write.size(); ++i)
        {
            current_row = writeColumnSingleGranule(columns_to_write[i], offset_columns, skip_offsets, serialization_states[i], serialize_settings, current_row, rows_to_write);
        }

        if (write_marks)
        {
            writeMark();
            ++current_mark;
        }
    }

    /// We always write end granule for block in Compact parts.
    return 0;
}

size_t MergeTreeDataPartWriterCompact::writeColumnSingleGranule(const ColumnWithTypeAndName & column,
        WrittenOffsetColumns & offset_columns,
        bool skip_offsets,
        IDataType::SerializeBinaryBulkStatePtr & serialization_state,
        IDataType::SerializeBinaryBulkSettings & serialize_settings,
        size_t from_row,
        size_t number_of_rows)
{
    column.type->serializeBinaryBulkStatePrefix(serialize_settings, serialization_state);
    column.type->serializeBinaryBulkWithMultipleStreams(*column.column, from_row, number_of_rows, serialize_settings, serialization_state);
    column.type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_state);
}

void MergeTreeDataPartWriterWide::start()
{
    if (started)
        return;

    started = true;

    serialization_states.reserve(columns_list.size());
    WrittenOffsetColumns tmp_offset_columns;
    IDataType::SerializeBinaryBulkSettings settings;

    for (const auto & col : columns_list)
    {
        settings.getter = createStreamGetter(col.name, tmp_offset_columns, false);
        serialization_states.emplace_back(nullptr);
        col.type->serializeBinaryBulkStatePrefix(settings, serialization_states.back());
    }
}

}