#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>

namespace DB
{

size_t MergeTreeDataPartWriterWide::write(const Block & block, size_t from_mark, size_t index_offset,
        const MergeTreeIndexGranularity & index_granularity,
        const Block & primary_key_block, const Block & skip_indexes_block)
{
    if (serialization_states.empty())
    {
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

    WrittenOffsetColumns offset_columns;
    size_t new_index_offset = 0;

    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        const ColumnWithTypeAndName & column = block.getByName(it->name);

        if (permutation)
        {
            if (primary_key_block.has(it->name))
            {
                const auto & primary_column = *primary_key_block.getByName(it->name).column;
                std::tie(std::ignore, new_index_offset) = writeColumn(column.name, *column.type, primary_column, offset_columns, false, serialization_states[i], from_mark, index_offset);
            }
            else if (skip_indexes_block.has(it->name))
            {
                const auto & index_column = *skip_indexes_block.getByName(it->name).column;
                std::tie(std::ignore, new_index_offset) = writeColumn(column.name, *column.type, index_column, offset_columns, false, serialization_states[i], from_mark, index_offset);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                ColumnPtr permuted_column = column.column->permute(*permutation, 0);
                std::tie(std::ignore, new_index_offset) = writeColumn(column.name, *column.type, *permuted_column, offset_columns, false, serialization_states[i], from_mark, index_offset);
            }
        }
        else
        {
            std::tie(std::ignore, new_index_offset) = writeColumn(column.name, *column.type, *column.column, offset_columns, false, serialization_states[i], from_mark, index_offset);
        }
    }

    return new_index_offset;
}

void MergeTreeDataPartWriterWide::writeSingleMark(
    const String & name,
    const IDataType & type,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    size_t number_of_rows,
    DB::IDataType::SubstreamPath & path)
{
     type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path)
     {
         bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
         if (is_offsets && skip_offsets)
             return;

         String stream_name = IDataType::getFileNameForStream(name, substream_path);

         /// Don't write offsets more than one time for Nested type.
         if (is_offsets && offset_columns.count(stream_name))
             return;

         ColumnStream & stream = *column_streams[stream_name];

         /// There could already be enough data to compress into the new block.
         if (stream.compressed.offset() >= min_compress_block_size)
             stream.compressed.next();

         writeIntBinary(stream.plain_hashing.count(), stream.marks);
         writeIntBinary(stream.compressed.offset(), stream.marks);
         if (can_use_adaptive_granularity)
             writeIntBinary(number_of_rows, stream.marks);
     }, path);
}

size_t MergeTreeDataPartWriterWide::writeSingleGranule(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    IDataType::SerializeBinaryBulkSettings & serialize_settings,
    size_t from_row,
    size_t number_of_rows,
    bool write_marks)
{
    if (write_marks)
        writeSingleMark(name, type, offset_columns, skip_offsets, number_of_rows, serialize_settings.path);

    type.serializeBinaryBulkWithMultipleStreams(column, from_row, number_of_rows, serialize_settings, serialization_state);

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets && skip_offsets)
            return;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return;

        column_streams[stream_name]->compressed.nextIfAtEnd();
    }, serialize_settings.path);

    return from_row + number_of_rows;
}

/// column must not be empty. (column.size() !== 0)

std::pair<size_t, size_t> MergeTreeDataPartWriterWide::writeColumn(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    size_t from_mark)
{
    auto & settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name, offset_columns, skip_offsets);
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;

    size_t total_rows = column.size();
    size_t current_row = 0;
    size_t current_column_mark = from_mark;
    while (current_row < total_rows)
    {
        size_t rows_to_write;
        bool write_marks = true;

        /// If there is `index_offset`, then the first mark goes not immediately, but after this number of rows.
        if (current_row == 0 && index_offset != 0)
        {
            write_marks = false;
            rows_to_write = index_offset;
        }
        else
        {
            if (index_granularity.getMarksCount() <= current_column_mark)
                throw Exception(
                    "Incorrect size of index granularity expect mark " + toString(current_column_mark) + " totally have marks " + toString(index_granularity.getMarksCount()),
                    ErrorCodes::LOGICAL_ERROR);

            rows_to_write = index_granularity.getMarkRows(current_column_mark);
        }

        current_row = writeSingleGranule(
            name,
            type,
            column,
            offset_columns,
            skip_offsets,
            serialization_state,
            serialize_settings,
            current_row,
            rows_to_write,
            write_marks
        );

        if (write_marks)
            current_column_mark++;
    }

    /// Memoize offsets for Nested types, that are already written. They will not be written again for next columns of Nested structure.
    type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = IDataType::getFileNameForStream(name, substream_path);
            offset_columns.insert(stream_name);
        }
    }, serialize_settings.path);

    return std::make_pair(current_column_mark, current_row - total_rows);


void MergeTreeDataPartWriterWide::start()
{
    if (started)
        return;

    started = true;

}

}
