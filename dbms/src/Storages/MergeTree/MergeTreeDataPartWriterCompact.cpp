#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>

namespace DB
{

namespace
{
    constexpr auto DATA_FILE_NAME = "data";
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}


MergeTreeDataPartWriterCompact::MergeTreeDataPartWriterCompact(
    const String & part_path_,
    const MergeTreeData & storage_,
    const NamesAndTypesList & columns_list_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
: IMergeTreeDataPartWriter(part_path_,
    storage_, columns_list_,
    indices_to_recalc_, marks_file_extension_,
    default_codec_, settings_, index_granularity_, true)
{
    String data_file_name = DATA_FILE_NAME + settings.filename_suffix;
    stream = std::make_unique<ColumnStream>(
        data_file_name,
        part_path + data_file_name, DATA_FILE_EXTENSION,
        part_path + data_file_name, marks_file_extension,
        default_codec,
        settings.max_compress_block_size,
        settings.estimated_size,
        settings.aio_threshold);
}

void MergeTreeDataPartWriterCompact::write(
    const Block & block, const IColumn::Permutation * permutation,
    const Block & primary_key_block, const Block & skip_indexes_block)
{
    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    /// FIXME maybe it's wrong at this stage.
    if (compute_granularity)
        fillIndexGranularity(block);

    Block result_block;

    if (permutation)
    {
        for (const auto & it : columns_list)
        {
            if (primary_key_block.has(it.name))
                result_block.insert(primary_key_block.getByName(it.name));
            else if (skip_indexes_block.has(it.name))
                result_block.insert(skip_indexes_block.getByName(it.name));
            else
            {
                auto column = block.getByName(it.name);
                column.column = column.column->permute(*permutation, 0);
                result_block.insert(column);
            }
        }
    }
    else
    {
        result_block = block;
    }

    if (!header)
        header = result_block.cloneEmpty();

    columns_buffer.add(result_block.mutateColumns());
    size_t last_mark_rows = index_granularity.getLastMarkRows();
    size_t rows_in_buffer = columns_buffer.size();

    if (rows_in_buffer < last_mark_rows)
    {
        /// FIXME need comment
        next_index_offset = last_mark_rows - rows_in_buffer;
        return;
    }

    writeBlock(header.cloneWithColumns(columns_buffer.releaseColumns()));
}

void MergeTreeDataPartWriterCompact::writeBlock(const Block & block)
{
    size_t total_rows = block.rows();
    size_t from_mark = current_mark;
    size_t current_row = 0;

    while (current_row < total_rows)
    {
        size_t rows_to_write = index_granularity.getMarkRows(from_mark);

        if (rows_to_write)
            data_written = true;

        for (const auto & column : columns_list)
        {
            /// There could already be enough data to compress into the new block.
            if (stream->compressed.offset() >= settings.min_compress_block_size)
                stream->compressed.next();

            writeIntBinary(stream->plain_hashing.count(), stream->marks);
            writeIntBinary(stream->compressed.offset(), stream->marks);

            writeColumnSingleGranule(block.getByName(column.name), current_row, rows_to_write);
        }

        ++from_mark;
        size_t rows_written = total_rows - current_row;
        current_row += rows_to_write;

        if (current_row >= total_rows && rows_written != rows_to_write)
        {
            rows_to_write = rows_written;
            index_granularity.popMark();
            index_granularity.appendMark(rows_written);
        }

        writeIntBinary(rows_to_write, stream->marks);
    }

    next_index_offset = 0;
    next_mark = from_mark;
}

void MergeTreeDataPartWriterCompact::writeColumnSingleGranule(const ColumnWithTypeAndName & column, size_t from_row, size_t number_of_rows) const
{
    IDataType::SerializeBinaryBulkStatePtr state;
    IDataType::SerializeBinaryBulkSettings serialize_settings;

    serialize_settings.getter = [this](IDataType::SubstreamPath) -> WriteBuffer * { return &stream->compressed; };
    serialize_settings.position_independent_encoding = true;
    serialize_settings.low_cardinality_max_dictionary_size = 0;

    column.type->serializeBinaryBulkStatePrefix(serialize_settings, state);
    column.type->serializeBinaryBulkWithMultipleStreams(*column.column, from_row, number_of_rows, serialize_settings, state);
    column.type->serializeBinaryBulkStateSuffix(serialize_settings, state);
}

void MergeTreeDataPartWriterCompact::finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    if (columns_buffer.size() != 0)
        writeBlock(header.cloneWithColumns(columns_buffer.releaseColumns()));

    if (with_final_mark && data_written)
    {
        for (size_t i = 0; i < columns_list.size(); ++i)
        {
            writeIntBinary(stream->plain_hashing.count(), stream->marks);
            writeIntBinary(stream->compressed.offset(), stream->marks);
        }
        writeIntBinary(0ULL, stream->marks);
    }

    stream->finalize();
    if (sync)
        stream->sync();
    stream->addToChecksums(checksums);
    stream.reset();
}

void MergeTreeDataPartWriterCompact::ColumnsBuffer::add(MutableColumns && columns)
{
    if (accumulated_columns.empty())
        accumulated_columns = std::move(columns);
    else
    {
        for (size_t i = 0; i < columns.size(); ++i)
            accumulated_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
    }
}

Columns MergeTreeDataPartWriterCompact::ColumnsBuffer::releaseColumns()
{
    Columns res(std::make_move_iterator(accumulated_columns.begin()),
        std::make_move_iterator(accumulated_columns.end()));
    accumulated_columns.clear();
    return res;
}

size_t MergeTreeDataPartWriterCompact::ColumnsBuffer::size() const
{
    if (accumulated_columns.empty())
        return 0;
    return accumulated_columns.at(0)->size();
}

}
