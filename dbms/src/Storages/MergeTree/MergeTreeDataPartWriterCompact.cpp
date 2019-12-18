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
    , squashing(storage.getSettings()->index_granularity, storage.getSettings()->index_granularity_bytes) /// FIXME
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
    std::cerr << "(MergeTreeDataPartWriterCompact::write) block111: " << block.dumpStructure() << "\n";

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

    auto result = squashing.add(result_block.mutateColumns());
    if (!result.ready)
        return;

    result_block = header.cloneWithColumns(std::move(result.columns));

    writeBlock(result_block);
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

        /// There could already be enough data to compress into the new block.
        if (stream->compressed.offset() >= settings.min_compress_block_size)
             stream->compressed.next();

        size_t next_row = 0;
        writeIntBinary(rows_to_write, stream->marks);
        for (const auto & it : columns_list)
            next_row = writeColumnSingleGranule(block.getByName(it.name), current_row, rows_to_write);

        ++from_mark;
        current_row = next_row;
    }

    next_mark = from_mark;
    next_index_offset = total_rows - current_row;
}


size_t MergeTreeDataPartWriterCompact::writeColumnSingleGranule(const ColumnWithTypeAndName & column, size_t from_row, size_t number_of_rows)
{
    std::cerr << "(writeColumnSingleGranule) writing column: " << column.name << "\n";
    std::cerr << "(writeColumnSingleGranule) from_row: " << from_row << "\n";
    std::cerr << "(writeColumnSingleGranule) number_of_rows: " << number_of_rows << "\n";

    size_t old_uncompressed_size = stream->compressed.count();

    writeIntBinary(stream->plain_hashing.count(), stream->marks);
    writeIntBinary(stream->compressed.offset(), stream->marks);

    IDataType::SerializeBinaryBulkStatePtr state;
    IDataType::SerializeBinaryBulkSettings serialize_settings;

    serialize_settings.getter = [this](IDataType::SubstreamPath) -> WriteBuffer * { return &stream->compressed; };
    serialize_settings.position_independent_encoding = true;
    serialize_settings.low_cardinality_max_dictionary_size = 0;

    column.type->serializeBinaryBulkStatePrefix(serialize_settings, state);
    column.type->serializeBinaryBulkWithMultipleStreams(*column.column, from_row, number_of_rows, serialize_settings, state);
    column.type->serializeBinaryBulkStateSuffix(serialize_settings, state);

    /// We can't calculate compressed size by single column in compact format.
    size_t uncompressed_size = stream->compressed.count();
    columns_sizes[column.name].add(ColumnSize{0, 0, uncompressed_size - old_uncompressed_size});

    return from_row + number_of_rows;
}

void MergeTreeDataPartWriterCompact::finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    auto result = squashing.add({});
    if (result.ready && !result.columns.empty())
        writeBlock(header.cloneWithColumns(std::move(result.columns)));

    if (with_final_mark && data_written)
    {
        writeIntBinary(0ULL, stream->marks);
        for (size_t i = 0; i < columns_list.size(); ++i)
        {
            writeIntBinary(stream->plain_hashing.count(), stream->marks);
            writeIntBinary(stream->compressed.offset(), stream->marks);
        }
    }

    size_t marks_size = stream->marks.count();
    for (auto it = columns_sizes.begin(); it != columns_sizes.end(); ++it)
        it->second.marks = marks_size;

    stream->finalize();
    if (sync)
        stream->sync();
    stream->addToChecksums(checksums);
    stream.reset();
}

}
