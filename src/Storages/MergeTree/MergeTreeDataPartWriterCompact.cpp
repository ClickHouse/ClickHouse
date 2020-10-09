#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>

namespace DB
{

MergeTreeDataPartWriterCompact::MergeTreeDataPartWriterCompact(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : MergeTreeDataPartWriterOnDisk(data_part_, columns_list_, metadata_snapshot_,
        indices_to_recalc_, marks_file_extension_,
        default_codec_, settings_, index_granularity_)
    , plain_file(data_part->volume->getDisk()->writeFile(
            part_path + MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION,
            settings.max_compress_block_size,
            WriteMode::Rewrite,
            settings.estimated_size,
            settings.aio_threshold))
    , plain_hashing(*plain_file)
    , marks_file(data_part->volume->getDisk()->writeFile(
        part_path + MergeTreeDataPartCompact::DATA_FILE_NAME + marks_file_extension_,
        4096,
        WriteMode::Rewrite))
    , marks(*marks_file)
{
    const auto & storage_columns = metadata_snapshot->getColumns();
    for (const auto & column : columns_list)
        addStreams(column.name, *column.type, storage_columns.getCodecDescOrDefault(column.name, default_codec));
}

void MergeTreeDataPartWriterCompact::addStreams(const String & name, const IDataType & type, const ASTPtr & effective_codec_desc)
{
    IDataType::StreamCallback callback = [&] (const IDataType::SubstreamPath & substream_path, const IDataType & substream_type)
    {
        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Shared offsets for Nested type.
        if (compressed_streams.count(stream_name))
            return;

        CompressionCodecPtr compression_codec;

        /// If we can use special codec than just get it
        if (IDataType::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, &substream_type, default_codec);
        else /// otherwise return only generic codecs and don't use info about data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        UInt64 codec_id = compression_codec->getHash();
        auto & stream = streams_by_codec[codec_id];
        if (!stream)
            stream = std::make_shared<CompressedStream>(plain_hashing, compression_codec);

        compressed_streams.emplace(stream_name, stream);
    };

    IDataType::SubstreamPath stream_path;
    type.enumerateStreams(callback, stream_path);
}

void MergeTreeDataPartWriterCompact::write(
    const Block & block, const IColumn::Permutation * permutation,
    const Block & primary_key_block, const Block & skip_indexes_block)
{
    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeIndexGranularity(block);
        fillIndexGranularity(index_granularity_for_block, block.rows());
    }

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
        /// If it's not enough rows for granule, accumulate blocks
        ///  and save how much rows we already have.
        next_index_offset = last_mark_rows - rows_in_buffer;
        return;
    }

    writeBlock(header.cloneWithColumns(columns_buffer.releaseColumns()));
}

void MergeTreeDataPartWriterCompact::writeBlock(const Block & block)
{
    size_t total_rows = block.rows();
    size_t from_mark = getCurrentMark();
    size_t current_row = 0;

    while (current_row < total_rows)
    {
        size_t rows_to_write = index_granularity.getMarkRows(from_mark);

        if (rows_to_write)
            data_written = true;

        auto name_and_type = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++name_and_type)
        {
            /// Tricky part, because we share compressed streams between different columns substreams.
            /// Compressed streams write data to the single file, but with different compression codecs.
            /// So we flush each stream (using next()) before using new one, because otherwise we will override
            /// data in result file.
            CompressedStreamPtr prev_stream;
            auto stream_getter = [&, this](const IDataType::SubstreamPath & substream_path) -> WriteBuffer *
            {
                String stream_name = IDataType::getFileNameForStream(name_and_type->name, substream_path);

                auto & result_stream = compressed_streams[stream_name];
                /// Write one compressed block per column in granule for more optimal reading.
                if (prev_stream && prev_stream != result_stream)
                {
                    /// Offset should be 0, because compressed block is written for every granule.
                    assert(result_stream->hashing_buf.offset() == 0);
                    prev_stream->hashing_buf.next();
                }

                prev_stream = result_stream;

                return &result_stream->hashing_buf;
            };


            writeIntBinary(plain_hashing.count(), marks);
            writeIntBinary(UInt64(0), marks);

            writeColumnSingleGranule(block.getByName(name_and_type->name), stream_getter, current_row, rows_to_write);

            /// Each type always have at least one substream
            prev_stream->hashing_buf.next(); //-V522
        }

        ++from_mark;
        size_t rows_written = total_rows - current_row;
        current_row += rows_to_write;

        /// Correct last mark as it should contain exact amount of rows.
        if (current_row >= total_rows && rows_written != rows_to_write)
        {
            rows_to_write = rows_written;
            index_granularity.popMark();
            index_granularity.appendMark(rows_written);
        }

        writeIntBinary(rows_to_write, marks);
    }

    next_index_offset = 0;
    next_mark = from_mark;
}

void MergeTreeDataPartWriterCompact::writeColumnSingleGranule(
    const ColumnWithTypeAndName & column,
    IDataType::OutputStreamGetter stream_getter,
    size_t from_row,
    size_t number_of_rows)
{
    IDataType::SerializeBinaryBulkStatePtr state;
    IDataType::SerializeBinaryBulkSettings serialize_settings;

    serialize_settings.getter = stream_getter;
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

#ifndef NDEBUG
    /// Offsets should be 0, because compressed block is written for every granule.
    for (const auto & [_, stream] : streams_by_codec)
        assert(stream->hashing_buf.offset() == 0);
#endif

    if (with_final_mark && data_written)
    {
        for (size_t i = 0; i < columns_list.size(); ++i)
        {
            writeIntBinary(plain_hashing.count(), marks);
            writeIntBinary(UInt64(0), marks);
        }
        writeIntBinary(UInt64(0), marks);
    }

    plain_file->next();
    marks.next();
    addToChecksums(checksums);

    plain_file->finalize();
    marks_file->finalize();
    if (sync)
    {
        plain_file->sync();
        marks_file->sync();
    }
}

static void fillIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity,
    size_t index_offset,
    size_t index_granularity_for_block,
    size_t rows_in_block)
{
    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
    {
        size_t rows_left_in_block = rows_in_block - current_row;

        /// Try to extend last granule if block is large enough
        ///  or it isn't first in granule (index_offset != 0).
        if (rows_left_in_block < index_granularity_for_block &&
            (rows_in_block >= index_granularity_for_block || index_offset != 0))
        {
            // If enough rows are left, create a new granule. Otherwise, extend previous granule.
            // So, real size of granule differs from index_granularity_for_block not more than 50%.
            if (rows_left_in_block * 2 >= index_granularity_for_block)
                index_granularity.appendMark(rows_left_in_block);
            else
                index_granularity.addRowsToLastMark(rows_left_in_block);
        }
        else
        {
            index_granularity.appendMark(index_granularity_for_block);
        }
    }
}

void MergeTreeDataPartWriterCompact::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    fillIndexGranularityImpl(
        index_granularity,
        getIndexOffset(),
        index_granularity_for_block,
        rows_in_block);
}

void MergeTreeDataPartWriterCompact::addToChecksums(MergeTreeDataPartChecksums & checksums)
{
    String data_file_name = MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
    String marks_file_name = MergeTreeDataPartCompact::DATA_FILE_NAME +  marks_file_extension;

    size_t uncompressed_size = 0;
    CityHash_v1_0_2::uint128 uncompressed_hash{0, 0};

    for (const auto & [_, stream] : streams_by_codec)
    {
        uncompressed_size += stream->hashing_buf.count();
        auto stream_hash = stream->hashing_buf.getHash();
        uncompressed_hash = CityHash_v1_0_2::CityHash128WithSeed(
            reinterpret_cast<char *>(&stream_hash), sizeof(stream_hash), uncompressed_hash);
    }

    checksums.files[data_file_name].is_compressed = true;
    checksums.files[data_file_name].uncompressed_size = uncompressed_size;
    checksums.files[data_file_name].uncompressed_hash = uncompressed_hash;
    checksums.files[data_file_name].file_size = plain_hashing.count();
    checksums.files[data_file_name].file_hash = plain_hashing.getHash();

    checksums.files[marks_file_name].file_size = marks.count();
    checksums.files[marks_file_name].file_hash = marks.getHash();
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
