#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Interpreters/Context.h>
#include <Compression/CompressionFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

MergeTreeDataPartWriterWide::MergeTreeDataPartWriterWide(
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
{
    const auto & columns = metadata_snapshot->getColumns();
    for (const auto & it : columns_list)
        addStreams(it.name, *it.type, columns.getCodecDescOrDefault(it.name, default_codec), settings.estimated_size);
}

void MergeTreeDataPartWriterWide::addStreams(
    const String & name,
    const IDataType & type,
    const ASTPtr & effective_codec_desc,
    size_t estimated_size)
{
    IDataType::StreamCallback callback = [&] (const IDataType::SubstreamPath & substream_path, const IDataType & substream_type)
    {
        String stream_name = IDataType::getFileNameForStream(name, substream_path);
        /// Shared offsets for Nested type.
        if (column_streams.count(stream_name))
            return;

        CompressionCodecPtr compression_codec;
        /// If we can use special codec then just get it
        if (IDataType::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, &substream_type, default_codec);
        else /// otherwise return only generic codecs and don't use info about the data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        column_streams[stream_name] = std::make_unique<Stream>(
            stream_name,
            data_part->volume->getDisk(),
            part_path + stream_name, DATA_FILE_EXTENSION,
            part_path + stream_name, marks_file_extension,
            compression_codec,
            settings.max_compress_block_size,
            estimated_size,
            settings.aio_threshold);
    };

    IDataType::SubstreamPath stream_path;
    type.enumerateStreams(callback, stream_path);
}


IDataType::OutputStreamGetter MergeTreeDataPartWriterWide::createStreamGetter(
        const String & name, WrittenOffsetColumns & offset_columns) const
{
    return [&, this] (const IDataType::SubstreamPath & substream_path) -> WriteBuffer *
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return nullptr;

        return &column_streams.at(stream_name)->compressed;
    };
}

void MergeTreeDataPartWriterWide::write(const Block & block, const IColumn::Permutation * permutation)
{
    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeIndexGranularity(block);
        fillIndexGranularity(index_granularity_for_block, block.rows());
    }

    auto granules_to_write = getGranulesToWrite(index_granularity, block.rows(), getCurrentMark(), getRowsWrittenInLastMark());

    auto offset_columns = written_offset_columns ? *written_offset_columns : WrittenOffsetColumns{};
    Block primary_key_block;
    if (settings.rewrite_primary_key)
        primary_key_block = getBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), permutation);

    Block skip_indexes_block = getBlockAndPermute(block, getSkipIndicesColumns(), permutation);

    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        const ColumnWithTypeAndName & column = block.getByName(it->name);

        if (permutation)
        {
            if (primary_key_block.has(it->name))
            {
                const auto & primary_column = *primary_key_block.getByName(it->name).column;
                writeColumn(column.name, *column.type, primary_column, offset_columns, granules_to_write);
            }
            else if (skip_indexes_block.has(it->name))
            {
                const auto & index_column = *skip_indexes_block.getByName(it->name).column;
                writeColumn(column.name, *column.type, index_column, offset_columns, granules_to_write);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                ColumnPtr permuted_column = column.column->permute(*permutation, 0);
                writeColumn(column.name, *column.type, *permuted_column, offset_columns, granules_to_write);
            }
        }
        else
        {
            writeColumn(column.name, *column.type, *column.column, offset_columns, granules_to_write);
        }
    }

    if (settings.rewrite_primary_key)
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);

    calculateAndSerializeSkipIndices(skip_indexes_block, granules_to_write);

    auto last_granule = granules_to_write.back();
    if (!last_granule.is_completed)
    {
        setCurrentMark(getCurrentMark() + granules_to_write.size() - 1);
        setRowsWrittenInLastMark(last_granule.rows_count);
    }
    else
    {
        setCurrentMark(getCurrentMark() + granules_to_write.size());
        setRowsWrittenInLastMark(0);
    }
}

void MergeTreeDataPartWriterWide::writeSingleMark(
    const String & name,
    const IDataType & type,
    WrittenOffsetColumns & offset_columns,
    size_t number_of_rows,
    DB::IDataType::SubstreamPath & path)
{
    StreamsWithMarks marks = getCurrentMarksForColumn(name, type, offset_columns, path);
    for (const auto & mark : marks)
        flushMarkToFile(mark, number_of_rows);
}

void MergeTreeDataPartWriterWide::flushMarkToFile(const StreamNameAndMark & stream_with_mark, size_t rows_in_mark)
{
    Stream & stream = *column_streams[stream_with_mark.stream_name];
    writeIntBinary(stream_with_mark.mark.offset_in_compressed_file, stream.marks);
    writeIntBinary(stream_with_mark.mark.offset_in_decompressed_block, stream.marks);
    if (settings.can_use_adaptive_granularity)
        writeIntBinary(rows_in_mark, stream.marks);
}

StreamsWithMarks MergeTreeDataPartWriterWide::getCurrentMarksForColumn(
    const String & name,
    const IDataType & type,
    WrittenOffsetColumns & offset_columns,
    DB::IDataType::SubstreamPath & path)
{
    StreamsWithMarks result;
    type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return;

        Stream & stream = *column_streams[stream_name];

        /// There could already be enough data to compress into the new block.
        if (stream.compressed.offset() >= settings.min_compress_block_size)
            stream.compressed.next();

        StreamNameAndMark stream_with_mark;
        stream_with_mark.stream_name = stream_name;
        stream_with_mark.mark.offset_in_compressed_file = stream.plain_hashing.count();
        stream_with_mark.mark.offset_in_decompressed_block = stream.compressed.offset();

        result.push_back(stream_with_mark);
    }, path);

    return result;
}

void MergeTreeDataPartWriterWide::writeSingleGranule(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    IDataType::SerializeBinaryBulkSettings & serialize_settings,
    size_t from_row,
    size_t number_of_rows,
    bool write_marks)
{
    if (write_marks)
        writeSingleMark(name, type, offset_columns, number_of_rows, serialize_settings.path);

    type.serializeBinaryBulkWithMultipleStreams(column, from_row, number_of_rows, serialize_settings, serialization_state);

    /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
    type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return;

        column_streams[stream_name]->compressed.nextIfAtEnd();
    }, serialize_settings.path);
}

/// Column must not be empty. (column.size() !== 0)
void MergeTreeDataPartWriterWide::writeColumn(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    const Granules & granules)
{
    auto [it, inserted] = serialization_states.emplace(name, nullptr);
    if (inserted)
    {
        IDataType::SerializeBinaryBulkSettings serialize_settings;
        serialize_settings.getter = createStreamGetter(name, offset_columns);
        type.serializeBinaryBulkStatePrefix(serialize_settings, it->second);
    }

    const auto & global_settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name, offset_columns);
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;

    for (const auto & granule : granules)
    {
         writeSingleGranule(
            name,
            type,
            column,
            offset_columns,
            it->second,
            serialize_settings,
            granule.start,
            granule.rows_count,
            granule.mark_on_start
        );
    }

    type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = IDataType::getFileNameForStream(name, substream_path);
            offset_columns.insert(stream_name);
        }
    }, serialize_settings.path);
}

void MergeTreeDataPartWriterWide::finishDataSerialization(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    const auto & global_settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = global_settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = global_settings.low_cardinality_use_single_dictionary_for_part != 0;
    WrittenOffsetColumns offset_columns;

    bool write_final_mark = (with_final_mark && data_written);

    {
        auto it = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++it)
        {
            if (!serialization_states.empty())
            {
                serialize_settings.getter = createStreamGetter(it->name, written_offset_columns ? *written_offset_columns : offset_columns);
                it->type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[it->name]);
            }

            if (write_final_mark)
            {
                writeFinalMark(it->name, it->type, offset_columns, serialize_settings.path);
            }
        }
    }

    for (auto & stream : column_streams)
    {
        stream.second->finalize();
        stream.second->addToChecksums(checksums);
        if (sync)
            stream.second->sync();
    }

    column_streams.clear();
    serialization_states.clear();
}

void MergeTreeDataPartWriterWide::finish(IMergeTreeDataPart::Checksums & checksums, bool sync)
{
    finishDataSerialization(checksums, sync);
    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(checksums, sync);

    finishSkipIndicesSerialization(checksums, sync);
}

void MergeTreeDataPartWriterWide::writeFinalMark(
    const std::string & column_name,
    const DataTypePtr column_type,
    WrittenOffsetColumns & offset_columns,
    DB::IDataType::SubstreamPath & path)
{
    writeSingleMark(column_name, *column_type, offset_columns, 0, path);
    /// Memoize information about offsets
    column_type->enumerateStreams([&] (const IDataType::SubstreamPath & substream_path, const IDataType & /* substream_type */)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = IDataType::getFileNameForStream(column_name, substream_path);
            offset_columns.insert(stream_name);
        }
    }, path);
}

static void fillIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity,
    size_t rows_in_last_granule,
    size_t index_granularity_for_block,
    size_t rows_in_block)
{

    size_t start = 0;
    if (rows_in_last_granule != 0)
        start = index_granularity.getLastMarkRows() - rows_in_last_granule;

    for (size_t current_row = start; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void MergeTreeDataPartWriterWide::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    fillIndexGranularityImpl(
        index_granularity,
        getRowsWrittenInLastMark(),
        index_granularity_for_block,
        rows_in_block);
}

}
