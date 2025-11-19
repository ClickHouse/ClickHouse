#include <Compression/CompressionFactory.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/NullWriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeDataPartWriterCompact::MergeTreeDataPartWriterCompact(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const VirtualsDescriptionPtr & virtual_columns_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const ColumnsStatistics & stats_to_recalc,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    MergeTreeIndexGranularityPtr index_granularity_)
    : MergeTreeDataPartWriterOnDisk(
        data_part_name_, logger_name_, serializations_,
        data_part_storage_, index_granularity_info_, storage_settings_,
        columns_list_, metadata_snapshot_, virtual_columns_,
        indices_to_recalc_, stats_to_recalc, marks_file_extension_,
        default_codec_, settings_, std::move(index_granularity_))
    , plain_file(getDataPartStorage().writeFile(
            MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION,
            settings.max_compress_block_size,
            settings_.query_write_settings))
    , plain_hashing(*plain_file)
{
    marks_file = getDataPartStorage().writeFile(
            MergeTreeDataPartCompact::DATA_FILE_NAME + marks_file_extension_,
            4096,
            settings_.query_write_settings);

    marks_file_hashing = std::make_unique<HashingWriteBuffer>(*marks_file);

    if (index_granularity_info.mark_type.compressed)
    {
        marks_compressor = std::make_unique<CompressedWriteBuffer>(
            *marks_file_hashing,
             CompressionCodecFactory::instance().get(settings_.marks_compression_codec),
            settings_.marks_compress_block_size);

        marks_source_hashing = std::make_unique<HashingWriteBuffer>(*marks_compressor);
    }

    if (settings.save_marks_in_cache)
    {
        cached_marks[MergeTreeDataPartCompact::DATA_FILE_NAME] = std::make_unique<MarksInCompressedFile::PlainArray>();
    }

    for (const auto & column : columns_list)
    {
        auto compression = getCodecDescOrDefault(column.name, default_codec);
        MergeTreeDataPartWriterCompact::addStreams(column, compression);
    }
}

void MergeTreeDataPartWriterCompact::addStreams(const NameAndTypePair & name_and_type, const ASTPtr & effective_codec_desc)
{
    ISerialization::StreamCallback callback = [&](const auto & substream_path)
    {
        assert(!substream_path.empty());
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

        /// Shared offsets for Nested type.
        if (compressed_streams.contains(stream_name))
            return;

        const auto & subtype = substream_path.back().data.type;
        CompressionCodecPtr compression_codec;

        /// If we can use special codec than just get it
        if (ISerialization::isSpecialCompressionAllowed(substream_path))
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, subtype.get(), default_codec);
        else /// otherwise return only generic codecs and don't use info about data_type
            compression_codec = CompressionCodecFactory::instance().get(effective_codec_desc, nullptr, default_codec, true);

        UInt64 codec_id = compression_codec->getHash();
        auto & stream = streams_by_codec[codec_id];
        if (!stream)
            stream = std::make_shared<CompressedStream>(plain_hashing, compression_codec);

        compressed_streams.emplace(stream_name, stream);
    };

    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    enumerate_settings.object_serialization_version = settings.object_serialization_version;
    enumerate_settings.object_shared_data_serialization_version = settings.object_shared_data_serialization_version;
    enumerate_settings.object_shared_data_buckets = settings.object_shared_data_buckets;
    enumerate_settings.data_part_type = MergeTreeDataPartType::Compact;
    auto serialization = getSerialization(name_and_type.name);
    auto * sample_column = block_sample.findByName(name_and_type.name);
    auto substream_data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withColumn(sample_column ? sample_column->column : nullptr);
    getSerialization(name_and_type.name)->enumerateStreams(enumerate_settings, callback, substream_data);
}

namespace
{

/// Get granules for block using index_granularity
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, bool last_block)
{
    if (current_mark >= index_granularity.getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Request to get granules from mark {} but index granularity size is {}",
                        current_mark, index_granularity.getMarksCount());

    Granules result;
    size_t current_row = 0;
    while (current_row < block_rows)
    {
        size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
        size_t rows_left_in_block = block_rows - current_row;
        if (rows_left_in_block < expected_rows_in_mark && !last_block)
        {
            /// Invariant: we always have equal amount of rows for block in compact parts because we accumulate them in buffer.
            /// The only exclusion is the last block, when we cannot accumulate more rows.
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Required to write {} rows, but only {} rows was written for the non last granule",
                            expected_rows_in_mark, rows_left_in_block);
        }

        result.emplace_back(Granule{
            .start_row = current_row,
            .rows_to_write = std::min(rows_left_in_block, expected_rows_in_mark),
            .mark_number = current_mark,
            .mark_on_start = true,
            .is_complete = (rows_left_in_block >= expected_rows_in_mark)
        });
        current_row += result.back().rows_to_write;
        ++current_mark;
    }

    return result;
}

/// Write single granule of one column (rows between 2 marks)
void writeColumnSingleGranule(
    const ColumnWithTypeAndName & column,
    const SerializationPtr & serialization,
    ISerialization::OutputStreamGetter stream_getter,
    ISerialization::StreamMarkGetter stream_mark_getter,
    size_t from_row,
    size_t number_of_rows,
    bool is_first_granule,
    const MergeTreeWriterSettings & settings)
{
    ISerialization::SerializeBinaryBulkStatePtr state;
    ISerialization::SerializeBinaryBulkSettings serialize_settings;

    serialize_settings.getter = stream_getter;
    serialize_settings.stream_mark_getter = stream_mark_getter;
    serialize_settings.position_independent_encoding = true;
    serialize_settings.low_cardinality_max_dictionary_size = 0;
    serialize_settings.use_compact_variant_discriminators_serialization = settings.use_compact_variant_discriminators_serialization;
    serialize_settings.dynamic_serialization_version = settings.dynamic_serialization_version;
    serialize_settings.object_serialization_version = settings.object_serialization_version;
    serialize_settings.object_shared_data_serialization_version = settings.object_shared_data_serialization_version;
    serialize_settings.object_shared_data_buckets = settings.object_shared_data_buckets;
    /// Write object and dynamic statistics only in first granule, it is used
    /// only during merges and we always get it from the first granule.
    if (is_first_granule)
        serialize_settings.object_and_dynamic_write_statistics = ISerialization::SerializeBinaryBulkSettings::ObjectAndDynamicStatisticsMode::PREFIX;
    else
        serialize_settings.object_and_dynamic_write_statistics = ISerialization::SerializeBinaryBulkSettings::ObjectAndDynamicStatisticsMode::PREFIX_EMPTY;
    serialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    serialize_settings.data_part_type = MergeTreeDataPartType::Compact;

    serialization->serializeBinaryBulkStatePrefix(*column.column, serialize_settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column.column, from_row, number_of_rows, serialize_settings, state);
    serialization->serializeBinaryBulkStateSuffix(serialize_settings, state);
}

}

void MergeTreeDataPartWriterCompact::write(const Block & block, const IColumnPermutation * permutation)
{
    Block result_block = block;

    /// During serialization columns with dynamic subcolumns (like JSON/Dynamic) must have the same dynamic structure.
    /// But it may happen that they don't (for example during ALTER MODIFY COLUMN from some type to JSON/Dynamic).
    /// In this case we use dynamic structure of the column from the first written block and adjust columns from
    /// the next blocks so they match this dynamic structure.
    initOrAdjustDynamicStructureIfNeeded(result_block);

    initColumnsSubstreamsIfNeeded(result_block);

    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeIndexGranularity(result_block);
        assert(index_granularity_for_block >= 1);
        fillIndexGranularity(index_granularity_for_block, result_block.rows());
    }

    result_block = permuteBlockIfNeeded(result_block, permutation);
    calculateAndSerializeStatistics(result_block);

    if (header.empty())
        header = result_block.cloneEmpty();

    size_t current_mark_rows = index_granularity->getMarkRows(getCurrentMark());
    Block flushed_block;
    if (columns_buffer.size() == 0 && result_block.rows() >= current_mark_rows)
    {
        flushed_block = std::move(result_block);
    }
    else
    {
        columns_buffer.add(result_block.mutateColumns());
        size_t rows_in_buffer = columns_buffer.size();
        if (rows_in_buffer >= current_mark_rows)
            flushed_block = header.cloneWithColumns(columns_buffer.releaseColumns());
    }

    if (!flushed_block.empty())
    {
        auto granules_to_write = getGranulesToWrite(*index_granularity, flushed_block.rows(), getCurrentMark(), /* last_block = */ false);
        writeDataBlockPrimaryIndexAndSkipIndices(flushed_block, granules_to_write);
        setCurrentMark(getCurrentMark() + granules_to_write.size());
    }
}

void MergeTreeDataPartWriterCompact::writeDataBlockPrimaryIndexAndSkipIndices(const Block & block, const Granules & granules_to_write)
{
    writeDataBlock(block, granules_to_write);

    if (settings.rewrite_primary_key)
    {
        Block primary_key_block = getIndexBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), nullptr);
        calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);
    }

    Block skip_indices_block = getIndexBlockAndPermute(block, getSkipIndicesColumns(), nullptr);
    calculateAndSerializeSkipIndices(skip_indices_block, granules_to_write);
}

void MergeTreeDataPartWriterCompact::writeDataBlock(const Block & block, const Granules & granules)
{
    WriteBuffer & marks_out = marks_source_hashing ? *marks_source_hashing : *marks_file_hashing;

    for (const auto & granule : granules)
    {
        auto name_and_type = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++name_and_type)
        {
            /// Tricky part, because we share compressed streams between different columns substreams.
            /// Compressed streams write data to the single file, but with different compression codecs.
            /// So we flush each stream (using next()) before using new one, because otherwise we will override
            /// data in result file.
            CompressedStreamPtr prev_stream;
            auto stream_getter = [&, this](const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
            {
                String stream_name = ISerialization::getFileNameForStream(*name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

                auto & result_stream = compressed_streams[stream_name];
                /// Write one compressed block per column in granule for more optimal reading.
                if (prev_stream && prev_stream != result_stream)
                {
                    /// Offset should be 0, because compressed block is written for every granule.
                    assert(result_stream->hashing_buf.offset() == 0);
                    prev_stream->hashing_buf.next();
                }

                /// We have 2 types of marks in Compact part. With or without substreams.
                /// In format without substreams we write single mark per column (here once on the first requested substream).
                /// In format with substreams we write a mark for each column substream.
                if (!prev_stream || index_granularity_info.mark_type.with_substreams)
                {
                    MarkInCompressedFile mark{plain_hashing.count(), result_stream->hashing_buf.offset()};
                    writeBinaryLittleEndian(mark.offset_in_compressed_file, marks_out);
                    writeBinaryLittleEndian(mark.offset_in_decompressed_block, marks_out);

                    if (!cached_marks.empty())
                        cached_marks.begin()->second->push_back(mark);
                }

                prev_stream = result_stream;

                return &result_stream->hashing_buf;
            };

            auto stream_mark_getter = [&](const ISerialization::SubstreamPath & substream_path) -> MarkInCompressedFile
            {
                String stream_name = ISerialization::getFileNameForStream(*name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
                return {plain_hashing.count(), compressed_streams[stream_name]->hashing_buf.offset()};
            };

            writeColumnSingleGranule(
                block.getByName(name_and_type->name), getSerialization(name_and_type->name),
                stream_getter, stream_mark_getter, granule.start_row, granule.rows_to_write, !data_written, settings);

            /// Each type always have at least one substream
            prev_stream->hashing_buf.next();
        }

        writeBinaryLittleEndian(granule.rows_to_write, marks_out);
        data_written = true;
    }
}

void MergeTreeDataPartWriterCompact::fillDataChecksums(MergeTreeDataPartChecksums & checksums)
{
    if (columns_buffer.size() != 0)
    {
        auto block = header.cloneWithColumns(columns_buffer.releaseColumns());
        auto granules_to_write = getGranulesToWrite(*index_granularity, block.rows(), getCurrentMark(), /*last_block=*/ true);
        if (!granules_to_write.back().is_complete)
        {
            /// Correct last mark as it should contain exact amount of rows.
            index_granularity->adjustLastMark(granules_to_write.back().rows_to_write);
        }
        writeDataBlockPrimaryIndexAndSkipIndices(block, granules_to_write);
    }

    /// If there was no data written, we still need to initialize columns substreams for marks with substreams.
    if (index_granularity_info.mark_type.with_substreams && !data_written)
    {
        Block sample;
        for (const auto & [name, type] : columns_list)
            sample.insert(ColumnWithTypeAndName(type->createColumn(), type, name));
        initColumnsSubstreamsIfNeeded(sample);
    }

#ifndef NDEBUG
    /// Offsets should be 0, because compressed block is written for every granule.
    for (const auto & [_, stream] : streams_by_codec)
        assert(stream->hashing_buf.offset() == 0);
#endif

    WriteBuffer & marks_out = marks_source_hashing ? *marks_source_hashing : *marks_file_hashing;

    if (with_final_mark && data_written)
    {
        MarkInCompressedFile mark{plain_hashing.count(), 0};
        size_t num_marks = index_granularity_info.mark_type.with_substreams ? columns_substreams.getTotalSubstreams() : columns_list.size();
        for (size_t i = 0; i < num_marks; ++i)
        {
            writeBinaryLittleEndian(mark.offset_in_compressed_file, marks_out);
            writeBinaryLittleEndian(mark.offset_in_decompressed_block, marks_out);

            if (!cached_marks.empty())
                cached_marks.begin()->second->push_back(mark);
        }

        writeBinaryLittleEndian(static_cast<UInt64>(0), marks_out);
    }

    for (const auto & [_, stream] : streams_by_codec)
    {
        stream->hashing_buf.finalize();
        stream->compressed_buf.finalize();
    }

    plain_hashing.finalize();

    plain_file->next();

    if (marks_source_hashing)
        marks_source_hashing->finalize();
    if (marks_compressor)
        marks_compressor->finalize();

    marks_file_hashing->finalize();

    addToChecksums(checksums);

    plain_file->preFinalize();
    marks_file->preFinalize();
}

void MergeTreeDataPartWriterCompact::initColumnsSubstreamsIfNeeded(const Block & sample)
{
    if (!index_granularity_info.mark_type.with_substreams || columns_substreams.getTotalSubstreams())
        return;

    NullWriteBuffer buf;
    for (const auto & name_and_type : columns_list)
    {
        columns_substreams.addColumn(name_and_type.name);
        auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            columns_substreams.addSubstreamToLastColumn(ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings)));
            return &buf;
        };

        auto mark_getter = [](const ISerialization::SubstreamPath &) { return MarkInCompressedFile(); };
        const auto & column = sample.getByName(name_and_type.name);
        writeColumnSingleGranule(column, getSerialization(name_and_type.name), buffer_getter, mark_getter, column.column->size(), 0, false, settings);
    }
}

void MergeTreeDataPartWriterCompact::finishDataSerialization(bool sync)
{
    if (sync)
    {
        plain_file->sync();
        marks_file->sync();
    }

    plain_file->finalize();
    marks_file->finalize();
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
    size_t index_offset = 0;
    if (index_granularity->getMarksCount() > getCurrentMark())
        index_offset = index_granularity->getMarkRows(getCurrentMark()) - columns_buffer.size();

    fillIndexGranularityImpl(
        *index_granularity,
        index_offset,
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
        transformEndianness<std::endian::little>(stream_hash);
        uncompressed_hash = CityHash_v1_0_2::CityHash128WithSeed(
            reinterpret_cast<const char *>(&stream_hash), sizeof(stream_hash), uncompressed_hash);
    }

    checksums.files[data_file_name].is_compressed = true;
    checksums.files[data_file_name].uncompressed_size = uncompressed_size;
    checksums.files[data_file_name].uncompressed_hash = uncompressed_hash;
    checksums.files[data_file_name].file_size = plain_hashing.count();
    checksums.files[data_file_name].file_hash = plain_hashing.getHash();

    if (marks_compressor)
    {
        checksums.files[marks_file_name].is_compressed = true;
        checksums.files[marks_file_name].uncompressed_size = marks_source_hashing->count();
        checksums.files[marks_file_name].uncompressed_hash = marks_source_hashing->getHash();
    }

    checksums.files[marks_file_name].file_size = marks_file_hashing->count();
    checksums.files[marks_file_name].file_hash = marks_file_hashing->getHash();
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

void MergeTreeDataPartWriterCompact::fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & /*checksums_to_remove*/)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        fillDataChecksums(checksums);

    if (settings.rewrite_primary_key)
        fillPrimaryIndexChecksums(checksums);

    fillSkipIndicesChecksums(checksums);
    fillStatisticsChecksums(checksums);
}

void MergeTreeDataPartWriterCompact::finish(bool sync)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        finishDataSerialization(sync);

    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(sync);

    finishSkipIndicesSerialization(sync);
    finishStatisticsSerialization(sync);
}

void MergeTreeDataPartWriterCompact::cancel() noexcept
{
    for (const auto & [_, stream] : streams_by_codec)
    {
        stream->hashing_buf.cancel();
        stream->compressed_buf.cancel();
    }

    plain_hashing.cancel();

    plain_file->cancel();

    if (marks_source_hashing)
        marks_source_hashing->cancel();

    if (marks_compressor)
        marks_compressor->cancel();

    marks_file_hashing->cancel();

    marks_file->cancel();

    Base::cancel();
}


}
