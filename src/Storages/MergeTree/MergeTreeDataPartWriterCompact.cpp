#include <Compression/CompressionFactory.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ParallelSyncFiles.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/NullWriteBuffer.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/SipHash.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool compress_per_column_in_compact_parts;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char compact_part_writer_fail_in_add_streams[];
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
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    MergeTreeIndexGranularityPtr index_granularity_)
    : MergeTreeDataPartWriterOnDisk(
        data_part_name_, logger_name_, serializations_,
        data_part_storage_, index_granularity_info_, storage_settings_,
        columns_list_, metadata_snapshot_,
        indices_to_recalc_, marks_file_extension_,
        default_codec_, settings_, std::move(index_granularity_),
        static_cast<WrittenOffsetSubstreams *>(nullptr))
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
        cached_marks[MergeTreeDataPartCompact::DATA_FILE_NAME] = std::make_unique<MarksInCompressedFile::PlainArray>();
}

void MergeTreeDataPartWriterCompact::addStreams(const NameAndTypePair & name_and_type)
{
    ISerialization::StreamCallback callback = [&](const auto & substream_path)
    {
        chassert(!substream_path.empty());
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

        /// Shared offsets for Nested type.
        if (compressed_streams.contains(stream_name))
            return;

        auto compression_codec = getSubstreamCodec(name_and_type, substream_path);

        UInt64 codec_id = compression_codec->getHash();
        /// Codecs that need the vector dimension upfront (e.g. SZ3) keep per-stream state in the codec
        /// object, so they must not be shared between streams. Make the key unique per stream so that
        /// every such stream gets its own codec instance, while still being tracked for finalize/cancel.
        if (compression_codec->needsVectorDimensionUpfront())
        {
            SipHash codec_hash;
            codec_hash.update(codec_id);
            codec_hash.update(stream_name.data(), stream_name.size());
            codec_id = codec_hash.get64();
        }
        /// Exception safety: if `make_shared` throws, the map is not modified, avoiding null entries in `cancel`.
        auto it = streams_by_codec.find(codec_id);
        if (it == streams_by_codec.end())
        {
            fiu_do_on(FailPoints::compact_part_writer_fail_in_add_streams,
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected failure in Compact part writer addStreams");
            });
            it = streams_by_codec.emplace(codec_id, std::make_shared<CompressedStream>(plain_hashing, compression_codec)).first;
        }

        compressed_streams.emplace(stream_name, it->second);
    };

    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    enumerate_settings.object_serialization_version = settings.object_serialization_version;
    enumerate_settings.object_shared_data_serialization_version = settings.object_shared_data_serialization_version;
    enumerate_settings.object_shared_data_buckets = settings.object_shared_data_buckets;
    enumerate_settings.object_shared_data_target_chunk_rows = settings.object_shared_data_target_chunk_rows;
    enumerate_settings.max_buckets_in_map = settings.max_buckets_in_map;
    enumerate_settings.map_buckets_strategy = settings.map_buckets_strategy;
    enumerate_settings.map_buckets_coefficient = settings.map_buckets_coefficient;
    enumerate_settings.map_buckets_min_avg_size = settings.map_buckets_min_avg_size;
    enumerate_settings.data_part_type = MergeTreeDataPartType::Compact;
    auto serialization = getSerialization(name_and_type.name);
    auto substream_data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withColumn(block_sample.getByName(name_and_type.name).column);
    serialization->enumerateStreams(enumerate_settings, callback, substream_data);
}

namespace
{

/// Get granules for block using index_granularity. The last granule may be incomplete.
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark)
{
    size_t marks_count = index_granularity.getMarksCount();
    if (current_mark >= marks_count)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Request to get granules from mark {} but index granularity size is {}",
                        current_mark, marks_count);

    Granules result;
    size_t current_row = 0;
    while (current_row < block_rows && current_mark < marks_count)
    {
        size_t expected_rows_in_mark = index_granularity.getMarkRows(current_mark);
        size_t rows_left_in_block = block_rows - current_row;

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
    const ColumnWithTypeAndName & sample_column,
    const SerializationPtr & serialization,
    ISerialization::OutputStreamGetter stream_getter,
    ISerialization::StreamMarkGetter stream_mark_getter,
    size_t from_row,
    size_t number_of_rows,
    bool is_first_granule,
    ISerialization::SerializeBinaryBulkSettings && serialize_settings)
{
    ISerialization::SerializeBinaryBulkStatePtr state;

    serialize_settings.getter = stream_getter;
    serialize_settings.stream_mark_getter = stream_mark_getter;
    /// Write object and dynamic statistics only in first granule, it is used
    /// only during merges and we always get it from the first granule.
    if (!is_first_granule)
        serialize_settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::PREFIX_EMPTY;
    serialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    serialize_settings.data_part_type = MergeTreeDataPartType::Compact;

    /// Use the sample column (from block_sample) for the state prefix because
    /// serializeBinaryBulkStatePrefix only reads column structure and statistics
    /// (not actual row data) to determine things like the number of Map buckets.
    /// block_sample always has statistics consistent with what was used in
    /// enumerateStreams (via addStreams), so using it here guarantees that the
    /// bucket count written to the prefix matches the streams that were created.
    serialization->serializeBinaryBulkStatePrefix(*sample_column.column, serialize_settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column.column, from_row, number_of_rows, serialize_settings, state);
    serialization->serializeBinaryBulkStateSuffix(serialize_settings, state);
}

MutableColumns mutateColumns(Columns && columns)
{
    MutableColumns mutable_columns(columns.size());
    for (size_t i = 0; i != columns.size(); ++i)
        mutable_columns[i] = IColumn::mutate(std::move(columns[i]));
    return mutable_columns;
}

}

ISerialization::SerializeBinaryBulkSettings MergeTreeDataPartWriterCompact::getSerializationSettings() const
{
    ISerialization::SerializeBinaryBulkSettings serialize_settings;

    serialize_settings.position_independent_encoding = true;
    serialize_settings.low_cardinality_max_dictionary_size = 0;
    serialize_settings.use_compact_variant_discriminators_serialization = settings.use_compact_variant_discriminators_serialization;
    serialize_settings.dynamic_serialization_version = settings.dynamic_serialization_version;
    serialize_settings.object_serialization_version = settings.object_serialization_version;
    serialize_settings.object_shared_data_serialization_version = settings.object_shared_data_serialization_version;
    serialize_settings.object_shared_data_buckets = settings.object_shared_data_buckets;
    serialize_settings.object_shared_data_target_chunk_rows = settings.object_shared_data_target_chunk_rows;
    serialize_settings.object_typed_shared_data_serialization_version = settings.object_typed_shared_data_serialization_version;
    serialize_settings.max_buckets_in_map = settings.max_buckets_in_map;
    serialize_settings.map_buckets_strategy = settings.map_buckets_strategy;
    serialize_settings.map_buckets_coefficient = settings.map_buckets_coefficient;
    serialize_settings.map_buckets_min_avg_size = settings.map_buckets_min_avg_size;
    serialize_settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::PREFIX;
    serialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    serialize_settings.data_part_type = MergeTreeDataPartType::Compact;
    /// One complete granule per call.
    serialize_settings.granule_is_complete = true;

    return serialize_settings;
}

void MergeTreeDataPartWriterCompact::write(const Block & block, const IColumnPermutation * permutation, Block * /*permuted_columns_cache*/)
{
    /// The permuted columns cache is intentionally ignored in the Compact writer:
    /// `permuteBlockIfNeeded` below permutes the whole block once, and the subsequent
    /// `getIndexBlockAndPermute` calls in `writeDataBlockPrimaryIndexAndSkipIndices`
    /// pass `permutation = nullptr` (they only re-pick columns by name from the
    /// already-permuted block). So the cache would only ever be written to, never
    /// read from — pure overhead.
    Block result_block = block;

    /// For some columns the set of streams may depend on the actual column data.
    /// For example: dynamic structure and statistics for JSON, Dynamic and Map (with adaptive number of buckets).
    /// We must ensure that all blocks will be written in the same set of streams, so we have to make some
    /// preparations to achieve it.
    prepareBlockForWriting(result_block);

    initStreamsIfNeeded();
    initColumnsSubstreamsIfNeeded();

    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
    {
        size_t index_granularity_for_block = computeIndexGranularity(result_block);
        chassert(index_granularity_for_block >= 1);
        fillIndexGranularity(index_granularity_for_block, result_block.rows());
    }

    result_block = permuteBlockIfNeeded(result_block, permutation, nullptr);

    if (header.empty())
        header = result_block.cloneEmpty();

    size_t num_rows_in_current_block = result_block.rows() + columns_buffer.getNumRowsInLastIncompleteGranule();
    auto granules_to_write = getGranulesToWrite(*index_granularity, num_rows_in_current_block, getCurrentMark());

    /// Only complete granules are written. An incomplete last granule stays in the buffer
    /// until the next block completes it or the part is finalized.
    if (!granules_to_write.empty() && !granules_to_write.back().is_complete)
        granules_to_write.pop_back();

    setCurrentMark(getCurrentMark() + granules_to_write.size());

    columns_buffer.add(result_block.detachColumns(), std::move(granules_to_write));

    size_t num_granules_to_flush = 0;
    size_t num_granules_in_buffer = columns_buffer.getNumCompleteGranules();

    if (num_granules_in_buffer >= settings.compact_parts_max_granules_to_buffer)
        num_granules_to_flush = (num_granules_in_buffer / settings.compact_parts_max_granules_to_buffer) * settings.compact_parts_max_granules_to_buffer;

    if (!num_granules_to_flush && settings.compact_parts_max_bytes_to_buffer)
    {
        size_t num_bytes_in_buffer = columns_buffer.getNumBytes();
        if (num_bytes_in_buffer > settings.compact_parts_max_bytes_to_buffer)
            num_granules_to_flush = num_granules_in_buffer;
    }

    if (num_granules_to_flush)
        writeDataBlockPrimaryIndexAndSkipIndices(makeBlocksAndGranules(columns_buffer.flush(num_granules_to_flush)));
}

MergeTreeDataPartWriterCompact::BlocksAndGranules
MergeTreeDataPartWriterCompact::makeBlocksAndGranules(ColumnsBuffer::ColumnsAndGranules && columns_and_granules) const
{
    BlocksAndGranules blocks_and_granules;
    blocks_and_granules.reserve(columns_and_granules.size());
    for (auto & [columns, granules] : columns_and_granules)
        blocks_and_granules.emplace_back(header.cloneWithColumns(columns), std::move(granules));
    return blocks_and_granules;
}

void MergeTreeDataPartWriterCompact::writeDataBlockPrimaryIndexAndSkipIndices(const BlocksAndGranules & blocks_and_granules)
{
    writeDataBlock(blocks_and_granules);

    /// The blocks here are already fully permuted by `permuteBlockIfNeeded` in `write`,
    /// so we pass `permutation = nullptr` and no cache — only Wide writer benefits
    /// from the permuted columns cache (see comment in `MergeTreeDataPartWriterCompact::write`).
    if (settings.rewrite_primary_key)
    {
        for (const auto & [block, granules_to_write] : blocks_and_granules)
        {
            Block primary_key_block = getIndexBlockAndPermute(block, metadata_snapshot->getPrimaryKeyColumns(), nullptr);
            calculateAndSerializePrimaryIndex(primary_key_block, granules_to_write);
        }
    }

    for (const auto & [block, granules_to_write] : blocks_and_granules)
    {
        Block skip_indices_block = getIndexBlockAndPermute(block, getSkipIndicesColumns(), nullptr);
        calculateAndSerializeSkipIndices(skip_indices_block, granules_to_write);
    }
}

void MergeTreeDataPartWriterCompact::writeDataBlock(const BlocksAndGranules & blocks_and_granules)
{
    Granules granules;
    std::vector<size_t> block_indexes;
    for (size_t i = 0; i != blocks_and_granules.size(); ++i)
    {
        for (const auto & granule : blocks_and_granules[i].second)
        {
            granules.push_back(granule);
            block_indexes.push_back(i);
        }
    }

    /// The marks file is granule-major while the data of a stripe is column-major, so the marks
    /// of the granules are collected while writing the stripes and are written after them.
    size_t num_marks_per_granule = index_granularity_info.mark_type.with_substreams ? columns_substreams.getTotalSubstreams() : columns_list.size();
    std::vector<std::vector<MarkInCompressedFile>> marks_to_write(granules.size(), std::vector<MarkInCompressedFile>(num_marks_per_granule));

    for (size_t stripe_begin = 0; stripe_begin < granules.size(); stripe_begin += settings.compact_parts_max_granules_to_buffer)
    {
        size_t stripe_end = std::min(granules.size(), stripe_begin + settings.compact_parts_max_granules_to_buffer);

        /// Tricky part, because we share compressed streams between different columns substreams.
        /// Compressed streams write data to the single file, but with different compression codecs.
        /// So we flush each stream (using next()) before using new one, because otherwise we will override
        /// data in result file.
        CompressedStreamPtr prev_stream;
        auto name_and_type = columns_list.begin();
        for (size_t column_idx = 0; column_idx < columns_list.size(); ++column_idx, ++name_and_type)
        {
            const UInt64 min_compress_block_size = getEffectiveMinCompressBlockSize(*name_and_type);
            for (size_t granule_idx = stripe_begin; granule_idx < stripe_end; ++granule_idx)
            {
                CurrentThread::checkIfNotCancelled();

                bool is_first_substream = true;
                auto stream_getter = [&, this](const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
                {
                    String stream_name = ISerialization::getFileNameForStream(*name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

                    auto stream_it = compressed_streams.find(stream_name);
                    if (stream_it == compressed_streams.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stream {} for column {} not found", stream_name, name_and_type->name);

                    auto & result_stream = stream_it->second;

                    /// Codecs that need the vector dimension upfront (e.g. SZ3) take it from the whole column once per flush,
                    /// before the first granule of the column is compressed: `setAndCheckVectorDimension` accumulates it
                    /// monotonically, and rescanning the column on every granule would cost O(rows * granules).
                    if (granule_idx == 0)
                    {
                        auto compression_codec = result_stream->compressed_buf.getCodec();
                        for (const auto & [block, _] : blocks_and_granules)
                            setVectorDimensionsIfNeeded(compression_codec, block.getColumnOrSubcolumnByName(name_and_type->name).column.get());
                    }

                    if (prev_stream && prev_stream != result_stream)
                    {
                        /// Every stream but the current one is flushed, so its offset should be 0.
                        chassert(result_stream->hashing_buf.offset() == 0);
                        prev_stream->hashing_buf.next();
                    }
                    else if (prev_stream && settings.compress_per_substream_in_compact_parts && index_granularity_info.mark_type.with_substreams
                        && settings.compress_per_column_in_compact_parts
                        && prev_stream->hashing_buf.offset() >= min_compress_block_size)
                    {
                        /// Cut a new block at a substream boundary only once it's worth closing: a subcolumn read
                        /// then doesn't decompress the column's other substreams, while small substreams keep
                        /// sharing a block.
                        prev_stream->hashing_buf.next();
                    }

                    /// We have 2 types of marks in Compact part. With or without substreams.
                    /// In format without substreams we write single mark per column (here once on the first requested substream).
                    /// In format with substreams we write a mark for each column substream.
                    if (is_first_substream || index_granularity_info.mark_type.with_substreams)
                    {
                        size_t mark_position = index_granularity_info.mark_type.with_substreams ? columns_substreams.getSubstreamPosition(column_idx, stream_name) : column_idx;
                        marks_to_write[granule_idx][mark_position] = MarkInCompressedFile{plain_hashing.count(), result_stream->hashing_buf.offset()};
                        is_first_substream = false;
                    }

                    prev_stream = result_stream;

                    return &result_stream->hashing_buf;
                };

                auto stream_mark_getter = [&](const ISerialization::SubstreamPath & substream_path) -> MarkInCompressedFile
                {
                    String stream_name = ISerialization::getFileNameForStream(*name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
                    return {plain_hashing.count(), compressed_streams[stream_name]->hashing_buf.offset()};
                };

                const auto & block = blocks_and_granules[block_indexes[granule_idx]].first;
                const auto & granule = granules[granule_idx];

                auto serialize_settings = getSerializationSettings();
                serialize_settings.min_compress_block_size = min_compress_block_size;
                writeColumnSingleGranule(
                    block.getByName(name_and_type->name), block_sample.getByName(name_and_type->name),
                    getSerialization(name_and_type->name),
                    stream_getter, stream_mark_getter, granule.start_row, granule.rows_to_write,
                    /*is_first_granule=*/ !data_written && granule_idx == 0, std::move(serialize_settings));

                /// Write one compressed block per column in granule for more optimal reading.
                /// Every type has at least one substream, so the stream of the column is set here.
                if (settings.compress_per_column_in_compact_parts)
                {
                    prev_stream->hashing_buf.next();
                    prev_stream = nullptr;
                }
            }
        }

        /// Otherwise the columns of a stripe share compressed blocks, which are cut only at the end of the stripe.
        if (prev_stream)
            prev_stream->hashing_buf.next();
    }

    WriteBuffer & marks_out = marks_source_hashing ? *marks_source_hashing : *marks_file_hashing;

    for (size_t granule_idx = 0; granule_idx < granules.size(); ++granule_idx)
    {
        for (const auto & mark : marks_to_write[granule_idx])
        {
            writeBinaryLittleEndian(mark.offset_in_compressed_file, marks_out);
            writeBinaryLittleEndian(mark.offset_in_decompressed_block, marks_out);

            if (!cached_marks.empty())
                cached_marks.begin()->second->push_back(mark);
        }

        writeBinaryLittleEndian(granules[granule_idx].rows_to_write, marks_out);
    }

    data_written = true;
}

void MergeTreeDataPartWriterCompact::finalizeIndexGranularity()
{
    /// If no data was written, streams and columns substreams will be uninitialized, but we need them.
    initStreamsIfNeeded();
    initColumnsSubstreamsIfNeeded();

    if (!columns_buffer.empty())
    {
        auto blocks_and_granules_to_flush = makeBlocksAndGranules(columns_buffer.flushAll());
        const auto & last_granule = blocks_and_granules_to_flush.back().second.back();
        if (!last_granule.is_complete)
        {
            /// Correct last mark as it should contain exact amount of rows.
            index_granularity->adjustLastMark(last_granule.rows_to_write);
        }

        writeDataBlockPrimaryIndexAndSkipIndices(blocks_and_granules_to_flush);
    }

#ifndef NDEBUG
    /// Offsets should be 0, because compressed blocks are cut at the end of every stripe.
    for (const auto & [_, stream] : streams_by_codec)
        chassert(stream->hashing_buf.offset() == 0);
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
}

void MergeTreeDataPartWriterCompact::fillDataChecksums(MergeTreeDataPartChecksums & checksums)
{
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

void MergeTreeDataPartWriterCompact::finishDataSerialization(bool sync)
{
    if (sync)
        parallelSyncFiles({plain_file.get(), marks_file.get()});

    plain_file->finalize();
    marks_file->finalize();

    /// Release the data (`data.bin`) and marks (`data.cmrk*`) file descriptors now that everything
    /// has been flushed and synced. Otherwise the writer keeps these handles open until it is
    /// destroyed, which happens only after the part's temporary directory has been renamed to its
    /// final name. Renaming a directory that still has open file descriptors inside fails on
    /// filesystems backed by Windows (WSL, CIFS/SMB, Docker Desktop bind mounts).
    /// See https://github.com/ClickHouse/ClickHouse/issues/56288.
    ///
    /// Only plain_file and marks_file own the file descriptors. The wrapper buffers
    /// (plain_hashing, streams_by_codec, marks_*_hashing, marks_compressor) were already finalized in
    /// fillDataChecksums, and a finalized WriteBuffer never touches its underlying buffer on
    /// destruction, so releasing the file streams here is safe even though the wrappers outlive them.
    plain_file = nullptr;
    marks_file = nullptr;
}

void MergeTreeDataPartWriterCompact::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    size_t index_offset = 0;
    if (index_granularity->getMarksCount() > getCurrentMark())
        index_offset = index_granularity->getMarkRows(getCurrentMark()) - columns_buffer.getNumRowsInLastIncompleteGranule();

    fillIndexGranularityForCompactPart(*index_granularity, index_offset, index_granularity_for_block, rows_in_block);
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

void MergeTreeDataPartWriterCompact::ColumnsBuffer::add(Columns && columns, Granules && granules)
{
    size_t num_rows_in_new_complete_granules = 0;
    for (const auto & granule : granules)
        num_rows_in_new_complete_granules += granule.rows_to_write;

    size_t num_rows_in_old_incomplete_granule = getNumRowsInLastIncompleteGranule();
    size_t num_rows_in_new_incomplete_granule = columns.at(0)->size() + num_rows_in_old_incomplete_granule - num_rows_in_new_complete_granules;

    if (num_rows_in_new_complete_granules == 0)
    {
        if (num_rows_in_old_incomplete_granule == 0)
        {
            incomplete_granule_columns = mutateColumns(std::move(columns));
        }
        else
        {
            for (size_t i = 0; i != columns.size(); ++i)
            {
                auto column = std::move(columns[i]);
                /// Fix dynamic structure so it won't changed after insertion of new rows.
                incomplete_granule_columns[i]->fixDynamicStructure();
                incomplete_granule_columns[i]->insertRangeFrom(*column, 0, column->size());
            }
        }

        return;
    }

    MutableColumns new_incomplete_granule_columns;
    if (num_rows_in_new_incomplete_granule != 0)
    {
        for (const auto & column : columns)
        {
            auto new_column = column->cloneEmpty();
            /// Fix dynamic structure so it won't changed after insertion of new rows.
            new_column->fixDynamicStructure();
            new_column->insertRangeFrom(*column, column->size() - num_rows_in_new_incomplete_granule, num_rows_in_new_incomplete_granule);
            new_incomplete_granule_columns.push_back(std::move(new_column));
        }
    }

    Columns new_complete_granules_columns;
    if (num_rows_in_old_incomplete_granule != 0)
    {
        new_complete_granules_columns.resize(incomplete_granule_columns.size());
        for (size_t i = 0; i != columns.size(); ++i)
        {
            auto column = std::move(columns[i]);
            auto new_column = std::move(incomplete_granule_columns[i]);
            /// Fix dynamic structure so it won't changed after insertion of new rows.
            new_column->fixDynamicStructure();
            new_column->insertRangeFrom(*column, 0, num_rows_in_new_complete_granules - num_rows_in_old_incomplete_granule);
            new_complete_granules_columns[i] = std::move(new_column);
        }
    }
    else
    {
        /// The rows of the incomplete granule stay at the end of these columns too: cutting them
        /// would copy the data, and the granules bound the rows to write anyway.
        new_complete_granules_columns = std::move(columns);
    }

    complete_granules_columns.push_back(std::move(new_complete_granules_columns));
    incomplete_granule_columns = std::move(new_incomplete_granule_columns);
    for (auto && granule : granules)
    {
        chassert(granule.is_complete);
        complete_granules.emplace_back(granule, std::prev(complete_granules_columns.end()));
    }

    if (!complete_granules.empty())
        last_complete_granule_mark_number = complete_granules.back().first.mark_number;
}

MergeTreeDataPartWriterCompact::ColumnsBuffer::ColumnsAndGranules
MergeTreeDataPartWriterCompact::ColumnsBuffer::flush(size_t num_granules)
{
    if (num_granules > complete_granules.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Requested to flush {} granules, have only {} granules",
            num_granules, complete_granules.size());

    ColumnsAndGranules result;
    auto prev_columns_it = complete_granules_columns.end();
    for (size_t i = 0; i != num_granules; ++i)
    {
        auto columns_it = complete_granules[i].second;
        if (columns_it != prev_columns_it)
        {
            result.emplace_back(*columns_it, Granules{});
            prev_columns_it = columns_it;
        }

        result.back().second.push_back(complete_granules[i].first);
    }

    complete_granules.erase(complete_granules.begin(), complete_granules.begin() + num_granules);
    if (complete_granules.empty())
        complete_granules_columns.clear();
    else
        complete_granules_columns.erase(complete_granules_columns.begin(), complete_granules.front().second);

    return result;
}

MergeTreeDataPartWriterCompact::ColumnsBuffer::ColumnsAndGranules
MergeTreeDataPartWriterCompact::ColumnsBuffer::flushAll()
{
    ColumnsAndGranules result = flush(complete_granules.size());

    auto num_rows_in_incomplete_granule = getNumRowsInLastIncompleteGranule();
    if (num_rows_in_incomplete_granule != 0)
    {
        auto granule = Granule{
            .start_row = 0,
            .rows_to_write = num_rows_in_incomplete_granule,
            .mark_number = size_t(last_complete_granule_mark_number + 1),
            .mark_on_start = true,
            .is_complete = false
        };

        Columns columns;
        for (auto & column : incomplete_granule_columns)
            columns.emplace_back(std::move(column));
        result.emplace_back(std::move(columns), Granules{granule});
        incomplete_granule_columns.clear();
    }

    return result;
}

size_t MergeTreeDataPartWriterCompact::ColumnsBuffer::getNumBytes() const
{
    size_t num_bytes = 0;
    for (const auto & columns : complete_granules_columns)
    {
        for (const auto & column : columns)
            num_bytes += column->byteSize();
    }

    for (const auto & column : incomplete_granule_columns)
        num_bytes += column->byteSize();

    return num_bytes;
}

size_t MergeTreeDataPartWriterCompact::ColumnsBuffer::getNumRowsInLastIncompleteGranule() const
{
    return incomplete_granule_columns.empty() ? 0 : incomplete_granule_columns.at(0)->size();
}

void MergeTreeDataPartWriterCompact::fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & /*checksums_to_remove*/)
{
    // If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        fillDataChecksums(checksums);

    if (settings.rewrite_primary_key)
        fillPrimaryIndexChecksums(checksums);

    fillSkipIndicesChecksums(checksums);
}

void MergeTreeDataPartWriterCompact::finish(bool sync)
{
    /// If we don't have anything to write, skip finalization.
    if (!columns_list.empty())
        finishDataSerialization(sync);

    if (settings.rewrite_primary_key)
        finishPrimaryIndexSerialization(sync);

    finishSkipIndicesSerialization(sync);
}

void MergeTreeDataPartWriterCompact::cancel() noexcept
{
    for (const auto & [_, stream] : streams_by_codec)
    {
        stream->hashing_buf.cancel();
        stream->compressed_buf.cancel();
    }

    plain_hashing.cancel();

    /// plain_file and marks_file may already be released: finishDataSerialization resets them as
    /// soon as the data is flushed and synced, before the part is committed. cancel() can still run
    /// afterwards (e.g. MergeTreeTemporaryPart::cancel from the sink destructor when a quorum INSERT
    /// finishes the part but the subsequent quorum wait throws), so guard against the null streams.
    /// The wrapper buffers above were finalized, so their cancel() is a no-op and never touches the
    /// underlying file. This mirrors the null guards in MergeTreeDataPartWriterOnDisk::cancel.
    if (plain_file)
        plain_file->cancel();

    if (marks_source_hashing)
        marks_source_hashing->cancel();

    if (marks_compressor)
        marks_compressor->cancel();

    marks_file_hashing->cancel();

    if (marks_file)
        marks_file->cancel();

    Base::cancel();
}


}
