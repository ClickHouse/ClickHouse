#include <Compression/CompressionFactory.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ParallelSyncFiles.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/NullWriteBuffer.h>
#include <Common/FailPoint.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>

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
    MergeTreeIndexGranularityPtr index_granularity_,
    const PlannedMapKeyColumnsKeys & map_key_columns_keys_)
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
    , map_key_columns_keys(map_key_columns_keys_)
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

void MergeTreeDataPartWriterCompact::addStream(const NameAndTypePair & name_and_type, const ISerialization::SubstreamPath & substream_path, const ASTPtr & effective_codec_desc)
{
    const bool column_uses_default_codec = columnUsesDefaultCodec(name_and_type.getNameInStorage());

    chassert(!substream_path.empty());
    String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

    /// Shared offsets for Nested type.
    if (compressed_streams.contains(stream_name))
        return;

    auto compression_codec = getSubstreamCodec(effective_codec_desc, substream_path, column_uses_default_codec);

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
}

void MergeTreeDataPartWriterCompact::addStreams(const NameAndTypePair & name_and_type, const ASTPtr & effective_codec_desc)
{
    ISerialization::StreamCallback callback = [&](const auto & substream_path)
    {
        addStream(name_and_type, substream_path, effective_codec_desc);
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

/// Write single granule of one column (rows between 2 marks). A caller-provided `state`
/// (e.g. the seeded state of a `with_key_columns` Map column) is adopted by the state
/// prefix instead of a fresh state.
void writeColumnSingleGranule(
    const ColumnWithTypeAndName & column,
    const ColumnWithTypeAndName & sample_column,
    const SerializationPtr & serialization,
    ISerialization::OutputStreamGetter stream_getter,
    ISerialization::StreamMarkGetter stream_mark_getter,
    size_t from_row,
    size_t number_of_rows,
    bool is_first_granule,
    ISerialization::SerializeBinaryBulkSettings && serialize_settings,
    ISerialization::SerializeBinaryBulkStatePtr & state)
{
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
    serialize_settings.max_buckets_in_map = settings.max_buckets_in_map;
    serialize_settings.map_buckets_strategy = settings.map_buckets_strategy;
    serialize_settings.map_buckets_coefficient = settings.map_buckets_coefficient;
    serialize_settings.map_buckets_min_avg_size = settings.map_buckets_min_avg_size;
    serialize_settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::PREFIX;
    serialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    serialize_settings.data_part_type = MergeTreeDataPartType::Compact;

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

    /// A `with_key_columns` Map fixes its part-level key set before the first mark of the
    /// part is recorded: the key set comes from `map_key_columns_keys` (precomputed over the
    /// complete part block by `MergeTreeDataWriter::writeTempPartImpl`, or the planned union
    /// for merges), unioned with the keys of this block, so a key first present in a later
    /// block is inside the seeded set. The per-key streams are opened now — after
    /// `initColumnsSubstreamsIfNeeded` declared the base substreams (`m.keys`) and before any
    /// mark exists — and appended to `columns_substreams`. The seed state survives into
    /// `writeDataBlock`, where the first granule's state prefix adopts it.
    for (const auto & column : columns_list)
    {
        const auto * per_key = typeid_cast<const SerializationMapKeyColumns *>(getSerialization(column.name).get());
        if (!per_key || map_key_columns_states.contains(column.name))
            continue;

        std::vector<String> planned_keys;
        std::vector<String> column_keys = per_key->collectColumnKeys(*result_block.getByName(column.name).column);
        if (auto planned_it = map_key_columns_keys.find(column.name); planned_it != map_key_columns_keys.end())
        {
            planned_keys = planned_it->second;
            planned_keys.insert(planned_keys.end(), column_keys.begin(), column_keys.end());
            std::sort(planned_keys.begin(), planned_keys.end());
            planned_keys.erase(std::unique(planned_keys.begin(), planned_keys.end()), planned_keys.end());
        }
        else
        {
            planned_keys = std::move(column_keys);
        }

        auto seed_state = SerializationMapKeyColumns::createSeedKeysState(planned_keys);

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

        auto compression = getCodecDescriptionOrDefault(column.name, default_codec);
        /// Enumerate from the seed state, not from the block column: the block column
        /// path would collect keys by scanning the sample column, and the first
        /// `writeDataBlock` may see only a prefix of the part's rows (the rest sits in
        /// `columns_buffer`), so its keys can be a subset of the seeded set.
        auto stream_data = ISerialization::SubstreamData(getSerialization(column.name)).withType(column.type).withSerializeState(seed_state);
        getSerialization(column.name)->enumerateStreams(
            enumerate_settings,
            [&](const auto & substream_path)
            {
                String stream_name = ISerialization::getFileNameForStream(column, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
                if (!compressed_streams.contains(stream_name))
                {
                    addStream(column, substream_path, compression);
                    if (index_granularity_info.mark_type.with_substreams)
                        columns_substreams.addSubstreamToColumn(column.name, stream_name);
                }
            },
            stream_data);

        map_key_columns_states.emplace(column.name, std::move(seed_state));
    }

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

    /// `block` here is already fully permuted by `permuteBlockIfNeeded` in `write`,
    /// so we pass `permutation = nullptr` and no cache — only Wide writer benefits
    /// from the permuted columns cache (see comment in `MergeTreeDataPartWriterCompact::write`).
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
    auto * cached_marks_ptr = cached_marks.empty() ? nullptr : cached_marks.begin()->second.get();

    /// Tricky part, because we share compressed streams between different columns substreams.
    /// Compressed streams write data to the single file, but with different compression codecs.
    /// So we flush each stream (using next()) before using new one, because otherwise we will override
    /// data in result file. The flush decision is local to one granule: the first substream of
    /// every column flushes the pending data of the stream used by the previous column, and
    /// `prev_stream` carries the pending stream from one column to the next within the granule.
    for (const auto & granule : granules)
    {
        prev_stream = nullptr;
        prev_stream_name.clear();
        auto name_and_type = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++name_and_type)
        {
            if (map_key_columns_states.contains(name_and_type->name))
            {
                /// A `with_key_columns` Map column writes one compressed block per key
                /// substream per granule, so that reading `m['k']` decodes only that key's
                /// streams.
                writeMapKeyColumnsGranule(block, granule, *name_and_type, marks_out, cached_marks_ptr);
                continue;
            }

            writeColumnGranule(block, *name_and_type, granule, &granule == &granules.front(), marks_out, cached_marks_ptr);
        }

        /// The last column's stream stays pending unless per-column compression flushes it;
        /// flush it now so that the granule's rows count written below closes the granule.
        if (prev_stream)
        {
            prev_stream->hashing_buf.next();
            prev_stream = nullptr;
            prev_stream_name.clear();
        }

        writeBinaryLittleEndian(granule.rows_to_write, marks_out);
        data_written = true;
    }
}

void MergeTreeDataPartWriterCompact::writeColumnGranule(
    const Block & block,
    const NameAndTypePair & name_and_type,
    const Granule & granule,
    bool is_first_granule_of_block,
    WriteBuffer & marks_out,
    MarksInCompressedFile::PlainArray * cached_marks_)
{
    bool is_first_substream = true;
    auto stream_getter = [&, this](const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

        auto stream_it = compressed_streams.find(stream_name);
        if (stream_it == compressed_streams.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stream {} for column {} not found", stream_name, name_and_type.name);

        auto & result_stream = stream_it->second;

        /// Some vector codecs (e.g., SZ3) used for compressing arrays like Array<Float>
        /// require specifying the array dimensions before compression starts.
        /// For 1D arrays, it's simply the length. The dimension is a property of the whole column
        /// and `setAndCheckVectorDimension` accumulates it monotonically, so it only needs to be
        /// computed once per block: rescanning the full column on every granule would make SZ3
        /// writes O(rows * granules) in the insert/merge hot path. Do it while writing the first
        /// granule, before its data is compressed.
        if (is_first_granule_of_block)
        {
            auto compression_codec = result_stream->compressed_buf.getCodec();
            setVectorDimensionsIfNeeded(compression_codec, block.getColumnOrSubcolumnByName(name_and_type.name).column.get());
        }

        /// Write one compressed block per column in granule for more optimal reading.
        if (prev_stream && prev_stream != result_stream)
        {
            /// Offset should be 0, because compressed block is written for every granule.
            chassert(result_stream->hashing_buf.offset() == 0);
            prev_stream->hashing_buf.next();
        }

        /// We have 2 types of marks in Compact part. With or without substreams.
        /// In format without substreams we write single mark per column (here once on the first requested substream).
        /// In format with substreams we write a mark for each column substream.
        if (is_first_substream || index_granularity_info.mark_type.with_substreams)
        {
            MarkInCompressedFile mark{plain_hashing.count(), result_stream->hashing_buf.offset()};
            writeBinaryLittleEndian(mark.offset_in_compressed_file, marks_out);
            writeBinaryLittleEndian(mark.offset_in_decompressed_block, marks_out);

            if (cached_marks_)
                cached_marks_->push_back(mark);

            is_first_substream = false;
        }

        prev_stream = result_stream;
        prev_stream_name = stream_name;

        return &result_stream->hashing_buf;
    };

    auto stream_mark_getter = [&](const ISerialization::SubstreamPath & substream_path) -> MarkInCompressedFile
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
        return {plain_hashing.count(), compressed_streams[stream_name]->hashing_buf.offset()};
    };

    auto serialize_settings = getSerializationSettings();
    serialize_settings.min_compress_block_size = getEffectiveMinCompressBlockSize(name_and_type);

    ISerialization::SerializeBinaryBulkStatePtr state;
    writeColumnSingleGranule(
        block.getByName(name_and_type.name), block_sample.getByName(name_and_type.name),
        getSerialization(name_and_type.name),
        stream_getter, stream_mark_getter, granule.start_row, granule.rows_to_write, !data_written, std::move(serialize_settings), state);

    if (settings.compress_per_column_in_compact_parts)
    {
        prev_stream->hashing_buf.next();
        prev_stream = nullptr;
        prev_stream_name.clear();
    }
}

void MergeTreeDataPartWriterCompact::writeMapKeyColumnsGranule(
    const Block & block,
    const Granule & granule,
    const NameAndTypePair & name_and_type,
    WriteBuffer & marks_out,
    MarksInCompressedFile::PlainArray * cached_marks_)
{
    auto & serialize_state = map_key_columns_states.at(name_and_type.name);
    auto serialization = getSerialization(name_and_type.name);

    /// Marks are written to the marks file in the order the substreams were recorded in
    /// `columns_substreams` (the enumeration order of `enumerateWriteStreams`), but the
    /// serialization requests the streams in its own order (all keys' prefixes first,
    /// then per-key data), so the marks are collected by stream name and flushed at the
    /// end of the granule in the recorded order.
    std::unordered_map<String, MarkInCompressedFile> marks_by_stream;

    auto stream_getter = [&, this](const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));

        auto stream_it = compressed_streams.find(stream_name);
        if (stream_it == compressed_streams.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stream {} for column {} not found", stream_name, name_and_type.name);

        auto & result_stream = stream_it->second;

        /// One compressed block per substream per granule: flush the pending data of the
        /// substream written before this one, even when both share the same compressed
        /// stream (e.g. the `m.keys` manifest and a `LowCardinality` key dictionary, both
        /// on the default codec), because the mark recorded below must point at exactly
        /// this substream's bytes. A substream whose prefix wrote nothing (e.g. a plain
        /// UInt8 presence stream) has no pending data; flushing it would emit an empty
        /// compressed block and shift every later mark.
        if (prev_stream && prev_stream->hashing_buf.offset() > 0
            && (prev_stream != result_stream || prev_stream_name != stream_name))
        {
            prev_stream->hashing_buf.next();
            prev_stream = nullptr;
        }

        /// A `with_key_columns` Map column always uses the substream marks format:
        /// record the mark of every substream in every granule, at most once per stream.
        if (!marks_by_stream.contains(stream_name))
            marks_by_stream.emplace(stream_name, MarkInCompressedFile{plain_hashing.count(), result_stream->hashing_buf.offset()});

        prev_stream = result_stream;
        prev_stream_name = stream_name;
        return &result_stream->hashing_buf;
    };

    auto stream_mark_getter = [&](const ISerialization::SubstreamPath & substream_path) -> MarkInCompressedFile
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path, ISerialization::StreamFileNameSettings(*storage_settings));
        return {plain_hashing.count(), compressed_streams[stream_name]->hashing_buf.offset()};
    };

    auto serialize_settings = getSerializationSettings();
    serialize_settings.min_compress_block_size = getEffectiveMinCompressBlockSize(name_and_type);
    serialize_settings.getter = stream_getter;
    serialize_settings.stream_mark_getter = stream_mark_getter;
    serialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    serialize_settings.data_part_type = MergeTreeDataPartType::Compact;
    if (granule.start_row != 0)
        serialize_settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::PREFIX_EMPTY;

    serialization->serializeBinaryBulkStatePrefix(*block_sample.getByName(name_and_type.name).column, serialize_settings, serialize_state);
    serialization->serializeBinaryBulkWithMultipleStreams(
        *block.getByName(name_and_type.name).column, granule.start_row, granule.rows_to_write, serialize_settings, serialize_state);
    serialization->serializeBinaryBulkStateSuffix(serialize_settings, serialize_state);

    /// Flush the marks in the recorded substream order of the column.
    const auto * column_substreams_order = columns_substreams.tryGetColumnSubstreams(name_and_type.name);
    if (!column_substreams_order)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No substreams recorded for column {}", name_and_type.name);
    for (const auto & stream_name : *column_substreams_order)
    {
        auto it = marks_by_stream.find(stream_name);
        if (it == marks_by_stream.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No mark recorded for stream {} of column {}", stream_name, name_and_type.name);

        writeBinaryLittleEndian(it->second.offset_in_compressed_file, marks_out);
        writeBinaryLittleEndian(it->second.offset_in_decompressed_block, marks_out);

        if (cached_marks_)
            cached_marks_->push_back(it->second);
    }

    /// The last substream's data stays pending for the next column (or the granule end) to flush.
}


void MergeTreeDataPartWriterCompact::finalizeIndexGranularity()
{
    /// If no data was written, streams and columns substreams will be uninitialized, but we need them.
    initStreamsIfNeeded();
    initColumnsSubstreamsIfNeeded();

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

#ifndef NDEBUG
    /// Offsets should be 0, because compressed block is written for every granule.
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
        {
            /// Fix dynamic structure so it won't changed after insertion of new rows.
            accumulated_columns[i]->fixDynamicStructure();
            accumulated_columns[i]->insertRangeFrom(*columns[i], 0, columns[i]->size());
        }
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
