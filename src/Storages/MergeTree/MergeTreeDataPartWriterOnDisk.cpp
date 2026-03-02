#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Compression/CompressionFactory.h>

namespace ProfileEvents
{
    extern const Event MergeTreeDataWriterSkipIndicesCalculationMicroseconds;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeDataPartWriterOnDisk::MergeTreeDataPartWriterOnDisk(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const VirtualsDescriptionPtr & virtual_columns_,
    const MergeTreeIndices & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    MergeTreeIndexGranularityPtr index_granularity_,
    WrittenOffsetSubstreams * written_offset_substreams_)
    : IMergeTreeDataPartWriter(
        data_part_name_, serializations_, data_part_storage_, index_granularity_info_,
        storage_settings_, columns_list_, metadata_snapshot_, virtual_columns_, settings_, std::move(index_granularity_))
    , skip_indices(indices_to_recalc_)
    , marks_file_extension(marks_file_extension_)
    , default_codec(default_codec_)
    , compute_granularity(index_granularity->empty())
    , compress_primary_key(settings.compress_primary_key)
    , written_offset_substreams(written_offset_substreams_)
    , execution_stats(skip_indices.size())
    , log(getLogger(logger_name_ + " (DataPartWriter)"))
{
    if (settings.blocks_are_granules_size && !index_granularity->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Can't take information about index granularity from blocks, when non empty index_granularity array specified");

    /// We don't need to check if it exists or not, createDirectories doesn't throw
    getDataPartStorage().createDirectories();

    if (settings.rewrite_primary_key)
        initPrimaryIndex();

    initSkipIndices();
}

void MergeTreeDataPartWriterOnDisk::cancel() noexcept
{
    if (index_file_stream)
        index_file_stream->cancel();
    if (index_file_hashing_stream)
        index_file_hashing_stream->cancel();
    if (index_compressor_stream)
        index_compressor_stream->cancel();
    if (index_source_hashing_stream)
        index_source_hashing_stream->cancel();

    for (auto & stream : skip_indices_streams_holders)
        stream->cancel();
}

size_t MergeTreeDataPartWriterOnDisk::computeIndexGranularity(const Block & block) const
{
    return DB::computeIndexGranularity(
        block.rows(),
        block.bytes(),
        (*storage_settings)[MergeTreeSetting::index_granularity_bytes],
        (*storage_settings)[MergeTreeSetting::index_granularity],
        settings.blocks_are_granules_size,
        settings.can_use_adaptive_granularity);
}

void MergeTreeDataPartWriterOnDisk::initPrimaryIndex()
{
    if (metadata_snapshot->hasPrimaryKey())
    {
        String index_name = "primary" + getIndexExtension(compress_primary_key);
        index_file_stream = getDataPartStorage().writeFile(index_name, DBMS_DEFAULT_BUFFER_SIZE, settings.query_write_settings);
        index_file_hashing_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);

        if (compress_primary_key)
        {
            CompressionCodecPtr primary_key_compression_codec = CompressionCodecFactory::instance().get(settings.primary_key_compression_codec);
            index_compressor_stream = std::make_unique<CompressedWriteBuffer>(*index_file_hashing_stream, primary_key_compression_codec, settings.primary_key_compress_block_size);
            index_source_hashing_stream = std::make_unique<HashingWriteBuffer>(*index_compressor_stream);
        }

        const auto & primary_key_types = metadata_snapshot->getPrimaryKey().data_types;
        index_serializations.reserve(primary_key_types.size());

        for (const auto & type : primary_key_types)
            index_serializations.push_back(type->getDefaultSerialization());
    }
}

void MergeTreeDataPartWriterOnDisk::initSkipIndices()
{
    if (skip_indices.empty())
        return;

    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(settings.marks_compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    CompressionCodecPtr marks_compression_codec = CompressionCodecFactory::instance().get(ast, nullptr);

    for (const auto & skip_index : skip_indices)
    {
        auto index_name = skip_index->getFileName();
        auto index_substreams = skip_index->getSubstreams();
        auto & index_streams = skip_indices_streams.emplace_back();

        for (const auto & index_substream : index_substreams)
        {
            auto full_stream_name = index_name + index_substream.suffix;
            auto stream_name = replaceFileNameToHashIfNeeded(full_stream_name, *storage_settings, data_part_storage.get());

            auto stream = std::make_unique<MergeTreeIndexWriterStream>(
                stream_name,
                data_part_storage,
                stream_name,
                index_substream.extension,
                stream_name,
                marks_file_extension,
                default_codec,
                settings.max_compress_block_size,
                marks_compression_codec,
                settings.marks_compress_block_size,
                settings.query_write_settings);

            index_streams[index_substream.type] = stream.get();
            skip_indices_streams_holders.push_back(std::move(stream));

            if (settings.save_marks_in_cache)
                cached_index_marks.emplace(stream_name, std::make_unique<MarksInCompressedFile::PlainArray>());
        }

        skip_indices_aggregators.push_back(skip_index->createIndexAggregator());
        skip_index_accumulated_marks.push_back(0);
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndexRow(const Block & index_block, size_t row)
{
    chassert(index_block.columns() == index_serializations.size());
    auto & index_stream = compress_primary_key ? *index_source_hashing_stream : *index_file_hashing_stream;

    for (size_t i = 0; i < index_block.columns(); ++i)
    {
        const auto & column = index_block.getByPosition(i).column;
        index_serializations[i]->serializeBinary(*column, row, index_stream, {});

        if (settings.save_primary_index_in_memory)
            index_columns[i]->insertFrom(*column, row);
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndex(const Block & primary_index_block, const Granules & granules_to_write)
{
    if (!metadata_snapshot->hasPrimaryKey())
        return;

    {
        /** While filling index (index_columns), disable memory tracker.
         * Because memory is allocated here (maybe in context of INSERT query),
         *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
         * And otherwise it will look like excessively growing memory consumption in context of query.
         *  (observed in long INSERT SELECTs)
         */
        MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

        if (settings.save_primary_index_in_memory && index_columns.empty())
        {
            index_columns = primary_index_block.cloneEmptyColumns();
        }

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (const auto & granule : granules_to_write)
        {
            if (granule.mark_on_start)
                calculateAndSerializePrimaryIndexRow(primary_index_block, granule.start_row);
        }
    }

    /// Store block with last index row to write final mark at the end of column
    if (with_final_mark)
        last_index_block = primary_index_block;
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices(const Block & skip_indexes_block, const Granules & granules_to_write)
{
    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index_helper = skip_indices[i];
        auto & index_streams = skip_indices_streams[i];

        for (const auto & granule : granules_to_write)
        {
            if (skip_index_accumulated_marks[i] == index_helper->index.granularity)
            {
                auto index_granule = skip_indices_aggregators[i]->getGranuleAndReset();
                index_granule->serializeBinaryWithMultipleStreams(index_streams);
                skip_index_accumulated_marks[i] = 0;
            }

            if (skip_indices_aggregators[i]->empty() && granule.mark_on_start)
            {
                skip_indices_aggregators[i] = index_helper->createIndexAggregator();

                for (const auto & [type, stream] : index_streams)
                {
                    auto & marks_out = stream->compress_marks ? stream->marks_compressed_hashing : stream->marks_hashing;

                    if (stream->compressed_hashing.offset() >= settings.min_compress_block_size)
                        stream->compressed_hashing.next();

                    MarkInCompressedFile mark{stream->plain_hashing.count(), stream->compressed_hashing.offset()};

                    writeBinaryLittleEndian(mark.offset_in_compressed_file, marks_out);
                    writeBinaryLittleEndian(mark.offset_in_decompressed_block, marks_out);

                    /// Actually this numbers is redundant, but we have to store them
                    /// to be compatible with the normal .mrk2 file format
                    if (settings.can_use_adaptive_granularity)
                        writeBinaryLittleEndian(1UL, marks_out);

                    if (auto it = cached_index_marks.find(stream->escaped_column_name); it != cached_index_marks.end())
                        it->second->push_back(mark);
                }
            }

            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterSkipIndicesCalculationMicroseconds);

            size_t pos = granule.start_row;
            skip_indices_aggregators[i]->update(skip_indexes_block, &pos, granule.rows_to_write);

            if (granule.is_complete)
                ++skip_index_accumulated_marks[i];

            execution_stats.skip_indices_build_us[i] += watch.elapsed();
        }
    }
}

void MergeTreeDataPartWriterOnDisk::fillPrimaryIndexChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    bool write_final_mark = (with_final_mark && data_written);
    if (write_final_mark && compute_granularity)
        index_granularity->appendMark(0);

    if (index_file_hashing_stream)
    {
        if (write_final_mark && !last_index_block.empty())
        {
            MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;
            calculateAndSerializePrimaryIndexRow(last_index_block, last_index_block.rows() - 1);
        }

        last_index_block.clear();

        if (compress_primary_key)
        {
            index_source_hashing_stream->finalize();
            index_compressor_stream->finalize();
        }

        index_file_hashing_stream->finalize();

        String index_name = "primary" + getIndexExtension(compress_primary_key);
        if (compress_primary_key)
        {
            checksums.files[index_name].is_compressed = true;
            checksums.files[index_name].uncompressed_size = index_source_hashing_stream->count();
            checksums.files[index_name].uncompressed_hash = index_source_hashing_stream->getHash();
        }

        checksums.files[index_name].file_size = index_file_hashing_stream->count();
        checksums.files[index_name].file_hash = index_file_hashing_stream->getHash();

        index_file_stream->preFinalize();
    }
}

void MergeTreeDataPartWriterOnDisk::finishPrimaryIndexSerialization(bool sync)
{
    if (index_file_hashing_stream)
    {
        index_file_stream->finalize();
        if (sync)
            index_file_stream->sync();

        if (compress_primary_key)
        {
            index_source_hashing_stream = nullptr;
            index_compressor_stream = nullptr;
        }

        index_file_hashing_stream = nullptr;
    }
}

void MergeTreeDataPartWriterOnDisk::fillSkipIndicesChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        if (!skip_indices_aggregators[i]->empty())
        {
            auto & index_streams = skip_indices_streams[i];
            auto index_granule = skip_indices_aggregators[i]->getGranuleAndReset();
            index_granule->serializeBinaryWithMultipleStreams(index_streams);
        }
    }

    for (const auto & streams : skip_indices_streams)
    {
        for (const auto & [type, stream] : streams)
        {
            stream->preFinalize();
            stream->addToChecksums(checksums, MergeTreeIndexSubstream::isCompressed(type));
        }
    }
}

void MergeTreeDataPartWriterOnDisk::finishSkipIndicesSerialization(bool sync)
{
    for (auto & stream : skip_indices_streams_holders)
    {
        stream->finalize();
        if (sync)
            stream->sync();
    }

    if (!skip_indices.empty() && log->is(Poco::Message::PRIO_DEBUG))
    {
        UInt64 total_us = 0;
        std::string indices_str;

        /// Create pairs of (index, time_us) for sorting
        std::vector<std::pair<size_t, UInt64>> index_times;
        index_times.reserve(skip_indices.size());

        for (size_t i = 0; i < skip_indices.size(); ++i)
        {
            index_times.emplace_back(i, execution_stats.skip_indices_build_us[i]);
            total_us += execution_stats.skip_indices_build_us[i];
        }

        /// If there are many indices, show only the slowest ones
        constexpr size_t max_indices_to_show = 10;
        if (skip_indices.size() > max_indices_to_show)
        {
            std::partial_sort(
                index_times.begin(),
                index_times.begin() + max_indices_to_show,
                index_times.end(),
                [](const auto & a, const auto & b) { return a.second > b.second; });
            index_times.resize(max_indices_to_show);
        }

        for (size_t i = 0; i < index_times.size(); ++i)
        {
            if (i > 0)
                indices_str += ", ";
            auto [idx, time_us] = index_times[i];
            indices_str += fmt::format("{}: {} ms", skip_indices[idx]->index.name, time_us / 1000);
        }

        if (skip_indices.size() > max_indices_to_show)
            indices_str += fmt::format(" (showing {} slowest out of {})", max_indices_to_show, skip_indices.size());

        LOG_DEBUG(
            log,
            "Spent {} ms calculating {} skip indices for the part {}: [{}]",
            total_us / 1000,
            skip_indices.size(),
            data_part_name,
            indices_str);
    }

    skip_indices_streams.clear();
    skip_indices_streams_holders.clear();
    skip_indices_aggregators.clear();
    skip_index_accumulated_marks.clear();
}

Names MergeTreeDataPartWriterOnDisk::getSkipIndicesColumns() const
{
    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : skip_indices)
        std::copy(index->index.column_names.cbegin(), index->index.column_names.cend(),
                  std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    return Names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());
}

void MergeTreeDataPartWriterOnDisk::initOrAdjustDynamicStructureIfNeeded(Block & block)
{
    if (!is_dynamic_streams_initialized)
    {
        block_sample = block.cloneEmpty();

        for (const auto & column : columns_list)
        {
            if (column.type->hasDynamicSubcolumns())
            {
                /// Create all streams for dynamic subcolumns using dynamic structure from block.
                auto compression = getCodecDescOrDefault(column.name, default_codec);
                addStreams(column, compression);
            }
        }
        is_dynamic_streams_initialized = true;
    }
    else
    {
        size_t size = block.columns();
        for (size_t i = 0; i != size; ++i)
        {
            auto & column = block.getByPosition(i);
            const auto & sample_column = block_sample.getByPosition(i);
            /// Check if the dynamic structure of this column is different from the sample column.
            if (column.type->hasDynamicSubcolumns() && !column.column->dynamicStructureEquals(*sample_column.column))
            {
                /// We need to change the dynamic structure of the column so it matches the sample column.
                /// To do it, we create empty column of this type, take dynamic structure from sample column
                /// and insert data into it. Resulting column will have required dynamic structure and the content
                /// of the column in current block.
                auto new_column = sample_column.type->createColumn();
                new_column->takeDynamicStructureFromColumn(sample_column.column);
                new_column->insertRangeFrom(*column.column, 0, column.column->size());
                column.column = std::move(new_column);
            }
        }
    }
}

}
