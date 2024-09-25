#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeIndexFullText.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event MergeTreeDataWriterSkipIndicesCalculationMicroseconds;
extern const Event MergeTreeDataWriterStatisticsCalculationMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::preFinalize()
{
    /// Here the main goal is to do preFinalize calls for plain_file and marks_file
    /// Before that all hashing and compression buffers have to be finalized
    /// Otherwise some data might stuck in the buffers above plain_file and marks_file
    /// Also the order is important

    compressed_hashing.finalize();
    compressor.finalize();
    plain_hashing.finalize();

    if constexpr (!only_plain_file)
    {
        if (compress_marks)
        {
            marks_compressed_hashing.finalize();
            marks_compressor.finalize();
        }

        marks_hashing.finalize();
    }

    plain_file->preFinalize();
    if constexpr (!only_plain_file)
        marks_file->preFinalize();

    is_prefinalized = true;
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::finalize()
{
    if (!is_prefinalized)
        preFinalize();

    plain_file->finalize();
    if constexpr (!only_plain_file)
        marks_file->finalize();
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::sync() const
{
    plain_file->sync();
    if constexpr (!only_plain_file)
        marks_file->sync();
}

template<>
MergeTreeDataPartWriterOnDisk::Stream<false>::Stream(
    const String & escaped_column_name_,
    const MutableDataPartStoragePtr & data_part_storage,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    const CompressionCodecPtr & marks_compression_codec_,
    size_t marks_compress_block_size_,
    const WriteSettings & query_write_settings) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(data_part_storage->writeFile(data_path_ + data_file_extension, max_compress_block_size_, query_write_settings)),
    plain_hashing(*plain_file),
    compressor(plain_hashing, compression_codec_, max_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    compressed_hashing(compressor),
    marks_file(data_part_storage->writeFile(marks_path_ + marks_file_extension, 4096, query_write_settings)),
    marks_hashing(*marks_file),
    marks_compressor(marks_hashing, marks_compression_codec_, marks_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    marks_compressed_hashing(marks_compressor),
    compress_marks(MarkType(marks_file_extension).compressed)
{
}

template<>
MergeTreeDataPartWriterOnDisk::Stream<true>::Stream(
    const String & escaped_column_name_,
    const MutableDataPartStoragePtr & data_part_storage,
    const String & data_path_,
    const std::string & data_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    const WriteSettings & query_write_settings) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    plain_file(data_part_storage->writeFile(data_path_ + data_file_extension, max_compress_block_size_, query_write_settings)),
    plain_hashing(*plain_file),
    compressor(plain_hashing, compression_codec_, max_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    compressed_hashing(compressor),
    compress_marks(false)
{
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed_hashing.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed_hashing.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    if constexpr (!only_plain_file)
    {
        if (compress_marks)
        {
            checksums.files[name + marks_file_extension].is_compressed = true;
            checksums.files[name + marks_file_extension].uncompressed_size = marks_compressed_hashing.count();
            checksums.files[name + marks_file_extension].uncompressed_hash = marks_compressed_hashing.getHash();
        }

        checksums.files[name + marks_file_extension].file_size = marks_hashing.count();
        checksums.files[name + marks_file_extension].file_hash = marks_hashing.getHash();
    }
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
    const ColumnsStatistics & stats_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : IMergeTreeDataPartWriter(
        data_part_name_, serializations_, data_part_storage_, index_granularity_info_,
        storage_settings_, columns_list_, metadata_snapshot_, virtual_columns_, settings_, index_granularity_)
    , skip_indices(indices_to_recalc_)
    , stats(stats_to_recalc_)
    , marks_file_extension(marks_file_extension_)
    , default_codec(default_codec_)
    , compute_granularity(index_granularity.empty())
    , compress_primary_key(settings.compress_primary_key)
    , execution_stats(skip_indices.size(), stats.size())
    , log(getLogger(logger_name_ + " (DataPartWriter)"))
{
    if (settings.blocks_are_granules_size && !index_granularity.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Can't take information about index granularity from blocks, when non empty index_granularity array specified");

    if (!getDataPartStorage().exists())
        getDataPartStorage().createDirectories();

    if (settings.rewrite_primary_key)
        initPrimaryIndex();

    initSkipIndices();
    initStatistics();
}

// Implementation is split into static functions for ability
/// of making unit tests without creation instance of IMergeTreeDataPartWriter,
/// which requires a lot of dependencies and access to filesystem.
static size_t computeIndexGranularityImpl(
    const Block & block,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    bool can_use_adaptive_index_granularity)
{
    size_t rows_in_block = block.rows();
    size_t index_granularity_for_block;

    if (!can_use_adaptive_index_granularity)
    {
        index_granularity_for_block = fixed_index_granularity_rows;
    }
    else
    {
        size_t block_size_in_memory = block.bytes();
        if (blocks_are_granules)
        {
            index_granularity_for_block = rows_in_block;
        }
        else if (block_size_in_memory >= index_granularity_bytes)
        {
            size_t granules_in_block = block_size_in_memory / index_granularity_bytes;
            index_granularity_for_block = rows_in_block / granules_in_block;
        }
        else
        {
            size_t size_of_row_in_bytes = std::max(block_size_in_memory / rows_in_block, 1UL);
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }

    /// We should be less or equal than fixed index granularity.
    /// But if block size is a granule size then do not adjust it.
    /// Granularity greater than fixed granularity might come from compact part.
    if (!blocks_are_granules)
        index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);

    /// Very rare case when index granularity bytes less than single row.
    if (index_granularity_for_block == 0)
        index_granularity_for_block = 1;

    return index_granularity_for_block;
}

size_t MergeTreeDataPartWriterOnDisk::computeIndexGranularity(const Block & block) const
{
    return computeIndexGranularityImpl(
            block,
            storage_settings->index_granularity_bytes,
            storage_settings->index_granularity,
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

void MergeTreeDataPartWriterOnDisk::initStatistics()
{
    for (const auto & stat_ptr : stats)
    {
        String stats_name = stat_ptr->getFileName();
        stats_streams.emplace_back(std::make_unique<MergeTreeDataPartWriterOnDisk::Stream<true>>(
                                       stats_name,
                                       data_part_storage,
                                       stats_name, STATS_FILE_SUFFIX,
                                       default_codec, settings.max_compress_block_size,
                                       settings.query_write_settings));
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
        String stream_name = skip_index->getFileName();
        skip_indices_streams.emplace_back(
                std::make_unique<MergeTreeDataPartWriterOnDisk::Stream<false>>(
                        stream_name,
                        data_part_storage,
                        stream_name, skip_index->getSerializedFileExtension(),
                        stream_name, marks_file_extension,
                        default_codec, settings.max_compress_block_size,
                        marks_compression_codec, settings.marks_compress_block_size,
                        settings.query_write_settings));

        GinIndexStorePtr store = nullptr;
        if (typeid_cast<const MergeTreeIndexFullText *>(&*skip_index) != nullptr)
        {
            store = std::make_shared<GinIndexStore>(stream_name, data_part_storage, data_part_storage, storage_settings->max_digestion_size_per_segment);
            gin_index_stores[stream_name] = store;
        }

        skip_indices_aggregators.push_back(skip_index->createIndexAggregatorForPart(store, settings));
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

        index_columns[i]->insertFrom(*column, row);
        index_serializations[i]->serializeBinary(*column, row, index_stream, {});
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

        if (index_columns.empty())
            index_columns = primary_index_block.cloneEmptyColumns();

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

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeStatistics(const Block & block)
{
    for (size_t i = 0; i < stats.size(); ++i)
    {
        const auto & stat_ptr = stats[i];
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterStatisticsCalculationMicroseconds);
        stat_ptr->build(block.getByName(stat_ptr->columnName()).column);
        execution_stats.statistics_build_us[i] += watch.elapsed();
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices(const Block & skip_indexes_block, const Granules & granules_to_write)
{
    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index_helper = skip_indices[i];
        auto & stream = *skip_indices_streams[i];
        WriteBuffer & marks_out = stream.compress_marks ? stream.marks_compressed_hashing : stream.marks_hashing;

        GinIndexStorePtr store;
        if (typeid_cast<const MergeTreeIndexFullText *>(&*index_helper) != nullptr)
        {
            String stream_name = index_helper->getFileName();
            auto it = gin_index_stores.find(stream_name);
            if (it == gin_index_stores.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index '{}' does not exist", stream_name);
            store = it->second;
        }

        for (const auto & granule : granules_to_write)
        {
            if (skip_index_accumulated_marks[i] == index_helper->index.granularity)
            {
                skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed_hashing);
                skip_index_accumulated_marks[i] = 0;
            }

            if (skip_indices_aggregators[i]->empty() && granule.mark_on_start)
            {
                skip_indices_aggregators[i] = index_helper->createIndexAggregatorForPart(store, settings);

                if (stream.compressed_hashing.offset() >= settings.min_compress_block_size)
                    stream.compressed_hashing.next();

                writeBinaryLittleEndian(stream.plain_hashing.count(), marks_out);
                writeBinaryLittleEndian(stream.compressed_hashing.offset(), marks_out);

                /// Actually this numbers is redundant, but we have to store them
                /// to be compatible with the normal .mrk2 file format
                if (settings.can_use_adaptive_granularity)
                    writeBinaryLittleEndian(1UL, marks_out);
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
        index_granularity.appendMark(0);

    if (index_file_hashing_stream)
    {
        if (write_final_mark && last_index_block)
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
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed_hashing);

        /// Register additional files written only by the full-text index. Required because otherwise DROP TABLE complains about unknown
        /// files. Note that the provided actual checksums are bogus. The problem is that at this point the file writes happened already and
        /// we'd need to re-open + hash the files (fixing this is TODO). For now, CHECK TABLE skips these four files.
        if (typeid_cast<const MergeTreeIndexFullText *>(&*skip_indices[i]) != nullptr)
        {
            String filename_without_extension = skip_indices[i]->getFileName();
            checksums.files[filename_without_extension + ".gin_dict"] = MergeTreeDataPartChecksums::Checksum();
            checksums.files[filename_without_extension + ".gin_post"] = MergeTreeDataPartChecksums::Checksum();
            checksums.files[filename_without_extension + ".gin_seg"] = MergeTreeDataPartChecksums::Checksum();
            checksums.files[filename_without_extension + ".gin_sid"] = MergeTreeDataPartChecksums::Checksum();
        }
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->preFinalize();
        stream->addToChecksums(checksums);
    }
}

void MergeTreeDataPartWriterOnDisk::finishStatisticsSerialization(bool sync)
{
    for (auto & stream : stats_streams)
    {
        stream->finalize();
        if (sync)
            stream->sync();
    }

    for (size_t i = 0; i < stats.size(); ++i)
        LOG_DEBUG(log, "Spent {} ms calculating statistics {} for the part {}", execution_stats.statistics_build_us[i] / 1000, stats[i]->columnName(), data_part_name);
}

void MergeTreeDataPartWriterOnDisk::fillStatisticsChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < stats.size(); i++)
    {
        auto & stream = *stats_streams[i];
        stats[i]->serialize(stream.compressed_hashing);
        stream.preFinalize();
        stream.addToChecksums(checksums);
    }
}

void MergeTreeDataPartWriterOnDisk::finishSkipIndicesSerialization(bool sync)
{
    for (auto & stream : skip_indices_streams)
    {
        stream->finalize();
        if (sync)
            stream->sync();
    }
    for (auto & store: gin_index_stores)
        store.second->finalize();

    for (size_t i = 0; i < skip_indices.size(); ++i)
        LOG_DEBUG(log, "Spent {} ms calculating index {} for the part {}", execution_stats.skip_indices_build_us[i] / 1000, skip_indices[i]->index.name, data_part_name);

    gin_index_stores.clear();
    skip_indices_streams.clear();
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

template struct MergeTreeDataPartWriterOnDisk::Stream<false>;
template struct MergeTreeDataPartWriterOnDisk::Stream<true>;

}
