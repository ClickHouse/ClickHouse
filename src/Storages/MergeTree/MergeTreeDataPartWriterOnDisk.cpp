#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Common/MemoryTrackerBlockerInThread.h>

#include <utility>
#include "IO/WriteBufferFromFileDecorator.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void MergeTreeDataPartWriterOnDisk::Stream::preFinalize()
{
    compressed.next();
    /// 'compressed_buf' doesn't call next() on underlying buffer ('plain_hashing'). We should do it manually.
    plain_hashing.next();

    if (is_compress_marks)
        marks_compressed.next();

    marks_hashing.next();

    plain_file->preFinalize();
    marks_file->preFinalize();

    is_prefinalized = true;
}

void MergeTreeDataPartWriterOnDisk::Stream::finalize()
{
    if (!is_prefinalized)
        preFinalize();

    plain_file->finalize();
    marks_file->finalize();
}

void MergeTreeDataPartWriterOnDisk::Stream::sync() const
{
    plain_file->sync();
    marks_file->sync();
}

MergeTreeDataPartWriterOnDisk::Stream::Stream(
    const String & escaped_column_name_,
    const DataPartStorageBuilderPtr & data_part_storage_builder,
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
    plain_file(data_part_storage_builder->writeFile(data_path_ + data_file_extension, max_compress_block_size_, query_write_settings)),
    plain_hashing(*plain_file),
    compressed_buf(plain_hashing, compression_codec_, max_compress_block_size_),
    compressed(compressed_buf),
    marks_file(data_part_storage_builder->writeFile(marks_path_ + marks_file_extension, 4096, query_write_settings)),
    marks_hashing(*marks_file),
    marks_compressed_buf(marks_hashing, marks_compression_codec_, marks_compress_block_size_),
    marks_compressed(marks_compressed_buf),
    is_compress_marks(isCompressFromMrkExtension(marks_file_extension))
{
}

void MergeTreeDataPartWriterOnDisk::Stream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    if (is_compress_marks)
    {
        checksums.files[name + marks_file_extension].is_compressed = true;
        checksums.files[name + marks_file_extension].uncompressed_size = marks_compressed.count();
        checksums.files[name + marks_file_extension].uncompressed_hash = marks_compressed.getHash();
    }
    checksums.files[name + marks_file_extension].file_size = marks_hashing.count();
    checksums.files[name + marks_file_extension].file_hash = marks_hashing.getHash();
}


MergeTreeDataPartWriterOnDisk::MergeTreeDataPartWriterOnDisk(
    const MergeTreeData::DataPartPtr & data_part_,
    DataPartStorageBuilderPtr data_part_storage_builder_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeIndices & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : IMergeTreeDataPartWriter(data_part_, std::move(data_part_storage_builder_),
        columns_list_, metadata_snapshot_, settings_, index_granularity_)
    , skip_indices(indices_to_recalc_)
    , marks_file_extension(marks_file_extension_)
    , default_codec(default_codec_)
    , compute_granularity(index_granularity.empty())
    , is_compress_primary_key(settings.is_compress_primary_key)
{
    if (settings.blocks_are_granules_size && !index_granularity.empty())
        throw Exception("Can't take information about index granularity from blocks, when non empty index_granularity array specified", ErrorCodes::LOGICAL_ERROR);

    if (!data_part_storage_builder->exists())
        data_part_storage_builder->createDirectories();

    if (settings.rewrite_primary_key)
        initPrimaryIndex();
    initSkipIndices();
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
        index_granularity_for_block = fixed_index_granularity_rows;
    else
    {
        size_t block_size_in_memory = block.bytes();
        if (blocks_are_granules)
            index_granularity_for_block = rows_in_block;
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
    if (index_granularity_for_block == 0) /// very rare case when index granularity bytes less then single row
        index_granularity_for_block = 1;

    /// We should be less or equal than fixed index granularity
    index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);
    return index_granularity_for_block;
}

size_t MergeTreeDataPartWriterOnDisk::computeIndexGranularity(const Block & block) const
{
    const auto storage_settings = storage.getSettings();
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
        String index_name = "primary" + getIndexExtension(is_compress_primary_key);
        index_file_stream = data_part_storage_builder->writeFile(index_name, DBMS_DEFAULT_BUFFER_SIZE, settings.query_write_settings);
        index_hashing_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);

        if (is_compress_primary_key)
        {
            ParserCodec codec_parser;
            auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(settings.primary_key_compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
            CompressionCodecPtr primary_key_compression_codec = CompressionCodecFactory::instance().get(ast, nullptr);
            index_compressed_buf = std::make_unique<CompressedWriteBuffer>(*index_hashing_stream, primary_key_compression_codec, settings.primary_key_compress_block_size);
            index_compressed_stream = std::make_unique<HashingWriteBuffer>(*index_compressed_buf);
        }
    }
}

void MergeTreeDataPartWriterOnDisk::initSkipIndices()
{
    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(settings.marks_compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    CompressionCodecPtr marks_compression_codec = CompressionCodecFactory::instance().get(ast, nullptr);

    for (const auto & index_helper : skip_indices)
    {
        String stream_name = index_helper->getFileName();
        skip_indices_streams.emplace_back(
                std::make_unique<MergeTreeDataPartWriterOnDisk::Stream>(
                        stream_name,
                        data_part_storage_builder,
                        stream_name, index_helper->getSerializedFileExtension(),
                        stream_name, marks_file_extension,
                        default_codec, settings.max_compress_block_size,
                        marks_compression_codec, settings.marks_compress_block_size,
                        settings.query_write_settings));
        skip_indices_aggregators.push_back(index_helper->createIndexAggregator());
        skip_index_accumulated_marks.push_back(0);
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndex(const Block & primary_index_block, const Granules & granules_to_write)
{
    size_t primary_columns_num = primary_index_block.columns();
    if (index_columns.empty())
    {
        index_types = primary_index_block.getDataTypes();
        index_columns.resize(primary_columns_num);
        last_block_index_columns.resize(primary_columns_num);
        for (size_t i = 0; i < primary_columns_num; ++i)
            index_columns[i] = primary_index_block.getByPosition(i).column->cloneEmpty();
    }

    {
        /** While filling index (index_columns), disable memory tracker.
         * Because memory is allocated here (maybe in context of INSERT query),
         *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
         * And otherwise it will look like excessively growing memory consumption in context of query.
         *  (observed in long INSERT SELECTs)
         */
        MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (const auto & granule : granules_to_write)
        {
            if (metadata_snapshot->hasPrimaryKey() && granule.mark_on_start)
            {
                for (size_t j = 0; j < primary_columns_num; ++j)
                {
                    const auto & primary_column = primary_index_block.getByPosition(j);
                    index_columns[j]->insertFrom(*primary_column.column, granule.start_row);
                    primary_column.type->getDefaultSerialization()->serializeBinary(
                        *primary_column.column, granule.start_row, is_compress_primary_key ? *index_compressed_stream : *index_hashing_stream);
                }
            }
        }
    }

    /// store last index row to write final mark at the end of column
    for (size_t j = 0; j < primary_columns_num; ++j)
        last_block_index_columns[j] = primary_index_block.getByPosition(j).column;
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices(const Block & skip_indexes_block, const Granules & granules_to_write)
{
    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index_helper = skip_indices[i];
        auto & stream = *skip_indices_streams[i];
        for (const auto & granule : granules_to_write)
        {
            if (skip_index_accumulated_marks[i] == index_helper->index.granularity)
            {
                skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
                skip_index_accumulated_marks[i] = 0;
            }

            if (skip_indices_aggregators[i]->empty() && granule.mark_on_start)
            {
                skip_indices_aggregators[i] = index_helper->createIndexAggregator();

                if (stream.compressed.offset() >= settings.min_compress_block_size)
                    stream.compressed.next();

                writeIntBinary(stream.plain_hashing.count(), stream.is_compress_marks? stream.marks_compressed : stream.marks_hashing);
                writeIntBinary(stream.compressed.offset(), stream.is_compress_marks? stream.marks_compressed : stream.marks_hashing);
                /// Actually this numbers is redundant, but we have to store them
                /// to be compatible with normal .mrk2 file format
                if (settings.can_use_adaptive_granularity)
                    writeIntBinary(1UL, stream.is_compress_marks? stream.marks_compressed : stream.marks_hashing);
            }

            size_t pos = granule.start_row;
            skip_indices_aggregators[i]->update(skip_indexes_block, &pos, granule.rows_to_write);
            if (granule.is_complete)
                ++skip_index_accumulated_marks[i];
        }
    }
}

void MergeTreeDataPartWriterOnDisk::fillPrimaryIndexChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    bool write_final_mark = (with_final_mark && data_written);
    if (write_final_mark && compute_granularity)
        index_granularity.appendMark(0);

    if (index_hashing_stream)
    {
        if (write_final_mark)
        {
            for (size_t j = 0; j < index_columns.size(); ++j)
            {
                const auto & column = *last_block_index_columns[j];
                size_t last_row_number = column.size() - 1;
                index_columns[j]->insertFrom(column, last_row_number);
                index_types[j]->getDefaultSerialization()->serializeBinary(
                    column, last_row_number, is_compress_primary_key ? *index_compressed_stream : *index_hashing_stream);
            }
            last_block_index_columns.clear();
        }

        if (is_compress_primary_key)
            index_compressed_stream->next();

        index_hashing_stream->next();

        String index_name = "primary" + getIndexExtension(is_compress_primary_key);
        if (is_compress_primary_key)
        {
            checksums.files[index_name].is_compressed = true;
            checksums.files[index_name].uncompressed_size = index_compressed_stream->count();
            checksums.files[index_name].uncompressed_hash = index_compressed_stream->getHash();
        }
        checksums.files[index_name].file_size = index_hashing_stream->count();
        checksums.files[index_name].file_hash = index_hashing_stream->getHash();
        index_file_stream->preFinalize();
    }
}

void MergeTreeDataPartWriterOnDisk::finishPrimaryIndexSerialization(bool sync)
{
    if (index_hashing_stream)
    {
        index_file_stream->finalize();
        if (sync)
            index_file_stream->sync();

        if (is_compress_primary_key)
        {
            index_compressed_stream = nullptr;
            index_compressed_buf = nullptr;
        }
        index_hashing_stream = nullptr;
    }
}

void MergeTreeDataPartWriterOnDisk::fillSkipIndicesChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->preFinalize();
        stream->addToChecksums(checksums);
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

}
