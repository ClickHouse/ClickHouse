#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr auto INDEX_FILE_EXTENSION = ".idx";
}

void IMergeTreeDataPartWriter::Stream::finalize()
{
    compressed.next();
    plain_file->next();
    marks.next();
}

void IMergeTreeDataPartWriter::Stream::sync() const
{
    plain_file->sync();
    marks_file->sync();
}

IMergeTreeDataPartWriter::Stream::Stream(
    const String & escaped_column_name_,
    DiskPtr disk_,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    size_t estimated_size_,
    size_t aio_threshold_) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(disk_->writeFile(data_path_ + data_file_extension, max_compress_block_size_, WriteMode::Rewrite, estimated_size_, aio_threshold_)),
    plain_hashing(*plain_file), compressed_buf(plain_hashing, compression_codec_), compressed(compressed_buf),
    marks_file(disk_->writeFile(marks_path_ + marks_file_extension, 4096, WriteMode::Rewrite)), marks(*marks_file)
{
}

void IMergeTreeDataPartWriter::Stream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    checksums.files[name + marks_file_extension].file_size = marks.count();
    checksums.files[name + marks_file_extension].file_hash = marks.getHash();
}


IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const std::vector<MergeTreeIndexPtr> & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : data_part(data_part_)
    , part_path(data_part_->getFullRelativePath())
    , storage(data_part_->storage)
    , columns_list(columns_list_)
    , marks_file_extension(marks_file_extension_)
    , index_granularity(index_granularity_)
    , default_codec(default_codec_)
    , skip_indices(indices_to_recalc_)
    , settings(settings_)
    , compute_granularity(index_granularity.empty())
    , with_final_mark(storage.getSettings()->write_final_mark && settings.can_use_adaptive_granularity)
{
    if (settings.blocks_are_granules_size && !index_granularity.empty())
        throw Exception("Can't take information about index granularity from blocks, when non empty index_granularity array specified", ErrorCodes::LOGICAL_ERROR);

    auto disk = data_part->volume->getDisk();
    if (!disk->exists(part_path))
        disk->createDirectories(part_path);
}

IMergeTreeDataPartWriter::~IMergeTreeDataPartWriter() = default;

/// Implemetation is splitted into static functions for ability
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
            size_t size_of_row_in_bytes = block_size_in_memory / rows_in_block;
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }
    if (index_granularity_for_block == 0) /// very rare case when index granularity bytes less then single row
        index_granularity_for_block = 1;

    /// We should be less or equal than fixed index granularity
    index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);
    return index_granularity_for_block;
}

static void fillIndexGranularityImpl(
    MergeTreeIndexGranularity & index_granularity,
    size_t index_offset,
    size_t index_granularity_for_block,
    size_t rows_in_block)
{
    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

size_t IMergeTreeDataPartWriter::computeIndexGranularity(const Block & block)
{
    const auto storage_settings = storage.getSettings();
    return computeIndexGranularityImpl(
            block,
            storage_settings->index_granularity_bytes,
            storage_settings->index_granularity,
            settings.blocks_are_granules_size,
            settings.can_use_adaptive_granularity);
}

void IMergeTreeDataPartWriter::fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block)
{
    fillIndexGranularityImpl(
        index_granularity,
        index_offset,
        index_granularity_for_block,
        rows_in_block);
}

void IMergeTreeDataPartWriter::initPrimaryIndex()
{
    if (storage.hasPrimaryKey())
    {
        index_file_stream = data_part->volume->getDisk()->writeFile(part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        index_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);
    }

    primary_index_initialized = true;
}

void IMergeTreeDataPartWriter::initSkipIndices()
{
    for (const auto & index_helper : skip_indices)
    {
        String stream_name = index_helper->getFileName();
        skip_indices_streams.emplace_back(
                std::make_unique<IMergeTreeDataPartWriter::Stream>(
                        stream_name,
                        data_part->volume->getDisk(),
                        part_path + stream_name, INDEX_FILE_EXTENSION,
                        part_path + stream_name, marks_file_extension,
                        default_codec, settings.max_compress_block_size,
                        0, settings.aio_threshold));
        skip_indices_aggregators.push_back(index_helper->createIndexAggregator());
        skip_index_filling.push_back(0);
    }

    skip_indices_initialized = true;
}

void IMergeTreeDataPartWriter::calculateAndSerializePrimaryIndex(const Block & primary_index_block, size_t rows)
{
    if (!primary_index_initialized)
        throw Exception("Primary index is not initialized", ErrorCodes::LOGICAL_ERROR);

    size_t primary_columns_num = primary_index_block.columns();
    if (index_columns.empty())
    {
        index_types = primary_index_block.getDataTypes();
        index_columns.resize(primary_columns_num);
        last_index_row.resize(primary_columns_num);
        for (size_t i = 0; i < primary_columns_num; ++i)
            index_columns[i] = primary_index_block.getByPosition(i).column->cloneEmpty();
    }

    /** While filling index (index_columns), disable memory tracker.
     * Because memory is allocated here (maybe in context of INSERT query),
     *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
     * And otherwise it will look like excessively growing memory consumption in context of query.
     *  (observed in long INSERT SELECTs)
     */
    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    /// Write index. The index contains Primary Key value for each `index_granularity` row.

    size_t current_row = index_offset;
    size_t total_marks = index_granularity.getMarksCount();

    while (index_mark < total_marks && current_row < rows)
    {
        if (storage.hasPrimaryKey())
        {
            for (size_t j = 0; j < primary_columns_num; ++j)
            {
                const auto & primary_column = primary_index_block.getByPosition(j);
                index_columns[j]->insertFrom(*primary_column.column, current_row);
                primary_column.type->serializeBinary(*primary_column.column, current_row, *index_stream);
            }
        }

        current_row += index_granularity.getMarkRows(index_mark++);
    }

    /// store last index row to write final mark at the end of column
    for (size_t j = 0; j < primary_columns_num; ++j)
    {
        const IColumn & primary_column = *primary_index_block.getByPosition(j).column.get();
        primary_column.get(rows - 1, last_index_row[j]);
    }
}

void IMergeTreeDataPartWriter::calculateAndSerializeSkipIndices(
        const Block & skip_indexes_block, size_t rows)
{
    if (!skip_indices_initialized)
        throw Exception("Skip indices are not initialized", ErrorCodes::LOGICAL_ERROR);

    size_t skip_index_current_data_mark = 0;

    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index_helper = skip_indices[i];
        auto & stream = *skip_indices_streams[i];
        size_t prev_pos = 0;
        skip_index_current_data_mark = skip_index_data_mark;
        while (prev_pos < rows)
        {
            UInt64 limit = 0;
            if (prev_pos == 0 && index_offset != 0)
            {
                limit = index_offset;
            }
            else
            {
                limit = index_granularity.getMarkRows(skip_index_current_data_mark);
                if (skip_indices_aggregators[i]->empty())
                {
                    skip_indices_aggregators[i] = index_helper->createIndexAggregator();
                    skip_index_filling[i] = 0;

                    if (stream.compressed.offset() >= settings.min_compress_block_size)
                        stream.compressed.next();

                    writeIntBinary(stream.plain_hashing.count(), stream.marks);
                    writeIntBinary(stream.compressed.offset(), stream.marks);
                    /// Actually this numbers is redundant, but we have to store them
                    /// to be compatible with normal .mrk2 file format
                    if (settings.can_use_adaptive_granularity)
                        writeIntBinary(1UL, stream.marks);
                }
                /// this mark is aggregated, go to the next one
                skip_index_current_data_mark++;
            }

            size_t pos = prev_pos;
            skip_indices_aggregators[i]->update(skip_indexes_block, &pos, limit);

            if (pos == prev_pos + limit)
            {
                ++skip_index_filling[i];

                /// write index if it is filled
                if (skip_index_filling[i] == index_helper->index.granularity)
                {
                    skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
                    skip_index_filling[i] = 0;
                }
            }
            prev_pos = pos;
        }
    }
    skip_index_data_mark = skip_index_current_data_mark;
}

void IMergeTreeDataPartWriter::finishPrimaryIndexSerialization(MergeTreeData::DataPart::Checksums & checksums)
{
    bool write_final_mark = (with_final_mark && data_written);
    if (write_final_mark && compute_granularity)
        index_granularity.appendMark(0);

    if (index_stream)
    {
        if (write_final_mark)
        {
            for (size_t j = 0; j < index_columns.size(); ++j)
            {
                index_columns[j]->insert(last_index_row[j]);
                index_types[j]->serializeBinary(last_index_row[j], *index_stream);
            }

            last_index_row.clear();
        }

        index_stream->next();
        checksums.files["primary.idx"].file_size = index_stream->count();
        checksums.files["primary.idx"].file_hash = index_stream->getHash();
        index_stream = nullptr;
    }
}

void IMergeTreeDataPartWriter::finishSkipIndicesSerialization(
        MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->finalize();
        stream->addToChecksums(checksums);
    }

    skip_indices_streams.clear();
    skip_indices_aggregators.clear();
    skip_index_filling.clear();
}

void IMergeTreeDataPartWriter::next()
{
    current_mark = next_mark;
    index_offset = next_index_offset;
}

}
