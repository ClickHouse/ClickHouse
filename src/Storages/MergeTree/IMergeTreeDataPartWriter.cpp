#include <Storages/MergeTree/IMergeTreeDataPartIndexWriter.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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

void IMergeTreeDataPartWriter::next()
{
    current_mark = next_mark;
    index_offset = next_index_offset;
}

void IMergeTreeDataPartWriter::initPrimaryIndex()
{
    index_writer->initPrimaryIndex();
}

void IMergeTreeDataPartWriter::initSkipIndices()
{
    index_writer->initSkipIndices();
}

void IMergeTreeDataPartWriter::calculateAndSerializePrimaryIndex(const Block & primary_index_block, size_t rows)
{
    index_writer->calculateAndSerializePrimaryIndex(primary_index_block, rows);
}

void IMergeTreeDataPartWriter::calculateAndSerializeSkipIndices(const Block & skip_indexes_block, size_t rows)
{
    index_writer->calculateAndSerializeSkipIndices(skip_indexes_block, rows);
}

void IMergeTreeDataPartWriter::finishPrimaryIndexSerialization(IMergeTreeDataPart::Checksums & checksums)
{
    index_writer->finishPrimaryIndexSerialization(checksums);
}

void IMergeTreeDataPartWriter::finishSkipIndicesSerialization(IMergeTreeDataPart::Checksums & checksums)
{
    index_writer->finishSkipIndicesSerialization(checksums);
}


}
