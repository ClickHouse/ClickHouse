#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const NamesAndTypesList & columns_list_,
    CompressionCodecPtr default_codec,
    bool blocks_are_granules_size)
    : IMergedBlockOutputStream(data_part)
    , columns_list(columns_list_)
{
    MergeTreeWriterSettings writer_settings(data_part->storage.global_context.getSettings(),
        data_part->storage.canUseAdaptiveGranularity(), blocks_are_granules_size);

    writer = data_part->getWriter(columns_list, data_part->storage.getSkipIndices(), default_codec, std::move(writer_settings));
    init();
}

MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const NamesAndTypesList & columns_list_,
    CompressionCodecPtr default_codec,
    const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size,
    size_t aio_threshold,
    bool blocks_are_granules_size)
    : IMergedBlockOutputStream(data_part)
    , columns_list(columns_list_)
{
    MergeTreeWriterSettings writer_settings(data_part->storage.global_context.getSettings(),
        data_part->storage.canUseAdaptiveGranularity(), blocks_are_granules_size);
    writer_settings.aio_threshold = aio_threshold;

    if (aio_threshold > 0 && !merged_column_to_size.empty())
    {
        for (const auto & column : columns_list)
        {
            auto size_it = merged_column_to_size.find(column.name);
            if (size_it != merged_column_to_size.end())
                writer_settings.estimated_size += size_it->second;
        }
    }

    writer = data_part->getWriter(columns_list,
        data_part->storage.getSkipIndices(), default_codec, writer_settings);
    init();
}

std::string MergedBlockOutputStream::getPartPath() const
{
    return part_path;
}

/// If data is pre-sorted.
void MergedBlockOutputStream::write(const Block & block)
{
    writeImpl(block, nullptr);
}

/** If the data is not sorted, but we pre-calculated the permutation, after which they will be sorted.
    * This method is used to save RAM, since you do not need to keep two blocks at once - the source and the sorted.
    */
void MergedBlockOutputStream::writeWithPermutation(const Block & block, const IColumn::Permutation * permutation)
{
    writeImpl(block, permutation);
}

void MergedBlockOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedBlockOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

void MergedBlockOutputStream::writeSuffixAndFinalizePart(
        MergeTreeData::MutableDataPartPtr & new_part,
        const NamesAndTypesList * total_column_list,
        MergeTreeData::DataPart::Checksums * additional_column_checksums)
{
    /// Finish write and get checksums.
    MergeTreeData::DataPart::Checksums checksums;

    if (additional_column_checksums)
        checksums = std::move(*additional_column_checksums);

    /// Finish columns serialization.
    writer->finishDataSerialization(checksums);
    writer->finishPrimaryIndexSerialization(checksums);
    writer->finishSkipIndicesSerialization(checksums);

    if (!total_column_list)
        total_column_list = &columns_list;

    if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        new_part->partition.store(storage, part_path, checksums);
        if (new_part->minmax_idx.initialized)
            new_part->minmax_idx.store(storage, part_path, checksums);
        else if (rows_count)
            throw Exception("MinMax index was not initialized for new non-empty part " + new_part->name
                + ". It is a bug.", ErrorCodes::LOGICAL_ERROR);
    }

    {
        WriteBufferFromFile count_out(part_path + "count.txt", 4096);
        HashingWriteBuffer count_out_hashing(count_out);
        writeIntText(rows_count, count_out_hashing);
        count_out_hashing.next();
        checksums.files["count.txt"].file_size = count_out_hashing.count();
        checksums.files["count.txt"].file_hash = count_out_hashing.getHash();
    }

    if (!new_part->ttl_infos.empty())
    {
        /// Write a file with ttl infos in json format.
        WriteBufferFromFile out(part_path + "ttl.txt", 4096);
        HashingWriteBuffer out_hashing(out);
        new_part->ttl_infos.write(out_hashing);
        checksums.files["ttl.txt"].file_size = out_hashing.count();
        checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
    }

    {
        /// Write a file with a description of columns.
        WriteBufferFromFile out(part_path + "columns.txt", 4096);
        total_column_list->writeText(out);
    }

    {
        /// Write file with checksums.
        WriteBufferFromFile out(part_path + "checksums.txt", 4096);
        checksums.write(out);
    }

    new_part->rows_count = rows_count;
    new_part->modification_time = time(nullptr);
    new_part->index = writer->releaseIndexColumns();
    new_part->checksums = checksums;
    new_part->bytes_on_disk = checksums.getTotalSizeOnDisk();
    new_part->index_granularity = writer->getIndexGranularity();
}

void MergedBlockOutputStream::init()
{
    Poco::File(part_path).createDirectories();
    writer->initPrimaryIndex();
    writer->initSkipIndices();
}


void MergedBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    block.checkNumberOfRows();
    size_t rows = block.rows();
    if (!rows)
        return;

    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : storage.skip_indices)
        std::copy(index->columns.cbegin(), index->columns.cend(),
                std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    Names skip_indexes_column_names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());

    Block primary_key_block = getBlockAndPermute(block, storage.primary_key_columns, permutation);
    Block skip_indexes_block = getBlockAndPermute(block, skip_indexes_column_names, permutation);

    writer->write(block, permutation, primary_key_block, skip_indexes_block);
    writer->calculateAndSerializeSkipIndices(skip_indexes_block, rows);
    writer->calculateAndSerializePrimaryIndex(primary_key_block, rows);
    writer->next();

    rows_count += rows;
}

}
