#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityInfo.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/NestedUtils.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/MemoryTracker.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    CompressionCodecPtr default_codec_,
    bool blocks_are_granules_size_)
    : IMergedBlockOutputStream(
        data_part_, default_codec_,
        {
            data_part_->storage.global_context.getSettings().min_compress_block_size,
            data_part_->storage.global_context.getSettings().max_compress_block_size,
            data_part_->storage.global_context.getSettings().min_bytes_to_use_direct_io
        },
        blocks_are_granules_size_,
        std::vector<MergeTreeIndexPtr>(std::begin(data_part_->storage.skip_indices), std::end(data_part_->storage.skip_indices)),
        data_part_->storage.canUseAdaptiveGranularity())
    , columns_list(columns_list_)
{ 
    const auto & global_settings = data_part->storage.global_context.getSettings();
    writer = data_part_->getWriter(columns_list_, default_codec_, WriterSettings(global_settings));
}

can_use_adaptive_granularity
index_granularity
skip_indices
blocks_are_granule_size

MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    CompressionCodecPtr default_codec_,
    const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size,
    size_t aio_threshold,
    bool blocks_are_granules_size_)
    : IMergedBlockOutputStream(
        data_part_, default_codec_,
        blocks_are_granules_size_,
        std::vector<MergeTreeIndexPtr>(std::begin(data_part_->storage.skip_indices), std::end(data_part_->storage.skip_indices)),
        data_part_->storage.canUseAdaptiveGranularity())
    , columns_list(columns_list_)
{
    WriterSettings writer_settings(data_part->storage.global_context.getSettings());
    writer_settings.aio_threshold = aio_threshold;

    if (aio_threshold > 0 && !merged_column_to_size.empty())
    {
        for (const auto & it : columns_list)
        {
            auto it2 = merged_column_to_size.find(it.name);
            if (it2 != merged_column_to_size.end())
                writer_settings.estimated_size += it2->second;
        }
    }

    writer = data_part_->getWriter(columns_list_, default_codec_, writer_settings);
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
    bool write_final_mark = (with_final_mark && rows_count != 0);
    writer->finalize(checksums, write_final_mark);
    writer->finishPrimaryIndexSerialization(checksums, write_final_mark);
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

        WriteBufferFromFile count_out(part_path + "count.txt", 4096);
        HashingWriteBuffer count_out_hashing(count_out);
        writeIntText(rows_count, count_out_hashing);
        count_out_hashing.next();
        checksums.files["count.txt"].file_size = count_out_hashing.count();
        checksums.files["count.txt"].file_hash = count_out_hashing.getHash();
    }

    if (new_part->ttl_infos.part_min_ttl)
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
    new_part->columns = *total_column_list;
    new_part->index.assign(std::make_move_iterator(index_columns.begin()), std::make_move_iterator(index_columns.end()));
    new_part->checksums = checksums;
    new_part->bytes_on_disk = checksums.getTotalSizeOnDisk();
    new_part->index_granularity = index_granularity;
}

void MergedBlockOutputStream::init()
{
}


void MergedBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    block.checkNumberOfRows();
    size_t rows = block.rows();
    if (!rows)
        return;

    /// Fill index granularity for this block
    /// if it's unknown (in case of insert data or horizontal merge,
    /// but not in case of vertical merge)
    if (compute_granularity)
        fillIndexGranularity(block);

    Block primary_key_block;
    Block skip_indexes_block;

    auto primary_key_column_names = storage.primary_key_columns;

    std::set<String> skip_indexes_column_names_set;
    for (const auto & index : storage.skip_indices)
        std::copy(index->columns.cbegin(), index->columns.cend(),
                std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    Names skip_indexes_column_names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());

    for (size_t i = 0, size = primary_key_column_names.size(); i < size; ++i)
    {
        const auto & name = primary_key_column_names[i];
        primary_key_block.insert(i, block.getByName(name));

        /// Reorder primary key columns in advance and add them to `primary_key_columns`.
        if (permutation)
        {
            auto & column = primary_key_block.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }

    for (size_t i = 0, size = skip_indexes_column_names.size(); i < size; ++i)
    {
        const auto & name = skip_indexes_column_names[i];
        skip_indexes_block.insert(i, block.getByName(name));

        /// Reorder index columns in advance.
        if (permutation)
        {
            auto & column = skip_indexes_block.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }

    writer->write(block, permutation, current_mark, index_offset, index_granularity, primary_key_block, skip_indexes_block);
    writer->calculateAndSerializeSkipIndices(skip_indexes_block, rows);
    writer->calculateAndSerializePrimaryIndex(primary_index_block);
    writer->next();

    rows_count += rows;

    // index_offset = new_index_offset;
}

}
