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
    init();
    writer = data_part_->getWriter(columns_list_, default_codec_, writer_settings);
}

MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    CompressionCodecPtr default_codec_,
    const MergeTreeData::DataPart::ColumnToSize & /* merged_column_to_size_ */,
    size_t aio_threshold_,
    bool blocks_are_granules_size_)
    : IMergedBlockOutputStream(
        data_part_, default_codec_,
        {
            data_part_->storage.global_context.getSettings().min_compress_block_size,
            data_part_->storage.global_context.getSettings().max_compress_block_size,
            aio_threshold_
        },
        blocks_are_granules_size_,
        std::vector<MergeTreeIndexPtr>(std::begin(data_part_->storage.skip_indices), std::end(data_part_->storage.skip_indices)), {})
    , columns_list(columns_list_)
{
    init();
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

    /// Finish columns serialization.
    bool write_final_mark = (with_final_mark && rows_count != 0);
    writer->finalize(checksums, write_final_mark);

    if (write_final_mark)
        index_granularity.appendMark(0); /// last mark

    if (!total_column_list)
        total_column_list = &columns_list;

    if (additional_column_checksums)
        checksums = std::move(*additional_column_checksums);

    if (index_stream)
    {
        if (with_final_mark && rows_count != 0)
        {
            for (size_t j = 0; j < index_columns.size(); ++j)
            {
                auto & column = *last_index_row[j].column;
                index_columns[j]->insertFrom(column, 0); /// it has only one element
                last_index_row[j].type->serializeBinary(column, 0, *index_stream);
            }
            last_index_row.clear();
        }

        index_stream->next();
        checksums.files["primary.idx"].file_size = index_stream->count();
        checksums.files["primary.idx"].file_hash = index_stream->getHash();
        index_stream = nullptr;
    }

    finishSkipIndicesSerialization(checksums);

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
    Poco::File(part_path).createDirectories();

    if (storage.hasPrimaryKey())
    {
        index_file_stream = std::make_unique<WriteBufferFromFile>(
            part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY);
        index_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);
    }

    initSkipIndices();
}


void MergedBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    std::cerr << "(MergedBlockOutputStream::writeImpl) block.rows(): " << block.rows() << "\n";
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

    if (index_columns.empty())
    {
        index_columns.resize(primary_key_column_names.size());
        last_index_row.resize(primary_key_column_names.size());
        for (size_t i = 0, size = primary_key_column_names.size(); i < size; ++i)
        {
            last_index_row[i] = primary_key_block.getByPosition(i).cloneEmpty();
            index_columns[i] = last_index_row[i].column->cloneEmpty();
        }
    }

    size_t new_index_offset = writer->write(block, permutation, current_mark, index_offset, index_granularity, primary_key_block, skip_indexes_block).second;
    rows_count += rows;

    /// Should be written before index offset update, because we calculate,
    /// indices of currently written granules
    calculateAndSerializeSkipIndices(skip_indexes_block, rows);

    {
        /** While filling index (index_columns), disable memory tracker.
          * Because memory is allocated here (maybe in context of INSERT query),
          *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
          * And otherwise it will look like excessively growing memory consumption in context of query.
          *  (observed in long INSERT SELECTs)
          */
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (size_t i = index_offset; i < rows;)
        {
            if (storage.hasPrimaryKey())
            {
                for (size_t j = 0, size = primary_key_block.columns(); j < size; ++j)
                {
                    const auto & primary_column = primary_key_block.getByPosition(j);
                    std::cerr << "(writeImpl) primary_column: " << !!primary_column.column << "\n";
                    std::cerr << "(writeImpl) index_column: " << !!index_columns[j] << "\n";
                    index_columns[j]->insertFrom(*primary_column.column, i);
                    primary_column.type->serializeBinary(*primary_column.column, i, *index_stream);
                }
            }

            ++current_mark;
            if (current_mark < index_granularity.getMarksCount())
                i += index_granularity.getMarkRows(current_mark);
            else
                break;
        }
    }

    /// store last index row to write final mark at the end of column
    for (size_t j = 0, size = primary_key_block.columns(); j < size; ++j)
    {
        const IColumn & primary_column = *primary_key_block.getByPosition(j).column.get();
        auto mutable_column = std::move(*last_index_row[j].column).mutate();
        if (!mutable_column->empty())
            mutable_column->popBack(1);
        mutable_column->insertFrom(primary_column, rows - 1);
        last_index_row[j].column = std::move(mutable_column);
    }

    index_offset = new_index_offset;
}

}
