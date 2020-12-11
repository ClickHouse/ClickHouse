#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{
Granules getGranulesToWrite(const MergeTreeIndexGranularity & index_granularity, size_t block_rows, size_t current_mark, size_t rows_written_in_last_mark)
{
    if (current_mark >= index_granularity.getMarksCount())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Request to get granules from mark {} but index granularity size is {}", current_mark, index_granularity.getMarksCount());

    Granules result;
    size_t current_row = 0;
    if (rows_written_in_last_mark > 0)
    {
        size_t rows_left_in_last_mark = index_granularity.getMarkRows(current_mark) - rows_written_in_last_mark;
        result.emplace_back(Granule{current_row, rows_left_in_last_mark, current_mark, false, true});
        current_row += rows_left_in_last_mark;
        current_mark++;
    }

    while (current_row < block_rows)
    {
        size_t expected_rows = index_granularity.getMarkRows(current_mark);
        size_t rest_rows = block_rows - current_row;
        if (rest_rows < expected_rows)
        {
            result.emplace_back(Granule{current_row, rest_rows, current_mark, true, false});
            current_row += rest_rows;
        }
        else
        {
            result.emplace_back(Granule{current_row, expected_rows, current_mark, true, true});
            current_row += expected_rows;
        }

        current_mark++;
    }

    return result;
}

Block getBlockAndPermute(const Block & block, const Names & names, const IColumn::Permutation * permutation)
{
    Block result;
    for (size_t i = 0, size = names.size(); i < size; ++i)
    {
        const auto & name = names[i];
        result.insert(i, block.getByName(name));

        /// Reorder primary key columns in advance and add them to `primary_key_columns`.
        if (permutation)
        {
            auto & column = result.getByPosition(i);
            column.column = column.column->permute(*permutation, 0);
        }
    }

    return result;
}

IMergeTreeDataPartWriter::IMergeTreeDataPartWriter(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : data_part(data_part_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , columns_list(columns_list_)
    , settings(settings_)
    , index_granularity(index_granularity_)
    , with_final_mark(storage.getSettings()->write_final_mark && settings.can_use_adaptive_granularity){}

Columns IMergeTreeDataPartWriter::releaseIndexColumns()
{
    return Columns(
        std::make_move_iterator(index_columns.begin()),
        std::make_move_iterator(index_columns.end()));
}

IMergeTreeDataPartWriter::~IMergeTreeDataPartWriter() = default;

}
