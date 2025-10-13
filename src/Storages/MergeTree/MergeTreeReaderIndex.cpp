#include <Storages/MergeTree/MergeTreeReaderIndex.h>

#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReaderIndex::MergeTreeReaderIndex(const IMergeTreeReader * main_reader_, MergeTreeIndexReadResultPtr index_read_result_)
    : IMergeTreeReader(
          main_reader_->data_part_info_for_read,
          {},
          {},
          main_reader_->storage_snapshot,
          nullptr,
          nullptr,
          main_reader_->all_mark_ranges,
          main_reader_->settings)
    , main_reader(main_reader_)
    , index_read_result(std::move(index_read_result_))
{
    chassert(index_read_result);
    chassert(index_read_result->skip_index_read_result);
}

size_t MergeTreeReaderIndex::readRows(
    size_t from_mark,
    size_t /* current_task_last_mark */,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
    if (!res_columns.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Invalid number of columns passed to MergeTreeReaderIndex::readRows. "
            "Expected 0, got {}",
            res_columns.size());
    }

    /// Determine the starting row.
    size_t starting_row;
    if (continue_reading)
        starting_row = current_row + rows_offset;
    else
        starting_row = data_part_info_for_read->getIndexGranularity().getMarkStartingRow(from_mark) + rows_offset;

    /// Clamp max_rows_to_read.
    size_t total_rows = data_part_info_for_read->getIndexGranularity().getTotalRows();
    if (starting_row < total_rows)
        max_rows_to_read = std::min(max_rows_to_read, total_rows - starting_row);

    /// TODO(ab): If projection index is available, attempt to construct the filter column.

    current_row += max_rows_to_read;
    return max_rows_to_read;
}

bool MergeTreeReaderIndex::canSkipMark(size_t mark, size_t /*current_task_last_mark*/)
{
    chassert(mark < index_read_result->skip_index_read_result->size());
    if (!index_read_result->skip_index_read_result->at(mark))
        return true;

    /// TODO(ab): If projection index is available, attempt to skip via projection bitmap.

    return false;
}

}
