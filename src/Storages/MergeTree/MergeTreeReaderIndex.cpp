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
          main_reader_->storage_settings,
          nullptr,
          nullptr,
          main_reader_->all_mark_ranges,
          main_reader_->settings)
    , main_reader(main_reader_)
    , index_read_result(std::move(index_read_result_))
{
    chassert(index_read_result);
    chassert(index_read_result->skip_index_read_result || index_read_result->projection_index_read_result);
}

size_t MergeTreeReaderIndex::readRows(
    size_t from_mark,
    size_t /* current_task_last_mark */,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
    if (res_columns.size() != 1)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Invalid number of columns passed to MergeTreeReaderIndex::readRows. "
            "Expected 1, got {}",
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

    /// If projection index is available, attempt to construct the filter column
    if (index_read_result->projection_index_read_result)
    {
        ColumnPtr & filter_column = res_columns.front();

        if (filter_column == nullptr)
        {
            filter_column = ColumnUInt8::create();
        }
        else if (!typeid_cast<const ColumnUInt8 *>(filter_column.get()))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Illegal type {} of column for projection index filter. Must be UInt8",
                filter_column->getName());
        }

        /// If there are rows to read, apply bitmap filtering.
        if (max_rows_to_read > 0)
        {
            auto mutable_filter_column = filter_column->assumeMutable();
            auto & filter_data = static_cast<ColumnUInt8 &>(*mutable_filter_column).getData();
            index_read_result->projection_index_read_result->appendToFilter(filter_data, starting_row, max_rows_to_read);
            filter_column = std::move(mutable_filter_column);
        }
    }

    current_row += max_rows_to_read;
    return max_rows_to_read;
}

bool MergeTreeReaderIndex::canSkipMark(size_t mark, size_t /*current_task_last_mark*/)
{
    if (index_read_result->skip_index_read_result)
    {
        chassert(mark < index_read_result->skip_index_read_result->size());
        if (!index_read_result->skip_index_read_result->at(mark))
            return true;
    }

    if (index_read_result->projection_index_read_result)
    {
        size_t begin = data_part_info_for_read->getIndexGranularity().getMarkStartingRow(mark);
        size_t end = begin + data_part_info_for_read->getIndexGranularity().getMarkRows(mark);
        if (index_read_result->projection_index_read_result->rangeAllZero(begin, end))
            return true;
    }

    return false;
}

}
