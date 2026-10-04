#include <Storages/MergeTree/MergeTreeReaderIndex.h>

#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

namespace ProfileEvents
{
extern const Event UniqueKeyBitmapRowsSkipped;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool MergeTreeReaderIndex::canSkipAnyMark() const
{
    return index_read_result && index_read_result->canSkipAnyMark();
}

MergeTreeReaderIndex::MergeTreeReaderIndex(
    const IMergeTreeReader * main_reader_,
    MergeTreeIndexReadResultPtr index_read_result_,
    const PaddedPODArray<UInt64> * lazy_materializing_rows_,
    ConstDeleteBitmapPtr delete_bitmap_)
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
    , index_read_result(std::move(index_read_result_))
    , lazy_materializing_rows(lazy_materializing_rows_)
    , delete_bitmap(std::move(delete_bitmap_))
    , main_reader(main_reader_)
{
    chassert(lazy_materializing_rows || delete_bitmap || index_read_result);
    chassert(lazy_materializing_rows || delete_bitmap || index_read_result->skip_index_read_result || index_read_result->projection_index_read_result);
}

size_t MergeTreeReaderIndex::readRows(
    size_t from_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    MutableColumns & res_columns)
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
    if (!continue_reading)
        current_row = data_part_info_for_read->getIndexGranularity().getMarkStartingRow(from_mark);

    size_t starting_row = current_row;

    if (!continue_reading && lazy_materializing_rows)
        next_lazy_row_it = std::lower_bound(lazy_materializing_rows->begin(), lazy_materializing_rows->end(), starting_row);

    /// Clamp max_rows_to_read to the actual number of remaining rows in the part.
    /// We use getRowCount() (from part metadata) rather than getTotalRows() (from index granularity)
    /// because for constant granularity parts with non-adaptive marks, getTotalRows() can overestimate
    /// the last granule size (the mark file does not store per-granule row counts, so the last mark
    /// is assumed to have full granularity).
    size_t total_rows = data_part_info_for_read->getRowCount();
    chassert(starting_row <= total_rows);
    if (starting_row < total_rows)
        max_rows_to_read = std::min(max_rows_to_read, total_rows - starting_row);
    else
        max_rows_to_read = 0;

    /// The filter's length before this call: each filter below appends this call's rows after it.
    const size_t filter_rows_before = res_columns.front() ? res_columns.front()->size() : 0;

    /// If projection index is available, attempt to construct the filter column
    if (index_read_result && index_read_result->projection_index_read_result)
    {
        MutableColumnPtr & filter_column = res_columns.front();

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
            auto & filter_data = static_cast<ColumnUInt8 &>(*filter_column).getData();
            index_read_result->projection_index_read_result->appendToFilter(filter_data, starting_row, max_rows_to_read);
        }
    }

    if (lazy_materializing_rows)
    {
        MutableColumnPtr & filter_column = res_columns.front();

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
            auto & filter_data = static_cast<ColumnUInt8 &>(*filter_column).getData();
            size_t old_size = filter_data.size();
            filter_data.resize(old_size + max_rows_to_read);
            memset(filter_data.begin() + old_size, 0, max_rows_to_read);

            if (next_lazy_row_it != lazy_materializing_rows->end())
            {
                if (*next_lazy_row_it < starting_row)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Next lazy materializing row {} is less than starting row {}", *next_lazy_row_it, starting_row);
            }

            while (next_lazy_row_it != lazy_materializing_rows->end() && *next_lazy_row_it < starting_row + max_rows_to_read)
            {
                filter_data[old_size + *next_lazy_row_it - starting_row] = 1;
                ++next_lazy_row_it;
            }
        }
    }

    if (delete_bitmap && max_rows_to_read > 0)
        appendDeleteBitmapFilter(res_columns.front(), filter_rows_before, starting_row, max_rows_to_read);

    current_row += max_rows_to_read;
    return max_rows_to_read;
}

void MergeTreeReaderIndex::appendDeleteBitmapFilter(
    MutableColumnPtr & filter_column, size_t filter_rows_before, size_t starting_row, size_t num_rows) const
{
    if (filter_column == nullptr)
        filter_column = ColumnUInt8::create();

    auto & filter_data = assert_cast<ColumnUInt8 &>(*filter_column).getData();
    chassert(filter_data.size() == filter_rows_before || filter_data.size() == filter_rows_before + num_rows,
        fmt::format("the index filter has {} row(s) for {} read, {} before this read", filter_data.size(), num_rows, filter_rows_before));

    filter_data.resize_fill(filter_rows_before + num_rows, 1);
    const size_t dead = delete_bitmap->clearInFilter(starting_row, num_rows, filter_data.data() + filter_rows_before);
    ProfileEvents::increment(ProfileEvents::UniqueKeyBitmapRowsSkipped, dead);
}

bool MergeTreeReaderIndex::canSkipMark(size_t mark)
{
    return index_read_result && index_read_result->canSkipMark(mark, data_part_info_for_read->getIndexGranularity());
}

}
