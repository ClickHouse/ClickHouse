#include <Storages/MergeTree/MergeTreeReaderProjectionIndex.h>

#include <Storages/MergeTree/MergeTreeReadPoolProjectionIndex.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReaderProjectionIndex::MergeTreeReaderProjectionIndex(
    const IMergeTreeReader * main_reader_, ProjectionIndexBitmaps projection_index_bitmaps_)
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
    , projection_index_bitmaps(std::move(projection_index_bitmaps_))
{
    chassert(!projection_index_bitmaps.empty());
}

size_t MergeTreeReaderProjectionIndex::readRows(
    size_t from_mark,
    size_t /* current_task_last_mark */,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
    if (res_columns.size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Invalid number of columns passed to MergeTreeReaderProjectionIndex::readRows. "
            "Expected 1, got {}",
            res_columns.size());

    ColumnPtr & filter_column = res_columns.front();

    if (filter_column == nullptr)
    {
        filter_column = ColumnUInt8::create();
    }
    else if (!typeid_cast<const ColumnUInt8 *>(filter_column.get()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Illegal type {} of column for projection index filter. Must be UInt8", filter_column->getName());
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

    /// If there are rows to read, apply bitmap filtering.
    if (max_rows_to_read > 0)
    {
        auto mutable_filter_column = filter_column->assumeMutable();
        auto & filter_data = static_cast<ColumnUInt8 &>(*mutable_filter_column).getData();

        auto dispatch_by_bitmap_type = [&]<typename Offset>(Offset)
        {
            /// Create a range bitmap initialized to [starting_row, starting_row + max_rows_to_read)
            /// Intersect it with all projection index bitmaps to narrow down the match.
            /// Write the result of the bitmap filter to the filter_column (append 0/1 flags).
            ProjectionIndexBitmapPtr final_bitmap
                = ProjectionIndexBitmap::createFromRange<Offset>(starting_row, starting_row + max_rows_to_read);
            for (const auto & bitmap : projection_index_bitmaps)
                final_bitmap->intersectWith(*bitmap);
            final_bitmap->appendToFilter<Offset>(filter_data, starting_row, max_rows_to_read);
        };

        if (projection_index_bitmaps.front()->type == ProjectionIndexBitmap::BitmapType::Bitmap32)
            dispatch_by_bitmap_type(UInt32{});
        else
            dispatch_by_bitmap_type(UInt64{});

        filter_column = std::move(mutable_filter_column);
    }

    current_row += max_rows_to_read;
    return max_rows_to_read;
}

}
