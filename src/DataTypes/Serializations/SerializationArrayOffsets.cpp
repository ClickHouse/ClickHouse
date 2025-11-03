#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationArrayOffsets.h>

namespace DB
{

void SerializationArrayOffsets::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr &,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);

    size_t num_read_rows = 0;
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        auto cached_column = cached_column_with_num_read_rows->first;
        num_read_rows = cached_column_with_num_read_rows->second;

        /// Cached column contains data without applied rows_offset and can be used in other serializations (for example in SerializationArray)
        /// so if rows_offset is not 0 we cannot use it as is because we will modify it here later by applying rows_offset.
        /// Instead we need to insert data from the current range from it.
        if (rows_offset)
            column->assumeMutable()->insertRangeFrom(*cached_column, cached_column->size() - num_read_rows, num_read_rows);
        else
            insertDataFromCachedColumn(settings, column, cached_column, num_read_rows);
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_column = column->assumeMutable();
        size_t prev_size = mutable_column->size();
        /// Deserialize rows_offset + limit rows, we will apply rows_offset later.
        deserializeBinaryBulk(*mutable_column, *stream, 0, rows_offset + limit, 0);
        num_read_rows = mutable_column->size() - prev_size;

        if (cache)
        {
            ColumnPtr column_for_cache;
            /// If rows_offset != 0 we should keep data without applied offsets in the cache to be able
            /// to calculate offset for nested data in SerializationArray if the whole array is also read.
            /// As we will apply offsets to the current column we cannot put in the cache, so we use cut()
            /// method to create a separate column with all the data from current range.
            if (rows_offset)
                column_for_cache = mutable_column->cut(prev_size, num_read_rows);
            else
                column_for_cache = column;

            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column_for_cache, num_read_rows);
        }
    }

    /// Apply rows_offset if needed.
    if (rows_offset)
    {
        auto mutable_column = column->assumeMutable();
        auto & data = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
        size_t prev_size = mutable_column->size() - num_read_rows;
        size_t actual_new_size = mutable_column->size() - rows_offset;
        for (size_t i = prev_size; i != actual_new_size; ++i)
            data[i] = data[i + rows_offset];
        data.resize(actual_new_size);
    }

    settings.path.pop_back();
}

}
