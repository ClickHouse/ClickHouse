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

    auto cached_column = getFromSubstreamsCache(cache, settings.path);
    if (cached_column)
    {
        /// Cached column contains data without applied rows_offset and can be used in other serializations (for example in SerializationArray)
        /// so if rows_offset is not 0 we cannot use it as is because we will modify it here later by applying rows_offset.
        /// Instead we need to insert data from the current range from it.
        if (rows_offset)
            column->assumeMutable()->insertRangeFrom(*cached_column, cached_column->size() - limit, limit);
        else
            column = cached_column;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_column = column->assumeMutable();
        /// Deserialize rows_offset + limit rows, we will apply rows_offset later.
        deserializeBinaryBulk(*mutable_column, *stream, 0, rows_offset + limit, settings.avg_value_size_hint);

        if (rows_offset)
        {
            /// If rows_offset != 0 we should keep data without applied offsets in the cache to be able
            /// to calculate offset for nested data in SerializationArray if the whole array is also read.
            /// As we will apply offsets to the current column we cannot put in the cache, so we have to copy
            /// the column to the cache.
            addToSubstreamsCache(cache, settings.path, IColumn::mutate(column));
            auto & data = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
            size_t prev_size = mutable_column->size() - limit - rows_offset;
            size_t actual_new_size = mutable_column->size() - rows_offset;
            for (size_t i = prev_size; i != actual_new_size; ++i)
                data[i] = data[i + rows_offset];
            data.resize(actual_new_size);
        }
        else
        {
            addToSubstreamsCache(cache, settings.path, column);
        }
    }

    settings.path.pop_back();
}

}
