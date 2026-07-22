#include <Columns/ColumnsNumber.h>
#include <Common/SipHash.h>
#include <DataTypes/Serializations/SerializationArrayOffsets.h>

namespace DB
{


UInt128 SerializationArrayOffsets::getHash()
{
    SipHash hash;
    hash.update("ArrayOffsets");
    return hash.get128();
}

SerializationPtr SerializationArrayOffsets::create()
{
    return ISerialization::pooled(getHash(), [] { return new SerializationArrayOffsets(); });
}

void SerializationArrayOffsets::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr &,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);

    size_t prev_size = column.size();
    if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
    {
        /// Do nothing, data was inserted from cache.
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        /// Deserialize rows_offset + limit rows, we will apply rows_offset later.
        deserializeBinaryBulk(column, *stream, 0, rows_offset + limit, 0);

        if (cache)
        {
            size_t num_read_rows = column.size() - prev_size;
            /// rows_offset is applied in place below, so cache an unmodified cut() copy for other readers of this substream.
            if (rows_offset)
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.cut(prev_size, num_read_rows), num_read_rows);
            else
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column.getPtr(), num_read_rows);
        }
    }

    /// Apply rows_offset if needed.
    if (rows_offset)
    {
        auto & data = assert_cast<ColumnUInt64 &>(column).getData();
        size_t actual_new_size = column.size() - rows_offset;
        for (size_t i = prev_size; i != actual_new_size; ++i)
            data[i] = data[i + rows_offset];
        data.resize(actual_new_size);
    }

    settings.path.pop_back();
}

}
