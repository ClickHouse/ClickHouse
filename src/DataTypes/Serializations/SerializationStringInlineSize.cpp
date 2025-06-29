#include <DataTypes/Serializations/SerializationStringInlineSize.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnStringAndSize.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

void SerializationStringInlineSize::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    chassert(settings.path.back().type == Substream::InlinedStringSizes);
    settings.path.back().data = data;
    callback(settings.path);
}

void SerializationStringInlineSize::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationStringInlineSize");
}

void SerializationStringInlineSize::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    chassert(settings.path.back().type == Substream::InlinedStringSizes);

    /// cache_path {InlinedStringSizes(size), Regular}
    auto cache_path = settings.path;
    cache_path.push_back(Substream::Regular);
    auto cached_column = getFromSubstreamsCache(cache, cache_path);
    ColumnPtr column_string;
    ColumnPtr column_size;
    if (cached_column)
    {
        const auto & column_string_and_size = assert_cast<const ColumnStringAndSize &>(*cached_column);
        column_string = column_string_and_size.getDataPtr();
        column_size = column_string_and_size.getSizePtr();
    }

    settings.path.back() = Substream::Regular;
    if (column_size)
    {
        column = column_size;
    }
    else if (column_string)
    {
        column = assert_cast<const ColumnString &>(*column_string).createSizeSubcolumn();
        addToOrUpdateSubstreamsCache(cache, cache_path, ColumnStringAndSize::create(column_string, column, nullptr, nullptr));
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_string_column = ColumnString::create();
        serialization_string.deserializeBinaryBulk(*mutable_string_column, *stream, rows_offset, limit, settings.avg_value_size_hint);
        const auto & offsets = mutable_string_column->getOffsets();

        auto mutable_column = column->assumeMutable();
        auto & sizes_data = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
        size_t existing_size = sizes_data.size();
        size_t total_size = existing_size + offsets.size();
        sizes_data.resize(total_size);
        for (size_t i = existing_size, j = 0; i < total_size; ++i, ++j)
            sizes_data[i] = offsets[j] - offsets[j - 1] - 1;
        column = std::move(mutable_column);
        addToOrUpdateSubstreamsCache(
            cache, cache_path, ColumnStringAndSize::create(nullptr, column, std::move(mutable_string_column), nullptr));
    }

    settings.path.back() = Substream::InlinedStringSizes;
}

void SerializationStringSize::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    chassert(settings.path.back().type == Substream::StringSizes);
    settings.path.back().data = data;
    callback(settings.path);
}

void SerializationStringSize::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationStringSize");
}

void SerializationStringSize::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    chassert(settings.path.back().type == Substream::StringSizes);

    /// cache_path {StringSizes(size), Regular}
    auto cache_path = settings.path;
    cache_path.push_back(Substream::Regular);
    auto cached_column = getFromSubstreamsCache(cache, cache_path);
    ColumnPtr column_string;
    ColumnPtr column_size;
    ColumnPtr column_partial_size;
    if (cached_column)
    {
        const auto & column_string_and_size = assert_cast<const ColumnStringAndSize &>(*cached_column);
        column_string = column_string_and_size.getDataPtr();
        column_size = column_string_and_size.getSizePtr();
        column_partial_size = column_string_and_size.getPartialSizePtr();
    }

    if (column_size)
    {
        column = column_size;
    }
    else if (column_string)
    {
        chassert(column_partial_size);
        chassert(column_partial_size->size() <= limit);
        auto mutable_column = column->assumeMutable();
        assert_cast<ColumnUInt64 &>(*mutable_column).insertRangeFrom(*column_partial_size, 0, column_partial_size->size());
        column = std::move(mutable_column);
        addToOrUpdateSubstreamsCache(cache, cache_path, ColumnStringAndSize::create(column_string, column));
    }
    else
    {
        ReadBuffer * stream = settings.getter(settings.path);
        if (stream)
        {
            auto mutable_column = column->assumeMutable();
            size_t skipped_bytes = 0;
            if (rows_offset > 0)
            {
                PaddedPODArray<UInt64> ignored_sizes(rows_offset);
                const size_t real_read_size
                    = stream->readBig(reinterpret_cast<char *>(ignored_sizes.data()), sizeof(UInt64) * rows_offset) / sizeof(UInt64);

                for (size_t i = 0; i < real_read_size; ++i)
                {
                    if constexpr (std::endian::native == std::endian::big)
                        transformEndianness<std::endian::big, std::endian::little>(ignored_sizes[i]);
                    skipped_bytes += ignored_sizes[i] + 1;
                }
            }

            SerializationNumber<UInt64>().deserializeBinaryBulk(*mutable_column, *stream, 0, limit, settings.avg_value_size_hint);
            column = std::move(mutable_column);
            addToOrUpdateSubstreamsCache(cache, cache_path, ColumnStringAndSize::create(nullptr, column, nullptr, nullptr, skipped_bytes));
        }
    }
}

}
