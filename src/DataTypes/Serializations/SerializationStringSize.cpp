#include <DataTypes/Serializations/SerializationStringSize.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationStringSize::SerializationStringSize(bool with_size_stream_)
    : with_size_stream(with_size_stream_)
    , serialization_string()
{
}

void SerializationStringSize::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (with_size_stream)
        deserializeBinaryBulkWithSizeStream(column, rows_offset, limit, settings, state, cache);
    else
        deserializeBinaryBulkWithoutSizeStream(column, rows_offset, limit, settings, state, cache);
}

struct DeserializeBinaryBulkStateStringSizeWithoutSizeStream : public ISerialization::DeserializeBinaryBulkState
{
    ColumnPtr string_column;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        return std::make_shared<DeserializeBinaryBulkStateStringSizeWithoutSizeStream>();
    }
};

void SerializationStringSize::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache *) const
{
    if (!with_size_stream)
        state = std::make_shared<DeserializeBinaryBulkStateStringSizeWithoutSizeStream>();
}

void SerializationStringSize::deserializeBinaryBulkWithoutSizeStream(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "WTF");

    settings.path.push_back(Substream::Regular);
    DeserializeBinaryBulkStateStringSizeWithoutSizeStream * size_state = nullptr;
    size_t num_read_rows = 0;
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        size_state = checkAndGetState<DeserializeBinaryBulkStateStringSizeWithoutSizeStream>(state);
        std::tie(size_state->string_column, num_read_rows) = *cached_column_with_num_read_rows;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        size_state = checkAndGetState<DeserializeBinaryBulkStateStringSizeWithoutSizeStream>(state);
        /// If we started to read a new column, reinitialize string column in the state.
        if (!size_state->string_column || column->empty())
            size_state->string_column = ColumnString::create();

        size_t prev_size = size_state->string_column->size();
        serialization_string.deserializeBinaryBulk(*size_state->string_column->assumeMutable(), *stream, rows_offset, limit, settings.avg_value_size_hint);
        num_read_rows = size_state->string_column->size() - prev_size;
        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, size_state->string_column, num_read_rows);
    }
    else
    {
        settings.path.pop_back();
        return;
    }

    settings.path.pop_back();

    auto mutable_column = column->assumeMutable();
    auto & sizes_data = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
    sizes_data.reserve(sizes_data.size() + num_read_rows);
    const auto & offsets = assert_cast<const ColumnString &>(*size_state->string_column).getOffsets();
    size_t prev_size = offsets.size() - num_read_rows;
    for (size_t i = prev_size; i != offsets.size(); ++i)
        sizes_data.push_back(offsets[i] - offsets[i - 1]);
}

void SerializationStringSize::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    if (with_size_stream)
    {
        settings.path.push_back(Substream::StringSizes);
        settings.path.back().data = data;
        callback(settings.path);
        settings.path.pop_back();
    }
    else
    {
        settings.path.push_back(Substream::Regular);
        settings.path.back().data = data;
        callback(settings.path);
        settings.path.pop_back();
    }
}

void SerializationStringSize::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationStringSize");
}

void SerializationStringSize::deserializeBinaryBulkWithSizeStream(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::StringSizes);
    settings.path.back().name_of_substream = "size";

    size_t num_read_rows = 0;
    if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
    {
        auto cached_column = cached_column_with_num_read_rows->first;
        num_read_rows = cached_column_with_num_read_rows->second;

        /// Cached column contains data without applied rows_offset and can be used in other serializations (for example in SerializationString)
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
        SerializationNumber<UInt64>().deserializeBinaryBulk(*mutable_column, *stream, 0, rows_offset + limit, settings.avg_value_size_hint);
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

void SerializationStringSize::serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for string size subcolumn");
}

void SerializationStringSize::deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for string size subcolumn");
}

void SerializationStringSize::serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for string size subcolumn");
}

void SerializationStringSize::deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for string size subcolumn");
}

void SerializationStringSize::serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for string size subcolumn");
}

void SerializationStringSize::deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for string size subcolumn");
}

bool SerializationStringSize::tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for string size subcolumn");
}

}
