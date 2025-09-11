#include <DataTypes/Serializations/SerializationStringWithSizeStream.h>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationStringInlineSize.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void SerializationStringWithSizeStream::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeBinary(field, ostr, settings);
}

void SerializationStringWithSizeStream::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    serialization_string.deserializeBinary(field, istr, settings);
}

void SerializationStringWithSizeStream::serializeBinary(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeBinary(column, row_num, ostr, settings);
}

void SerializationStringWithSizeStream::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    serialization_string.deserializeBinary(column, istr, settings);
}

void SerializationStringWithSizeStream::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    serialization_string.serializeBinaryBulk(column, ostr, offset, limit);
}

void SerializationStringWithSizeStream::deserializeBinaryBulk(
    IColumn & column, ReadBuffer & istr, size_t rows_offset, size_t limit, double avg_value_size_hint) const
{
    serialization_string.deserializeBinaryBulk(column, istr, rows_offset, limit, avg_value_size_hint);
}

namespace
{

void serializeStringSizes(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    const auto & column_string = typeid_cast<const ColumnString &>(column);
    const auto & offset_values = column_string.getOffsets();
    size_t size = offset_values.size();

    if (!size)
        return;

    size_t end = limit && (offset + limit < size) ? offset + limit : size;
    UInt64 prev_offset = offset_values[offset - 1];
    if constexpr (std::endian::native == std::endian::big)
    {
        for (size_t i = offset; i < end; ++i)
        {
            UInt64 current_offset = offset_values[i];
            writeBinaryLittleEndian(current_offset - prev_offset, ostr);
            prev_offset = current_offset;
        }
    }
    else
    {
        size_t count = end - offset;
        PaddedPODArray<UInt64> sizes;
        sizes.resize(count);

        for (size_t i = 0; i < count; ++i)
        {
            UInt64 current_offset = offset_values[offset + i];
            sizes[i] = current_offset - prev_offset;
            prev_offset = current_offset;
        }

        ostr.write(reinterpret_cast<const char *>(sizes.data()), sizeof(UInt64) * count);
    }
}

void appendStringSizesToColumnStringOffsets(ColumnString & column_string, const UInt64 * sizes, size_t start, size_t rows)
{
    auto & offsets = column_string.getOffsets();
    IColumn::Offset prev_offset = offsets.empty() ? 0 : offsets.back();

    offsets.reserve(offsets.size() + rows);

    for (size_t i = 0; i < rows; ++i)
    {
        prev_offset += sizes[start + i];
        offsets.push_back(prev_offset);
    }
}

}

void SerializationStringWithSizeStream::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    const auto * type_string = data.type ? &assert_cast<const DataTypeString &>(*data.type) : nullptr;
    const auto * column_string = data.column ? &assert_cast<const ColumnString &>(*data.column) : nullptr;
    ColumnPtr sizes_column;
    if (column_string)
    {
        /// TODO(ab): Will this unnecessarily create useless .size subcolumn?
        sizes_column = column_string->createSizeSubcolumn();
    }

    auto sizes_serialization
        = std::make_shared<SerializationNamed>(std::make_shared<SerializationStringSize>(), "size", SubstreamType::StringSizes);

    /// Size stream
    settings.path.push_back(Substream::StringSizes);
    settings.path.back().data = SubstreamData(sizes_serialization)
                                    .withType(type_string ? std::make_shared<DataTypeUInt64>() : nullptr)
                                    .withColumn(std::move(sizes_column))
                                    .withSerializationInfo(data.serialization_info);
    callback(settings.path);

    /// Regular stream
    settings.path.back() = Substream::Regular;
    settings.path.back().data = data;
    callback(settings.path);
    settings.path.pop_back();
}

void SerializationStringWithSizeStream::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & /* state */) const
{
    if (const size_t size = column.size(); limit == 0 || offset + limit > size)
        limit = size - offset;

    settings.path.push_back(Substream::StringSizes);
    settings.path.back().name_of_substream = "size";
    auto * size_stream = settings.getter(settings.path);
    if (!size_stream)
    {
        settings.path.pop_back();
        return;
    }

    const auto & column_string = typeid_cast<const ColumnString &>(column);
    serializeStringSizes(column, *size_stream, offset, limit);

    settings.path.back() = Substream::Regular;
    settings.path.back().name_of_substream = "";
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "String stream is missing when try to serialize string with separate size stream");

    /// Serialize string data
    const auto & offsets = column_string.getOffsets();
    size_t begin = (offset == 0) ? 0 : offsets[offset - 1];
    size_t end = offsets[offset + limit - 1];
    size_t bytes = end - begin;
    stream->write(reinterpret_cast<const char *>(&column_string.getChars()[begin]), bytes);
    settings.path.pop_back();
}

void SerializationStringWithSizeStream::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & /* state */,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::StringSizes);
    settings.path.back().name_of_substream = "size";

    /// cache_path {StringSizes(size), Regular}
    auto cache_path = settings.path;
    cache_path.push_back(Substream::Regular);

    const auto * cached_element = getElementFromSubstreamsCache(cache, cache_path);
    ColumnPtr column_string;
    ColumnPtr column_size;
    ColumnPtr column_partial_size;
    size_t skipped_bytes = 0;
    if (cached_element)
    {
        const auto & string_element = assert_cast<const SubstreamsCacheStringElement &>(*cached_element);
        column_string = string_element.data;
        column_size = string_element.size;
        skipped_bytes = string_element.skipped_bytes;
    }

    if (column_string)
    {
        column = column_string;
        settings.path.pop_back();
        return;
    }

    ReadBuffer * stream = nullptr;
    size_t bytes_to_read = 0;

    auto mutable_column = column->assumeMutable();
    auto & mutable_string_column = assert_cast<ColumnString &>(*mutable_column);
    auto & offsets = mutable_string_column.getOffsets();
    size_t prev_last_offset = offsets.back();

    if (column_size)
    {
        chassert(column_size->size() >= mutable_string_column.size());
        appendStringSizesToColumnStringOffsets(
            mutable_string_column,
            assert_cast<const ColumnUInt64 &>(*column_size).getData().data(),
            mutable_string_column.size(),
            column_size->size() - mutable_string_column.size());

        settings.path.back() = Substream::Regular;
        settings.path.back().name_of_substream = "";
        stream = settings.getter(settings.path);
        bytes_to_read = offsets.back() - prev_last_offset;
    }
    else if (ReadBuffer * size_stream = settings.getter(settings.path))
    {
        chassert(skipped_bytes == 0);
        auto mutable_size_column = ColumnUInt64::create();
        if (rows_offset > 0)
        {
            PaddedPODArray<UInt64> ignored_sizes(rows_offset);
            const size_t real_read_size
                = size_stream->readBig(reinterpret_cast<char *>(ignored_sizes.data()), sizeof(UInt64) * rows_offset) / sizeof(UInt64);

            for (size_t i = 0; i < real_read_size; ++i)
            {
                if constexpr (std::endian::native == std::endian::big)
                    transformEndianness<std::endian::big, std::endian::little>(ignored_sizes[i]);
                skipped_bytes += ignored_sizes[i];
            }
        }

        SerializationNumber<UInt64>().deserializeBinaryBulk(*mutable_size_column, *size_stream, 0, limit, 0);
        appendStringSizesToColumnStringOffsets(
            mutable_string_column, mutable_size_column->getData().data(), 0, mutable_size_column->size());
        column_partial_size = std::move(mutable_size_column);
        settings.path.back() = Substream::Regular;
        settings.path.back().name_of_substream = "";
        stream = settings.getter(settings.path);
        bytes_to_read = offsets.back() - prev_last_offset;
    }

    if (stream)
    {
        auto & data = mutable_string_column.getChars();
        const size_t initial_size = data.size();
        data.resize(initial_size + bytes_to_read);
        stream->ignore(skipped_bytes);
        const size_t size = stream->readBig(reinterpret_cast<char*>(&data[initial_size]), bytes_to_read);
        data.resize(initial_size + size);
        column = std::move(mutable_column);
        addElementToSubstreamsCache(
            cache, cache_path, std::make_unique<SubstreamsCacheStringElement>(column, column_size, nullptr, column_partial_size), true);
    }

    settings.path.pop_back();
}

void SerializationStringWithSizeStream::serializeText(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeText(column, row_num, ostr, settings);
}

void SerializationStringWithSizeStream::serializeTextEscaped(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeTextEscaped(column, row_num, ostr, settings);
}

void SerializationStringWithSizeStream::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    serialization_string.deserializeWholeText(column, istr, settings);
}

bool SerializationStringWithSizeStream::tryDeserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return serialization_string.tryDeserializeWholeText(column, istr, settings);
}

void SerializationStringWithSizeStream::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    serialization_string.deserializeTextEscaped(column, istr, settings);
}

bool SerializationStringWithSizeStream::tryDeserializeTextEscaped(
    IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return serialization_string.tryDeserializeTextEscaped(column, istr, settings);
}

void SerializationStringWithSizeStream::serializeTextQuoted(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeTextQuoted(column, row_num, ostr, settings);
}

void SerializationStringWithSizeStream::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    serialization_string.deserializeTextQuoted(column, istr, settings);
}

bool SerializationStringWithSizeStream::tryDeserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return serialization_string.tryDeserializeTextQuoted(column, istr, settings);
}

void SerializationStringWithSizeStream::serializeTextJSON(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeTextJSON(column, row_num, ostr, settings);
}

void SerializationStringWithSizeStream::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    serialization_string.deserializeTextJSON(column, istr, settings);
}

bool SerializationStringWithSizeStream::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return serialization_string.tryDeserializeTextJSON(column, istr, settings);
}

void SerializationStringWithSizeStream::serializeTextXML(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeTextXML(column, row_num, ostr, settings);
}

void SerializationStringWithSizeStream::serializeTextCSV(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeTextCSV(column, row_num, ostr, settings);
}

void SerializationStringWithSizeStream::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    serialization_string.deserializeTextCSV(column, istr, settings);
}

bool SerializationStringWithSizeStream::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return serialization_string.tryDeserializeTextCSV(column, istr, settings);
}

void SerializationStringWithSizeStream::serializeTextMarkdown(
    const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serialization_string.serializeTextMarkdown(column, row_num, ostr, settings);
}

}
