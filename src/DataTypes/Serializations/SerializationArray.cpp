#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <Columns/ColumnArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int LOGICAL_ERROR;
}

void SerializationArray::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const Array & a = get<const Array &>(field);
    writeVarUInt(a.size(), ostr);
    for (size_t i = 0; i < a.size(); ++i)
    {
        nested->serializeBinary(a[i], ostr);
    }
}


void SerializationArray::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    size_t size;
    readVarUInt(size, istr);
    field = Array(size);
    Array & arr = get<Array &>(field);
    for (size_t i = 0; i < size; ++i)
        nested->deserializeBinary(arr[i], istr);
}


void SerializationArray::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];
    size_t size = next_offset - offset;

    writeVarUInt(size, ostr);

    const IColumn & nested_column = column_array.getData();
    for (size_t i = offset; i < next_offset; ++i)
        nested->serializeBinary(nested_column, i, ostr);
}


void SerializationArray::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
    ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t size;
    readVarUInt(size, istr);

    IColumn & nested_column = column_array.getData();

    size_t i = 0;
    try
    {
        for (; i < size; ++i)
            nested->deserializeBinary(nested_column, istr);
    }
    catch (...)
    {
        if (i)
            nested_column.popBack(i);
        throw;
    }

    offsets.push_back(offsets.back() + size);
}


namespace
{
    void serializeArraySizesPositionIndependent(const IColumn & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
    {
        const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);
        const ColumnArray::Offsets & offset_values = column_array.getOffsets();
        size_t size = offset_values.size();

        if (!size)
            return;

        size_t end = limit && (offset + limit < size)
            ? offset + limit
            : size;

        ColumnArray::Offset prev_offset = offset_values[offset - 1];
        for (size_t i = offset; i < end; ++i)
        {
            ColumnArray::Offset current_offset = offset_values[i];
            writeIntBinary(current_offset - prev_offset, ostr);
            prev_offset = current_offset;
        }
    }

    void deserializeArraySizesPositionIndependent(ColumnArray & column_array, ReadBuffer & istr, UInt64 limit)
    {
        ColumnArray::Offsets & offset_values = column_array.getOffsets();
        size_t initial_size = offset_values.size();
        offset_values.resize(initial_size + limit);

        size_t i = initial_size;
        ColumnArray::Offset current_offset = initial_size ? offset_values[initial_size - 1] : 0;
        while (i < initial_size + limit && !istr.eof())
        {
            ColumnArray::Offset current_size = 0;
            readIntBinary(current_size, istr);
            current_offset += current_size;
            offset_values[i] = current_offset;
            ++i;
        }

        offset_values.resize(i);
    }

    ColumnPtr arraySizesToOffsets(const IColumn & column)
    {
        const auto & column_sizes = assert_cast<const ColumnArray::ColumnOffsets &>(column);
        MutableColumnPtr column_offsets = column_sizes.cloneEmpty();

        if (column_sizes.empty())
            return column_offsets;

        const auto & sizes_data = column_sizes.getData();
        auto & offsets_data = assert_cast<ColumnArray::ColumnOffsets &>(*column_offsets).getData();

        offsets_data.resize(sizes_data.size());

        IColumn::Offset prev_offset = 0;
        for (size_t i = 0, size = sizes_data.size(); i < size; ++i)
        {
            prev_offset += sizes_data[i];
            offsets_data[i] = prev_offset;
        }

        return column_offsets;
    }
}

ColumnPtr arrayOffsetsToSizes(const IColumn & column)
{
    const auto & column_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(column);
    MutableColumnPtr column_sizes = column_offsets.cloneEmpty();

    if (column_offsets.empty())
        return column_sizes;

    const auto & offsets_data = column_offsets.getData();
    auto & sizes_data = assert_cast<ColumnArray::ColumnOffsets &>(*column_sizes).getData();

    sizes_data.resize(offsets_data.size());

    IColumn::Offset prev_offset = 0;
    for (size_t i = 0, size = offsets_data.size(); i < size; ++i)
    {
        auto current_offset = offsets_data[i];
        sizes_data[i] = current_offset - prev_offset;
        prev_offset =  current_offset;
    }

    return column_sizes;
}


void SerializationArray::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::ArraySizes);
    callback(path);
    path.back() = Substream::ArrayElements;
    nested->enumerateStreams(callback, path);
    path.pop_back();
}


void SerializationArray::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void SerializationArray::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}


void SerializationArray::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void SerializationArray::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);

    /// First serialize array sizes.
    settings.path.push_back(Substream::ArraySizes);
    if (auto * stream = settings.getter(settings.path))
    {
        if (settings.position_independent_encoding)
            serializeArraySizesPositionIndependent(column, *stream, offset, limit);
        else
            SerializationNumber<ColumnArray::Offset>().serializeBinaryBulk(*column_array.getOffsetsPtr(), *stream, offset, limit);
    }

    /// Then serialize contents of arrays.
    settings.path.back() = Substream::ArrayElements;
    const ColumnArray::Offsets & offset_values = column_array.getOffsets();

    if (offset > offset_values.size())
        return;

    /** offset - from which array to write.
      * limit - how many arrays should be written, or 0, if you write everything that is.
      * end - up to which array the recorded piece ends.
      *
      * nested_offset - from which element of the innards to write.
      * nested_limit - how many elements of the innards to write, or 0, if you write everything that is.
      */

    size_t end = std::min(offset + limit, offset_values.size());

    size_t nested_offset = offset ? offset_values[offset - 1] : 0;
    size_t nested_limit = limit
        ? offset_values[end - 1] - nested_offset
        : 0;

    if (limit == 0 || nested_limit)
        nested->serializeBinaryBulkWithMultipleStreams(column_array.getData(), nested_offset, nested_limit, settings, state);
    settings.path.pop_back();
}


void SerializationArray::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto mutable_column = column->assumeMutable();
    ColumnArray & column_array = typeid_cast<ColumnArray &>(*mutable_column);
    settings.path.push_back(Substream::ArraySizes);

    if (auto cached_column = getFromSubstreamsCache(cache, settings.path))
    {
        column_array.getOffsetsPtr() = arraySizesToOffsets(*cached_column);
    }
    else if (auto * stream = settings.getter(settings.path))
    {
        if (settings.position_independent_encoding)
            deserializeArraySizesPositionIndependent(column_array, *stream, limit);
        else
            SerializationNumber<ColumnArray::Offset>().deserializeBinaryBulk(column_array.getOffsetsColumn(), *stream, limit, 0);

        addToSubstreamsCache(cache, settings.path, arrayOffsetsToSizes(column_array.getOffsetsColumn()));
    }

    settings.path.back() = Substream::ArrayElements;

    ColumnArray::Offsets & offset_values = column_array.getOffsets();
    ColumnPtr & nested_column = column_array.getDataPtr();

    /// Number of values corresponding with `offset_values` must be read.
    size_t last_offset = offset_values.back();
    if (last_offset < nested_column->size())
        throw Exception("Nested column is longer than last offset", ErrorCodes::LOGICAL_ERROR);
    size_t nested_limit = last_offset - nested_column->size();

    /// Adjust value size hint. Divide it to the average array size.
    settings.avg_value_size_hint = nested_limit ? settings.avg_value_size_hint / nested_limit * offset_values.size() : 0;

    nested->deserializeBinaryBulkWithMultipleStreams(nested_column, nested_limit, settings, state, cache);

    settings.path.pop_back();

    /// Check consistency between offsets and elements subcolumns.
    /// But if elements column is empty - it's ok for columns of Nested types that was added by ALTER.
    if (!nested_column->empty() && nested_column->size() != last_offset)
        throw ParsingException("Cannot read all array values: read just " + toString(nested_column->size()) + " of " + toString(last_offset),
            ErrorCodes::CANNOT_READ_ALL_DATA);

    column = std::move(mutable_column);
}


template <typename Writer>
static void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, Writer && write_nested)
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    writeChar('[', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);
        write_nested(nested_column, i);
    }
    writeChar(']', ostr);
}


template <typename Reader>
static void deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && read_nested, bool allow_unenclosed)
{
    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
    ColumnArray::Offsets & offsets = column_array.getOffsets();

    IColumn & nested_column = column_array.getData();

    size_t size = 0;

    bool has_braces = false;
    if (checkChar('[', istr))
        has_braces = true;
    else if (!allow_unenclosed)
        throw Exception(ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT, "Array does not start with '[' character");

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != ']')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                    ++istr.position();
                else
                    throw ParsingException(ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT,
                        "Cannot read array from text, expected comma or end of array, found '{}'",
                        *istr.position());
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == ']')
                break;

            read_nested(nested_column);
            ++size;

            skipWhitespaceIfAny(istr);
        }

        if (has_braces)
            assertChar(']', istr);
        else /// If array is not enclosed in braces, we read until EOF.
            assertEOF(istr);
    }
    catch (...)
    {
        if (size)
            nested_column.popBack(size);
        throw;
    }

    offsets.push_back(offsets.back() + size);
}


void SerializationArray::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&](const IColumn & nested_column, size_t i)
        {
            nested->serializeTextQuoted(nested_column, i, ostr, settings);
        });
}


void SerializationArray::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&](IColumn & nested_column)
        {
            nested->deserializeTextQuoted(nested_column, istr, settings);
        }, false);
}

void SerializationArray::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    writeChar('[', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);
        nested->serializeTextJSON(nested_column, i, ostr, settings);
    }
    writeChar(']', ostr);
}


void SerializationArray::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&](IColumn & nested_column)
        {
            nested->deserializeTextJSON(nested_column, istr, settings);
        }, false);
}


void SerializationArray::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    writeCString("<array>", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        writeCString("<elem>", ostr);
        nested->serializeTextXML(nested_column, i, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</array>", ostr);
}


void SerializationArray::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// There is no good way to serialize an array in CSV. Therefore, we serialize it into a string, and then write the resulting string in CSV.
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}


void SerializationArray::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);

    if (settings.csv.input_format_arrays_as_nested_csv)
    {
        deserializeTextImpl(column, rb,
            [&](IColumn & nested_column)
            {
                nested->deserializeTextCSV(nested_column, rb, settings);
            }, true);
    }
    else
    {
        deserializeTextImpl(column, rb,
            [&](IColumn & nested_column)
            {
                nested->deserializeTextQuoted(nested_column, rb, settings);
            }, true);
    }
}

}
