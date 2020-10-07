#include <Columns/ColumnArray.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Formats/FormatSettings.h>
#include <Formats/ProtobufReader.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>

#include <Parsers/IAST.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}


DataTypeArray::DataTypeArray(const DataTypePtr & nested_)
    : nested{nested_}
{
}


void DataTypeArray::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const Array & a = get<const Array &>(field);
    writeVarUInt(a.size(), ostr);
    for (size_t i = 0; i < a.size(); ++i)
    {
        nested->serializeBinary(a[i], ostr);
    }
}


void DataTypeArray::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    size_t size;
    readVarUInt(size, istr);
    field = Array(size);
    Array & arr = get<Array &>(field);
    for (size_t i = 0; i < size; ++i)
        nested->deserializeBinary(arr[i], istr);
}


void DataTypeArray::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
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


void DataTypeArray::deserializeBinary(IColumn & column, ReadBuffer & istr) const
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

    void deserializeArraySizesPositionIndependent(IColumn & column, ReadBuffer & istr, UInt64 limit)
    {
        ColumnArray & column_array = typeid_cast<ColumnArray &>(column);
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
}


void DataTypeArray::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    path.push_back(Substream::ArraySizes);
    callback(path);
    path.back() = Substream::ArrayElements;
    nested->enumerateStreams(callback, path);
    path.pop_back();
}


void DataTypeArray::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void DataTypeArray::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}


void DataTypeArray::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::ArrayElements);
    nested->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}


void DataTypeArray::serializeBinaryBulkWithMultipleStreams(
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
            DataTypeNumber<ColumnArray::Offset>().serializeBinaryBulk(*column_array.getOffsetsPtr(), *stream, offset, limit);
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


void DataTypeArray::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    ColumnArray & column_array = typeid_cast<ColumnArray &>(column);

    settings.path.push_back(Substream::ArraySizes);
    if (auto * stream = settings.getter(settings.path))
    {
        if (settings.position_independent_encoding)
            deserializeArraySizesPositionIndependent(column, *stream, limit);
        else
            DataTypeNumber<ColumnArray::Offset>().deserializeBinaryBulk(column_array.getOffsetsColumn(), *stream, limit, 0);
    }

    settings.path.back() = Substream::ArrayElements;

    ColumnArray::Offsets & offset_values = column_array.getOffsets();
    IColumn & nested_column = column_array.getData();

    /// Number of values corresponding with `offset_values` must be read.
    size_t last_offset = offset_values.back();
    if (last_offset < nested_column.size())
        throw Exception("Nested column is longer than last offset", ErrorCodes::LOGICAL_ERROR);
    size_t nested_limit = last_offset - nested_column.size();

    /// Adjust value size hint. Divide it to the average array size.
    settings.avg_value_size_hint = nested_limit ? settings.avg_value_size_hint / nested_limit * offset_values.size() : 0;

    nested->deserializeBinaryBulkWithMultipleStreams(nested_column, nested_limit, settings, state);
    settings.path.pop_back();

    /// Check consistency between offsets and elements subcolumns.
    /// But if elements column is empty - it's ok for columns of Nested types that was added by ALTER.
    if (!nested_column.empty() && nested_column.size() != last_offset)
        throw Exception("Cannot read all array values: read just " + toString(nested_column.size()) + " of " + toString(last_offset),
            ErrorCodes::CANNOT_READ_ALL_DATA);
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
static void deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && read_nested)
{
    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
    ColumnArray::Offsets & offsets = column_array.getOffsets();

    IColumn & nested_column = column_array.getData();

    size_t size = 0;
    assertChar('[', istr);

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
                    throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == ']')
                break;

            read_nested(nested_column);
            ++size;

            skipWhitespaceIfAny(istr);
        }
        assertChar(']', istr);
    }
    catch (...)
    {
        if (size)
            nested_column.popBack(size);
        throw;
    }

    offsets.push_back(offsets.back() + size);
}


void DataTypeArray::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&](const IColumn & nested_column, size_t i)
        {
            nested->serializeAsTextQuoted(nested_column, i, ostr, settings);
        });
}


void DataTypeArray::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&](IColumn & nested_column)
        {
            nested->deserializeAsTextQuoted(nested_column, istr, settings);
        });
}

void DataTypeArray::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
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
        nested->serializeAsTextJSON(nested_column, i, ostr, settings);
    }
    writeChar(']', ostr);
}


void DataTypeArray::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr, [&](IColumn & nested_column) { nested->deserializeAsTextJSON(nested_column, istr, settings); });
}


void DataTypeArray::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
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
        nested->serializeAsTextXML(nested_column, i, ostr, settings);
        writeCString("</elem>", ostr);
    }
    writeCString("</array>", ostr);
}


void DataTypeArray::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    /// There is no good way to serialize an array in CSV. Therefore, we serialize it into a string, and then write the resulting string in CSV.
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}


void DataTypeArray::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);
    deserializeText(column, rb, settings);
}


void DataTypeArray::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    const ColumnArray & column_array = assert_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets & offsets = column_array.getOffsets();
    size_t offset = offsets[row_num - 1] + value_index;
    size_t next_offset = offsets[row_num];
    const IColumn & nested_column = column_array.getData();
    size_t i;
    for (i = offset; i < next_offset; ++i)
    {
        size_t element_stored = 0;
        nested->serializeProtobuf(nested_column, i, protobuf, element_stored);
        if (!element_stored)
            break;
    }
    value_index += i - offset;
}


void DataTypeArray::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    row_added = false;
    ColumnArray & column_array = assert_cast<ColumnArray &>(column);
    IColumn & nested_column = column_array.getData();
    ColumnArray::Offsets & offsets = column_array.getOffsets();
    size_t old_size = offsets.size();
    try
    {
        bool nested_row_added;
        do
            nested->deserializeProtobuf(nested_column, protobuf, true, nested_row_added);
        while (nested_row_added && protobuf.canReadMoreValues());
        if (allow_add_row)
        {
            offsets.emplace_back(nested_column.size());
            row_added = true;
        }
        else
            offsets.back() = nested_column.size();
    }
    catch (...)
    {
        offsets.resize_assume_reserved(old_size);
        nested_column.popBack(nested_column.size() - offsets.back());
        throw;
    }
}


MutableColumnPtr DataTypeArray::createColumn() const
{
    return ColumnArray::create(nested->createColumn(), ColumnArray::ColumnOffsets::create());
}


Field DataTypeArray::getDefault() const
{
    return Array();
}


bool DataTypeArray::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && nested->equals(*static_cast<const DataTypeArray &>(rhs).nested);
}


size_t DataTypeArray::getNumberOfDimensions() const
{
    const DataTypeArray * nested_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!nested_array)
        return 1;
    return 1 + nested_array->getNumberOfDimensions();   /// Every modern C++ compiler optimizes tail recursion.
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("Array data type family must have exactly one argument - type of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0]));
}


void registerDataTypeArray(DataTypeFactory & factory)
{
    factory.registerDataType("Array", create);
}

}
