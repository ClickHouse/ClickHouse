#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
}


DataTypeArray::DataTypeArray(DataTypePtr nested_)
    : enriched_nested(std::make_pair(nested_, std::make_shared<DataTypeVoid>())), nested{nested_}
{
    offsets = std::make_shared<DataTypeNumber<ColumnArray::Offset_t>>();
}

DataTypeArray::DataTypeArray(DataTypeTraits::EnrichedDataTypePtr enriched_nested_)
    : enriched_nested{enriched_nested_}, nested{enriched_nested.first}
{
    offsets = std::make_shared<DataTypeNumber<ColumnArray::Offset_t>>();
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
    const ColumnArray & column_array = static_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

    size_t offset = row_num == 0 ? 0 : offsets[row_num - 1];
    size_t next_offset = offsets[row_num];
    size_t size = next_offset - offset;

    writeVarUInt(size, ostr);

    const IColumn & nested_column = column_array.getData();
    for (size_t i = offset; i < next_offset; ++i)
        nested->serializeBinary(nested_column, i, ostr);
}


void DataTypeArray::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    ColumnArray & column_array = static_cast<ColumnArray &>(column);
    ColumnArray::Offsets_t & offsets = column_array.getOffsets();

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

    offsets.push_back((offsets.empty() ? 0 : offsets.back()) + size);
}


void DataTypeArray::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

    if (offset > offsets.size())
        return;

    /** offset - from which array to write.
      * limit - how many arrays should be written, or 0, if you write everything that is.
      * end - up to which array the recorded piece ends.
      *
      * nested_offset - from which element of the innards to write.
      * nested_limit - how many elements of the innards to write, or 0, if you write everything that is.
      */

    size_t end = std::min(offset + limit, offsets.size());

    size_t nested_offset = offset ? offsets[offset - 1] : 0;
    size_t nested_limit = limit
        ? offsets[end - 1] - nested_offset
        : 0;

    if (limit == 0 || nested_limit)
        nested->serializeBinaryBulk(column_array.getData(), ostr, nested_offset, nested_limit);
}


void DataTypeArray::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double) const
{
    ColumnArray & column_array = typeid_cast<ColumnArray &>(column);
    ColumnArray::Offsets_t & offsets = column_array.getOffsets();
    IColumn & nested_column = column_array.getData();

    /// Number of values corresponding with `offsets` must be read.
    size_t last_offset = (offsets.empty() ? 0 : offsets.back());
    if (last_offset < nested_column.size())
        throw Exception("Nested column is longer than last offset", ErrorCodes::LOGICAL_ERROR);
    size_t nested_limit = last_offset - nested_column.size();
    nested->deserializeBinaryBulk(nested_column, istr, nested_limit, 0);

    if (column_array.getData().size() != last_offset)
        throw Exception("Cannot read all array values", ErrorCodes::CANNOT_READ_ALL_DATA);
}


void DataTypeArray::serializeOffsets(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    const ColumnArray & column_array = typeid_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets_t & offsets = column_array.getOffsets();
    size_t size = offsets.size();

    if (!size)
        return;

    size_t end = limit && (offset + limit < size)
        ? offset + limit
        : size;

    if (offset == 0)
    {
        writeIntBinary(offsets[0], ostr);
        ++offset;
    }

    for (size_t i = offset; i < end; ++i)
        writeIntBinary(offsets[i] - offsets[i - 1], ostr);
}


void DataTypeArray::deserializeOffsets(IColumn & column, ReadBuffer & istr, size_t limit) const
{
    ColumnArray & column_array = typeid_cast<ColumnArray &>(column);
    ColumnArray::Offsets_t & offsets = column_array.getOffsets();
    size_t initial_size = offsets.size();
    offsets.resize(initial_size + limit);

    size_t i = initial_size;
    ColumnArray::Offset_t current_offset = initial_size ? offsets[initial_size - 1] : 0;
    while (i < initial_size + limit && !istr.eof())
    {
        ColumnArray::Offset_t current_size = 0;
        readIntBinary(current_size, istr);
        current_offset += current_size;
        offsets[i] = current_offset;
        ++i;
    }

    offsets.resize(i);
}


template <typename Writer>
static void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, Writer && write_nested)
{
    const ColumnArray & column_array = static_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

    size_t offset = row_num == 0 ? 0 : offsets[row_num - 1];
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
    ColumnArray & column_array = static_cast<ColumnArray &>(column);
    ColumnArray::Offsets_t & offsets = column_array.getOffsets();

    IColumn & nested_column = column_array.getData();

    size_t size = 0;
    bool first = true;
    assertChar('[', istr);

    try
    {
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

    offsets.push_back((offsets.empty() ? 0 : offsets.back()) + size);
}


void DataTypeArray::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeTextImpl(column, row_num, ostr,
                    [&](const IColumn & nested_column, size_t i)
                    {
                        nested->serializeTextQuoted(nested_column, i, ostr);
                    });
}


void DataTypeArray::deserializeText(IColumn & column, ReadBuffer & istr) const
{
    deserializeTextImpl(column, istr,
                    [&](IColumn & nested_column)
                    {
                        nested->deserializeTextQuoted(nested_column, istr);
                    });
}


void DataTypeArray::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}


void DataTypeArray::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    deserializeText(column, istr);
}


void DataTypeArray::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    serializeText(column, row_num, ostr);
}


void DataTypeArray::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    deserializeText(column, istr);
}


void DataTypeArray::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON & settings) const
{
    const ColumnArray & column_array = static_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

    size_t offset = row_num == 0 ? 0 : offsets[row_num - 1];
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


void DataTypeArray::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    deserializeTextImpl(column, istr, [&](IColumn & nested_column) { nested->deserializeTextJSON(nested_column, istr); });
}


void DataTypeArray::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    const ColumnArray & column_array = static_cast<const ColumnArray &>(column);
    const ColumnArray::Offsets_t & offsets = column_array.getOffsets();

    size_t offset = row_num == 0 ? 0 : offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const IColumn & nested_column = column_array.getData();

    writeCString("<array>", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        writeCString("<elem>", ostr);
        nested->serializeTextXML(nested_column, i, ostr);
        writeCString("</elem>", ostr);
    }
    writeCString("</array>", ostr);
}


void DataTypeArray::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    /// There is no good way to serialize an array in CSV. Therefore, we serialize it into a string, and then write the resulting string in CSV.
    String s;
    {
        WriteBufferFromString wb(s);
        serializeText(column, row_num, wb);
    }
    writeCSV(s, ostr);
}


void DataTypeArray::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
    String s;
    readCSV(s, istr, delimiter);
    ReadBufferFromString rb(s);
    deserializeText(column, rb);
}


ColumnPtr DataTypeArray::createColumn() const
{
    if (nested->isNull())
    {
        ColumnPtr col = std::make_shared<ColumnUInt8>();
        ColumnPtr null_map = std::make_shared<ColumnUInt8>();
        ColumnPtr nullable_col = std::make_shared<ColumnNullable>(col, null_map);

        return std::make_shared<ColumnArray>(nullable_col);
    }
    else
        return std::make_shared<ColumnArray>(nested->createColumn());
}


ColumnPtr DataTypeArray::createConstColumn(size_t size, const Field & field) const
{
    /// `this` can not be passed as the last argument.
    return std::make_shared<ColumnConstArray>(size, get<const Array &>(field), std::make_shared<DataTypeArray>(nested));
}


const DataTypePtr & DataTypeArray::getMostNestedType() const
{
    const DataTypeArray * array = this;
    const IDataType * array_nested_type = array->getNestedType().get();

    while (true)
    {
        const DataTypeArray * type = typeid_cast<const DataTypeArray *>(array_nested_type);
        if (type == nullptr)
                break;
        else
        {
            array = type;
            array_nested_type = array->getNestedType().get();
        }
    }

    return array->getNestedType();
}

}
