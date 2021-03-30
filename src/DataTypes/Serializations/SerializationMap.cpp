#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationTuple.h>

#include <Common/StringUtils/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>

#include <ext/map.h>
#include <ext/enumerate.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_MAP_FROM_TEXT;
}

SerializationMap::SerializationMap(const SerializationPtr & key_, const SerializationPtr & value_, const SerializationPtr & nested_)
    : key(key_), value(value_), nested(nested_)
{
}

static const IColumn & extractNestedColumn(const IColumn & column)
{
    return assert_cast<const ColumnMap &>(column).getNestedColumn();
}

static IColumn & extractNestedColumn(IColumn & column)
{
    return assert_cast<ColumnMap &>(column).getNestedColumn();
}

void SerializationMap::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & map = get<const Map &>(field);
    writeVarUInt(map.size(), ostr);
    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<const Tuple>();
        assert(tuple.size() == 2);
        key->serializeBinary(tuple[0], ostr);
        value->serializeBinary(tuple[1], ostr);
    }
}

void SerializationMap::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    size_t size;
    readVarUInt(size, istr);
    field = Map(size);
    for (auto & elem : field.get<Map &>())
    {
        Tuple tuple(2);
        key->deserializeBinary(tuple[0], istr);
        value->deserializeBinary(tuple[1], istr);
        elem = std::move(tuple);
    }
}

void SerializationMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    nested->serializeBinary(extractNestedColumn(column), row_num, ostr);
}

void SerializationMap::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    nested->deserializeBinary(extractNestedColumn(column), istr);
}


template <typename Writer>
void SerializationMap::serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, Writer && writer) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);

    const auto & nested_array = column_map.getNestedColumn();
    const auto & nested_tuple = column_map.getNestedData();
    const auto & offsets = nested_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    writeChar('{', ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeChar(',', ostr);
        writer(key, nested_tuple.getColumn(0), i);
        writeChar(':', ostr);
        writer(value, nested_tuple.getColumn(1), i);
    }
    writeChar('}', ostr);
}

template <typename Reader>
void SerializationMap::deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && reader) const
{
    auto & column_map = assert_cast<ColumnMap &>(column);

    auto & nested_array = column_map.getNestedColumn();
    auto & nested_tuple = column_map.getNestedData();
    auto & offsets = nested_array.getOffsets();

    auto & key_column = nested_tuple.getColumn(0);
    auto & value_column = nested_tuple.getColumn(1);

    size_t size = 0;
    assertChar('{', istr);

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != '}')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                    ++istr.position();
                else
                    throw Exception("Cannot read Map from text", ErrorCodes::CANNOT_READ_MAP_FROM_TEXT);
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == '}')
                break;

            reader(key, key_column);
            skipWhitespaceIfAny(istr);
            assertChar(':', istr);

            ++size;
            skipWhitespaceIfAny(istr);
            reader(value, value_column);

            skipWhitespaceIfAny(istr);
        }

        offsets.push_back(offsets.back() + size);
        assertChar('}', istr);
    }
    catch (...)
    {
        throw;
    }
}

void SerializationMap::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&](const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            subcolumn_serialization->serializeTextQuoted(subcolumn, pos, ostr, settings);
        });
}

void SerializationMap::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{

    deserializeTextImpl(column, istr,
        [&](const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
        {
            subcolumn_serialization->deserializeTextQuoted(subcolumn, istr, settings);
        });
}


void SerializationMap::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&](const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            subcolumn_serialization->serializeTextJSON(subcolumn, pos, ostr, settings);
        });
}

void SerializationMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextImpl(column, istr,
        [&](const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
        {
            subcolumn_serialization->deserializeTextJSON(subcolumn, istr, settings);
        });
}

void SerializationMap::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & offsets = column_map.getNestedColumn().getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    const auto & nested_data = column_map.getNestedData();

    writeCString("<map>", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        writeCString("<elem>", ostr);
        writeCString("<key>", ostr);
        key->serializeTextXML(nested_data.getColumn(0), i, ostr, settings);
        writeCString("</key>", ostr);

        writeCString("<value>", ostr);
        value->serializeTextXML(nested_data.getColumn(1), i, ostr, settings);
        writeCString("</value>", ostr);
        writeCString("</elem>", ostr);
    }
    writeCString("</map>", ostr);
}

void SerializationMap::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    WriteBufferFromOwnString wb;
    serializeText(column, row_num, wb, settings);
    writeCSV(wb.str(), ostr);
}

void SerializationMap::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    readCSV(s, istr, settings.csv);
    ReadBufferFromString rb(s);
    deserializeText(column, rb, settings);
}


void SerializationMap::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    nested->enumerateStreams(callback, path);
}

void SerializationMap::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(settings, state);
}

void SerializationMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state);
}


void SerializationMap::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkWithMultipleStreams(extractNestedColumn(column), offset, limit, settings, state);
}

void SerializationMap::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto & column_map = assert_cast<ColumnMap &>(*column->assumeMutable());
    nested->deserializeBinaryBulkWithMultipleStreams(column_map.getNestedColumnPtr(), limit, settings, state, cache);
}

}
