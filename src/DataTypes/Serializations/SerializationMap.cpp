#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeMap.h>

#include <Common/StringUtils.h>
#include <Columns/ColumnMap.h>
#include <Core/Field.h>
#include <Formats/FormatSettings.h>
#include <Formats/JSONUtils.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_MAP_FROM_TEXT;
    extern const int TOO_LARGE_ARRAY_SIZE;
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

void SerializationMap::serializeBinary(const Field & field, WriteBuffer & ostr, const FormatSettings & settings) const
{
    const auto & map = field.safeGet<const Map &>();
    writeVarUInt(map.size(), ostr);
    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<const Tuple>();
        assert(tuple.size() == 2);
        key->serializeBinary(tuple[0], ostr, settings);
        value->serializeBinary(tuple[1], ostr, settings);
    }
}

void SerializationMap::deserializeBinary(Field & field, ReadBuffer & istr, const FormatSettings & settings) const
{
    size_t size;
    readVarUInt(size, istr);
    if (settings.binary.max_binary_string_size && size > settings.binary.max_binary_string_size)
        throw Exception(
            ErrorCodes::TOO_LARGE_ARRAY_SIZE,
            "Too large map size: {}. The maximum is: {}. To increase the maximum, use setting "
            "format_binary_max_array_size",
            size,
            settings.binary.max_binary_string_size);
    field = Map();
    Map & map = field.safeGet<Map &>();
    map.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        Tuple tuple(2);
        key->deserializeBinary(tuple[0], istr, settings);
        value->deserializeBinary(tuple[1], istr, settings);
        map.push_back(std::move(tuple));
    }
}

void SerializationMap::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeBinary(extractNestedColumn(column), row_num, ostr, settings);
}

void SerializationMap::deserializeBinary(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeBinary(extractNestedColumn(column), istr, settings);
}


template <typename KeyWriter, typename ValueWriter>
void SerializationMap::serializeTextImpl(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    KeyWriter && key_writer,
    ValueWriter && value_writer) const
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

        key_writer(ostr, key, nested_tuple.getColumn(0), i);
        writeChar(':', ostr);
        value_writer(ostr, value, nested_tuple.getColumn(1), i);
    }
    writeChar('}', ostr);
}

template <typename ReturnType, typename Reader>
ReturnType SerializationMap::deserializeTextImpl(IColumn & column, ReadBuffer & istr, Reader && reader) const
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto & column_map = assert_cast<ColumnMap &>(column);

    auto & nested_array = column_map.getNestedColumn();
    auto & nested_tuple = column_map.getNestedData();
    auto & offsets = nested_array.getOffsets();

    auto & key_column = nested_tuple.getColumn(0);
    auto & value_column = nested_tuple.getColumn(1);

    size_t size = 0;
    if constexpr (throw_exception)
        assertChar('{', istr);
    else if (!checkChar('{', istr))
        return ReturnType(false);

    auto on_error_no_throw = [&]()
    {
        if (size)
        {
            nested_tuple.getColumnPtr(0) = key_column.cut(0, offsets.back());
            nested_tuple.getColumnPtr(1) = value_column.cut(0, offsets.back());
        }

        return ReturnType(false);
    };

    try
    {
        bool first = true;
        while (!istr.eof() && *istr.position() != '}')
        {
            if (!first)
            {
                if (*istr.position() == ',')
                {
                    ++istr.position();
                }
                else
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::CANNOT_READ_MAP_FROM_TEXT, "Cannot read Map from text");
                    return on_error_no_throw();
                }
            }

            first = false;

            skipWhitespaceIfAny(istr);

            if (*istr.position() == '}')
                break;

            if constexpr (throw_exception)
                reader(istr, key, key_column);
            else if (!reader(istr, key, key_column))
                return on_error_no_throw();

            ++size;

            skipWhitespaceIfAny(istr);
            if constexpr (throw_exception)
                assertChar(':', istr);
            else if (!checkChar(':', istr))
                return on_error_no_throw();
            skipWhitespaceIfAny(istr);

            if constexpr (throw_exception)
                reader(istr, value, value_column);
            else if (!reader(istr, value, value_column))
                return on_error_no_throw();

            skipWhitespaceIfAny(istr);
        }

        if constexpr (throw_exception)
            assertChar('}', istr);
        else if (!checkChar('}', istr))
            return on_error_no_throw();
    }
    catch (...)
    {
        if (size)
        {
            nested_tuple.getColumnPtr(0) = key_column.cut(0, offsets.back());
            nested_tuple.getColumnPtr(1) = value_column.cut(0, offsets.back());
        }

        if constexpr (throw_exception)
            throw;
        return ReturnType(false);
    }

    offsets.push_back(offsets.back() + size);
    return ReturnType(true);
}

void SerializationMap::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    auto writer = [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
    {
        subcolumn_serialization->serializeTextQuoted(subcolumn, pos, buf, settings);
    };

    serializeTextImpl(column, row_num, ostr, writer, writer);
}

void SerializationMap::deserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    deserializeTextImpl(column, istr,
        [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextQuoted(subcolumn, buf, settings, subcolumn_serialization);
            else
                subcolumn_serialization->deserializeTextQuoted(subcolumn, buf, settings);
        });

    if (whole && !istr.eof())
        throwUnexpectedDataAfterParsedValue(column, istr, settings, "Map");
}

bool SerializationMap::tryDeserializeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings, bool whole) const
{
    auto reader = [&settings](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn)
    {
        if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
            return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextQuoted(subcolumn, buf, settings, subcolumn_serialization);
        return subcolumn_serialization->tryDeserializeTextQuoted(subcolumn, buf, settings);
    };

    auto ok = deserializeTextImpl<bool>(column, istr, reader);
    if (!ok)
        return false;

    if (whole && !istr.eof())
    {
        column.popBack(1);
        return false;
    }

    return true;
}

void SerializationMap::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    serializeTextImpl(column, row_num, ostr,
        [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            /// We need to double-quote all keys (including integers) to produce valid JSON.
            WriteBufferFromOwnString str_buf;
            subcolumn_serialization->serializeText(subcolumn, pos, str_buf, settings);
            writeJSONString(str_buf.str(), buf, settings);
        },
        [&settings](WriteBuffer & buf, const SerializationPtr & subcolumn_serialization, const IColumn & subcolumn, size_t pos)
        {
            subcolumn_serialization->serializeTextJSON(subcolumn, pos, buf, settings);
        });
}

void SerializationMap::serializeTextJSONPretty(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings, size_t indent) const
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);

    const auto & nested_array = column_map.getNestedColumn();
    const auto & nested_tuple = column_map.getNestedData();
    const auto & offsets = nested_array.getOffsets();

    size_t offset = offsets[row_num - 1];
    size_t next_offset = offsets[row_num];

    if (offset == next_offset)
    {
        writeCString("{}", ostr);
        return;
    }

    writeCString("{\n", ostr);
    for (size_t i = offset; i < next_offset; ++i)
    {
        if (i != offset)
            writeCString(",\n", ostr);

        WriteBufferFromOwnString str_buf;
        key->serializeText(nested_tuple.getColumn(0), i, str_buf, settings);

        writeChar(' ', (indent + 1) * 4, ostr);
        writeJSONString(str_buf.str(), ostr, settings);
        writeCString(": ", ostr);
        value->serializeTextJSONPretty(nested_tuple.getColumn(1), i, ostr, settings, indent + 1);
    }
    writeChar('\n', ostr);
    writeChar(' ', indent * 4, ostr);
    writeChar('}', ostr);
}


template <typename ReturnType>
ReturnType SerializationMap::deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    auto deserialize_nested = [&settings](IColumn & subcolumn, ReadBuffer & buf, const SerializationPtr & subcolumn_serialization) -> ReturnType
    {
        if constexpr (std::is_same_v<ReturnType, void>)
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
                SerializationNullable::deserializeNullAsDefaultOrNestedTextJSON(subcolumn, buf, settings, subcolumn_serialization);
            else
                subcolumn_serialization->deserializeTextJSON(subcolumn, buf, settings);
        }
        else
        {
            if (settings.null_as_default && !isColumnNullableOrLowCardinalityNullable(subcolumn))
                return SerializationNullable::tryDeserializeNullAsDefaultOrNestedTextJSON(subcolumn, buf, settings, subcolumn_serialization);
            return subcolumn_serialization->tryDeserializeTextJSON(subcolumn, buf, settings);
        }
    };

    if (settings.json.empty_as_default)
        return deserializeTextImpl<ReturnType>(column, istr,
            [&deserialize_nested](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn) -> ReturnType
            {
                return JSONUtils::deserializeEmpyStringAsDefaultOrNested<ReturnType>(subcolumn, buf,
                    [&deserialize_nested, &subcolumn_serialization](IColumn & subcolumn_, ReadBuffer & buf_) -> ReturnType
                    {
                        return deserialize_nested(subcolumn_, buf_, subcolumn_serialization);
                    });
            });
    return deserializeTextImpl<ReturnType>(
        column,
        istr,
        [&deserialize_nested](ReadBuffer & buf, const SerializationPtr & subcolumn_serialization, IColumn & subcolumn) -> ReturnType
        { return deserialize_nested(subcolumn, buf, subcolumn_serialization); });
}

void SerializationMap::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    deserializeTextJSONImpl<void>(column, istr, settings);
}

bool SerializationMap::tryDeserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    return deserializeTextJSONImpl<bool>(column, istr, settings);
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
    deserializeText(column, rb, settings, true);
}

bool SerializationMap::tryDeserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    String s;
    if (!tryReadCSV(s, istr, settings.csv))
        return false;
    ReadBufferFromString rb(s);
    return tryDeserializeText(column, rb, settings, true);
}

void SerializationMap::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    auto next_data = SubstreamData(nested)
        .withType(data.type ? assert_cast<const DataTypeMap &>(*data.type).getNestedType() : nullptr)
        .withColumn(data.column ? assert_cast<const ColumnMap &>(*data.column).getNestedColumnPtr() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(data.deserialize_state);

    nested->enumerateStreams(settings, callback, next_data);
}

void SerializationMap::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(extractNestedColumn(column), settings, state);
}

void SerializationMap::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationMap::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
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
