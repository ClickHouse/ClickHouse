#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Columns/IColumn.h>

namespace DB
{

void SerializationWrapper::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    nested_serialization->enumerateStreams(callback, path);
}

void SerializationWrapper::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested_serialization->serializeBinaryBulkStatePrefix(settings, state);
}

void SerializationWrapper::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationWrapper::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state);
}

void SerializationWrapper::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{

    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}

void SerializationWrapper::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{

    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
}

void SerializationWrapper::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    nested_serialization->serializeBinaryBulk(column, ostr, offset, limit);
}

void SerializationWrapper::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    nested_serialization->deserializeBinaryBulk(column, istr, limit, avg_value_size_hint);
}

void SerializationWrapper::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    nested_serialization->serializeBinary(field, ostr);
}

void SerializationWrapper::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    nested_serialization->deserializeBinary(field, istr);
}

void SerializationWrapper::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    nested_serialization->serializeBinary(column, row_num, ostr);
}

void SerializationWrapper::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    nested_serialization->deserializeBinary(column, istr);
}

void SerializationWrapper::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeTextEscaped(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeTextEscaped(column, istr, settings);
}

void SerializationWrapper::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeTextQuoted(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeTextQuoted(column, istr, settings);
}

void SerializationWrapper::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeTextCSV(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeTextCSV(column, istr, settings);
}

void SerializationWrapper::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeText(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeWholeText(column, istr, settings);
}

void SerializationWrapper::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeTextJSON(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested_serialization->deserializeTextJSON(column, istr, settings);
}

void SerializationWrapper::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested_serialization->serializeTextXML(column, row_num, ostr, settings);
}

}
