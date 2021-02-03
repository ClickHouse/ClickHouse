#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Columns/IColumn.h>

namespace DB
{

void SerializationWrapper::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    nested->enumerateStreams(callback, path);
}

void SerializationWrapper::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(settings, state);
}

void SerializationWrapper::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationWrapper::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state);
}
    
void SerializationWrapper::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}

void SerializationWrapper::deserializeBinaryBulkWithMultipleStreamsImpl(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    nested->deserializeBinaryBulkWithMultipleStreamsImpl(column, limit, settings, state, cache);
}

void SerializationWrapper::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    nested->serializeBinaryBulk(column, ostr, offset, limit);
}

void SerializationWrapper::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    nested->deserializeBinaryBulk(column, istr, limit, avg_value_size_hint);
}

void SerializationWrapper::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    nested->serializeBinary(field, ostr);
}

void SerializationWrapper::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    nested->deserializeBinary(field, istr);
}

void SerializationWrapper::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    nested->serializeBinary(column, row_num, ostr);
}

void SerializationWrapper::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    nested->deserializeBinary(column, istr);
}

void SerializationWrapper::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    nested->serializeProtobuf(column, row_num, protobuf, value_index);
}

void SerializationWrapper::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    nested->deserializeProtobuf(column, protobuf, allow_add_row, row_added);
}

void SerializationWrapper::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeTextEscaped(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeTextEscaped(column, istr, settings);
}
    
void SerializationWrapper::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeTextQuoted(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeTextQuoted(column, istr, settings);
}
    
void SerializationWrapper::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeTextCSV(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeTextCSV(column, istr, settings);
}
    
void SerializationWrapper::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeText(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeWholeText(column, istr, settings);
}

void SerializationWrapper::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeTextJSON(column, row_num, ostr, settings);
}

void SerializationWrapper::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    nested->deserializeTextJSON(column, istr, settings);
}

void SerializationWrapper::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    nested->serializeTextXML(column, row_num, ostr, settings);
}

}
