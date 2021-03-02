#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Columns/IColumn.h>

namespace DB
{

void SerializationWrapper::enumerateStreams(const StreamCallback & callback, SubstreamPath & path) const
{
    if (nested)
		nested->enumerateStreams(callback, path);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (nested)
		nested->serializeBinaryBulkStatePrefix(settings, state);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (nested)
		nested->serializeBinaryBulkStateSuffix(settings, state);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    if (nested)
		nested->deserializeBinaryBulkStatePrefix(settings, state);
	else
		throwNoSerialization();
}
    
void SerializationWrapper::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    if (nested)
		nested->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (nested)
		nested->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeBinaryBulk(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
    if (nested)
		nested->serializeBinaryBulk(column, ostr, offset, limit);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeBinaryBulk(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
    if (nested)
		nested->deserializeBinaryBulk(column, istr, limit, avg_value_size_hint);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    if (nested)
		nested->serializeBinary(field, ostr);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    if (nested)
		nested->deserializeBinary(field, istr);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    if (nested)
		nested->serializeBinary(column, row_num, ostr);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    if (nested)
		nested->deserializeBinary(column, istr);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeProtobuf(const IColumn & column, size_t row_num, ProtobufWriter & protobuf, size_t & value_index) const
{
    if (nested)
		nested->serializeProtobuf(column, row_num, protobuf, value_index);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeProtobuf(IColumn & column, ProtobufReader & protobuf, bool allow_add_row, bool & row_added) const
{
    if (nested)
		nested->deserializeProtobuf(column, protobuf, allow_add_row, row_added);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (nested)
		nested->serializeTextEscaped(column, row_num, ostr, settings);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeTextEscaped(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (nested)
		nested->deserializeTextEscaped(column, istr, settings);
	else
		throwNoSerialization();
}
    
void SerializationWrapper::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (nested)
		nested->serializeTextQuoted(column, row_num, ostr, settings);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeTextQuoted(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (nested)
		nested->deserializeTextQuoted(column, istr, settings);
	else
		throwNoSerialization();
}
    
void SerializationWrapper::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (nested)
		nested->serializeTextCSV(column, row_num, ostr, settings);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (nested)
		nested->deserializeTextCSV(column, istr, settings);
	else
		throwNoSerialization();
}
    
void SerializationWrapper::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (nested)
		nested->serializeText(column, row_num, ostr, settings);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeWholeText(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (nested)
		nested->deserializeWholeText(column, istr, settings);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (nested)
		nested->serializeTextJSON(column, row_num, ostr, settings);
	else
		throwNoSerialization();
}

void SerializationWrapper::deserializeTextJSON(IColumn & column, ReadBuffer & istr, const FormatSettings & settings) const
{
    if (nested)
		nested->deserializeTextJSON(column, istr, settings);
	else
		throwNoSerialization();
}

void SerializationWrapper::serializeTextXML(const IColumn & column, size_t row_num, WriteBuffer & ostr, const FormatSettings & settings) const
{
    if (nested)
		nested->serializeTextXML(column, row_num, ostr, settings);
	else
		throwNoSerialization();
}

}
