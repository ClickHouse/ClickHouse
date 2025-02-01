#include <DataTypes/Serializations/SerializationDetached.h>

namespace DB
{

void SerializationDetached::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStatePrefix(column, settings, state);
}

void SerializationDetached::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
}

void SerializationDetached::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    nested->serializeBinaryBulkStateSuffix(settings, state);
}

void SerializationDetached::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    nested->deserializeBinaryBulkStatePrefix(settings, state, cache);
}

void SerializationDetached::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    nested->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
}

}
