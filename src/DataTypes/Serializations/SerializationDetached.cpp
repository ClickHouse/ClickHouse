#include <DataTypes/Serializations/SerializationDetached.h>
#include "Columns/ColumnBlob.h"

namespace DB
{
SerializationDetached::SerializationDetached(const SerializationPtr & nested_) : nested(nested_)
{
}

void SerializationDetached::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    nested->enumerateStreams(settings, callback, data);
}

void SerializationDetached::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    const IColumn & nested_column = *typeid_cast<const ColumnBlob &>(column).getNestedColumn();
    nested->serializeBinaryBulkStatePrefix(nested_column, settings, state);
}

void SerializationDetached::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    const IColumn & nested_column = *typeid_cast<const ColumnBlob &>(column).getNestedColumn();
    nested->serializeBinaryBulkWithMultipleStreams(nested_column, offset, limit, settings, state);
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
    auto nested_column = typeid_cast<const ColumnBlob &>(*column).getNestedColumn();
    nested->deserializeBinaryBulkWithMultipleStreams(nested_column, limit, settings, state, cache);
}

}
