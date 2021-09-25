#include <DataTypes/Serializations/SerializationTupleElement.h>

namespace DB
{

void SerializationTupleElement::enumerateStreams(
    const StreamCallback & callback,
    SubstreamPath & path) const
{
    addToPath(path);
    nested_serialization->enumerateStreams(callback, path);
    path.pop_back();
}

void SerializationTupleElement::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void SerializationTupleElement::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationTupleElement::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void SerializationTupleElement::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
    settings.path.pop_back();
}

void SerializationTupleElement::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    addToPath(settings.path);
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, limit, settings, state, cache);
    settings.path.pop_back();
}

void SerializationTupleElement::addToPath(SubstreamPath & path) const
{
    path.push_back(Substream::TupleElement);
    path.back().tuple_element_name = name;
    path.back().escape_tuple_delimiter = escape_delimiter;
}

}
