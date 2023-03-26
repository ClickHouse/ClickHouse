#include <DataTypes/Serializations/SerializationNamed.h>

namespace DB
{

void SerializationNamed::enumerateStreams(
    SubstreamPath & path,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    addToPath(path);
    path.back().data = data;
    path.back().creator = std::make_shared<SubcolumnCreator>(name, escape_delimiter);

    nested_serialization->enumerateStreams(path, callback, data);
    path.pop_back();
}

void SerializationNamed::serializeBinaryBulkStatePrefix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->serializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void SerializationNamed::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();
}

void SerializationNamed::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state);
    settings.path.pop_back();
}

void SerializationNamed::serializeBinaryBulkWithMultipleStreams(
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

void SerializationNamed::deserializeBinaryBulkWithMultipleStreams(
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

void SerializationNamed::addToPath(SubstreamPath & path) const
{
    path.push_back(Substream::TupleElement);
    path.back().tuple_element_name = name;
    path.back().escape_tuple_delimiter = escape_delimiter;
}

}
