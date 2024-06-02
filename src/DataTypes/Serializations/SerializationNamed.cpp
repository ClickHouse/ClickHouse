#include <DataTypes/Serializations/SerializationNamed.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SerializationNamed::SerializationNamed(
    const SerializationPtr & nested_,
    const String & name_,
    SubstreamType substream_type_)
    : SerializationWrapper(nested_)
    , name(name_)
    , substream_type(substream_type_)
{
    if (!ISerialization::Substream::named_types.contains(substream_type))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SerializationNamed doesn't support substream type {}", substream_type);
}

void SerializationNamed::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    addToPath(settings.path);
    settings.path.back().data = data;
    settings.path.back().creator = std::make_shared<SubcolumnCreator>(name, substream_type);

    nested_serialization->enumerateStreams(settings, callback, data);
    settings.path.pop_back();
}

void SerializationNamed::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    addToPath(settings.path);
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
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
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    addToPath(settings.path);
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
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
    path.push_back(substream_type);
    path.back().name_of_substream = name;
}

}
