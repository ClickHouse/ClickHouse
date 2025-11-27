#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectTypedPath.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


void SerializationObjectTypedPath::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    auto path_data = SubstreamData(nested_serialization)
                         .withType(data.type)
                         .withColumn(data.column)
                         .withSerializationInfo(data.serialization_info)
                         .withDeserializeState(data.deserialize_state);
    nested_serialization->enumerateStreams(settings, callback, path_data);
    settings.path.pop_back();
    settings.path.pop_back();
}

void SerializationObjectTypedPath::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectTypedPath");
}

void SerializationObjectTypedPath::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectTypedPath");
}

void SerializationObjectTypedPath::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, state, cache);
    settings.path.pop_back();
    settings.path.pop_back();
}

void SerializationObjectTypedPath::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectTypedPath");
}

void SerializationObjectTypedPath::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectTypedPath);
    settings.path.back().object_path_name = path;
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, limit, settings, state, cache);
    settings.path.pop_back();
    settings.path.pop_back();
}

}
