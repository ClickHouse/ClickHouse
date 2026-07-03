#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectDynamicPath.h>
#include <DataTypes/Serializations/SerializationObjectSharedDataPath.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectDynamicPath::SerializationObjectDynamicPath(
    const DB::SerializationPtr & nested_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, const DataTypePtr & subcolumn_type_)
    : SerializationWrapper(nested_)
    , path(path_)
    , path_subcolumn(path_subcolumn_)
    , dynamic_serialization(std::make_shared<SerializationDynamic>())
    , dynamic_type(dynamic_type_)
    , subcolumn_type(subcolumn_type_)
{
}

struct DeserializeBinaryBulkStateObjectDynamicPath : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr structure_state;
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;
    SerializationPtr shared_data_path_serialization;
    bool read_from_shared_data;
    ColumnPtr shared_data;
    size_t shared_data_size = 0;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectDynamicPath>(*this);
        new_state->structure_state = structure_state ? structure_state->clone() : nullptr;
        new_state->nested_state = nested_state ? nested_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationObjectDynamicPath::enumerateStreams(
    ISerialization::EnumerateStreamsSettings & settings,
    const ISerialization::StreamCallback & callback,
    const ISerialization::SubstreamData & data) const
{
    settings.path.push_back(Substream::ObjectStructure);
    callback(settings.path);
    settings.path.pop_back();

    const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateObjectDynamicPath>(data.deserialize_state) : nullptr;

    /// We cannot enumerate anything if we don't have deserialization state, as we don't know the dynamic structure.
    if (!deserialize_state)
        return;

    settings.path.push_back(Substream::ObjectData);
    const auto * structure_state = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(deserialize_state->structure_state);
    /// Check if we have our path in dynamic paths.
    if (structure_state->dynamic_paths.contains(path))
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        auto path_data = SubstreamData(nested_serialization)
                             .withType(data.type)
                             .withColumn(data.column)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(deserialize_state->nested_state);
        settings.path.back().data = path_data;
        nested_serialization->enumerateStreams(settings, callback, path_data);
        settings.path.pop_back();
    }
    /// Otherwise we will have to read all shared data and try to find our path there.
    else
    {
        settings.path.push_back(Substream::ObjectSharedData);
        auto shared_data_path_substream_data = data;
        shared_data_path_substream_data.deserialize_state = deserialize_state->nested_state;
        settings.path.back().data = shared_data_path_substream_data;
        deserialize_state->shared_data_path_serialization->enumerateStreams(settings, callback, shared_data_path_substream_data);
        settings.path.pop_back();
    }

    settings.path.pop_back();
}

void SerializationObjectDynamicPath::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectDynamicPath");
}

void SerializationObjectDynamicPath::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectDynamicPath");
}

void SerializationObjectDynamicPath::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto structure_state = SerializationObject::deserializeObjectStructureStatePrefix(settings, cache);
    if (!structure_state)
        return;

    auto dynamic_path_state = std::make_shared<DeserializeBinaryBulkStateObjectDynamicPath>();
    dynamic_path_state->structure_state = std::move(structure_state);
    /// Remember if we need to read from shared data or we have this path in dynamic paths.
    auto * object_structure_state = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(dynamic_path_state->structure_state);
    dynamic_path_state->read_from_shared_data = !object_structure_state->dynamic_paths.contains(path);
    settings.path.push_back(Substream::ObjectData);
    if (dynamic_path_state->read_from_shared_data)
    {
        settings.path.push_back(Substream::ObjectSharedData);
        dynamic_path_state->shared_data_path_serialization = std::make_shared<SerializationObjectSharedDataPath>(
            nested_serialization,
            object_structure_state->shared_data_serialization_version,
            path,
            path_subcolumn,
            dynamic_type,
            subcolumn_type,
            getSharedDataPathBucket(path, object_structure_state->shared_data_buckets));
        dynamic_path_state->shared_data_path_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }
    else
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        nested_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }

    settings.path.pop_back();
    state = std::move(dynamic_path_state);
}

void SerializationObjectDynamicPath::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectDynamicPath");
}

void SerializationObjectDynamicPath::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * dynamic_path_state = checkAndGetState<DeserializeBinaryBulkStateObjectDynamicPath>(state);
    settings.path.push_back(Substream::ObjectData);
    /// Check if we don't need to read shared data. In this case just read data from dynamic path.
    if (!dynamic_path_state->read_from_shared_data)
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        nested_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, rows_offset, limit, settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }
    else
    {
        settings.path.push_back(Substream::ObjectSharedData);
        dynamic_path_state->shared_data_path_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, rows_offset, limit, settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }

    settings.path.pop_back();
}

}
