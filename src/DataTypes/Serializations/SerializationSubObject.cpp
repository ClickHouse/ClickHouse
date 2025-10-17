#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationSubObject.h>
#include <DataTypes/Serializations/SerializationSubObjectSharedData.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationSubObject::SerializationSubObject(
    const String & paths_prefix_, const std::unordered_map<String, SerializationPtr> & typed_paths_serializations_, const DataTypePtr & dynamic_type_)
    : paths_prefix(paths_prefix_)
    , typed_paths_serializations(typed_paths_serializations_)
    , dynamic_type(dynamic_type_)
    , dynamic_serialization(dynamic_type->getDefaultSerialization())
{
}

struct DeserializeBinaryBulkStateSubObject : public ISerialization::DeserializeBinaryBulkState
{
    std::unordered_map<String, ISerialization::DeserializeBinaryBulkStatePtr> typed_path_states;
    std::unordered_map<String, ISerialization::DeserializeBinaryBulkStatePtr> dynamic_path_states;
    std::vector<String> dynamic_paths;
    std::vector<String> dynamic_sub_paths;
    SerializationPtr shared_data_serialization;
    ISerialization::DeserializeBinaryBulkStatePtr shared_data_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateSubObject>(*this);

        for (const auto & [path, state] : typed_path_states)
            new_state->typed_path_states[path] = state ? state->clone() : nullptr;

        for (const auto & [path, state] : dynamic_path_states)
            new_state->dynamic_path_states[path] = state ? state->clone() : nullptr;

        new_state->shared_data_state = shared_data_state ? shared_data_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationSubObject::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
{
    settings.path.push_back(Substream::ObjectStructure);
    callback(settings.path);
    settings.path.pop_back();

    const auto * column_object = data.column ? &assert_cast<const ColumnObject &>(*data.column) : nullptr;
    const auto * type_object = data.type ? &assert_cast<const DataTypeObject &>(*data.type) : nullptr;
    const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateSubObject>(data.deserialize_state) : nullptr;

    settings.path.push_back(Substream::ObjectData);

    /// typed_paths_serializations contains only typed paths with requested prefix from original Object column.
    for (const auto & [path, serialization] : typed_paths_serializations)
    {
        settings.path.push_back(Substream::ObjectTypedPath);
        settings.path.back().object_path_name = path;
        auto path_data = SubstreamData(serialization)
                             .withType(type_object ? type_object->getTypedPaths().at(path.substr(paths_prefix.size())) : nullptr)
                             .withColumn(column_object ? column_object->getTypedPaths().at(path.substr(paths_prefix.size())) : nullptr)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(deserialize_state ? deserialize_state->typed_path_states.at(path) : nullptr);
        settings.path.back().data = path_data;
        serialization->enumerateStreams(settings, callback, path_data);
        settings.path.pop_back();
    }

    /// If deserialize state is provided, enumerate streams for dynamic paths and shared data.
    if (deserialize_state)
    {
        DataTypePtr type = std::make_shared<DataTypeDynamic>();
        for (const auto & [path, state] : deserialize_state->dynamic_path_states)
        {
            settings.path.push_back(Substream::ObjectDynamicPath);
            settings.path.back().object_path_name = path;
            auto path_data = SubstreamData(dynamic_serialization)
                                 .withType(type_object ? type : nullptr)
                                 .withColumn(nullptr)
                                 .withSerializationInfo(data.serialization_info)
                                 .withDeserializeState(state);
            settings.path.back().data = path_data;
            dynamic_serialization->enumerateStreams(settings, callback, path_data);
            settings.path.pop_back();
        }

        /// We will need to read shared data to find all paths with requested prefix.
        settings.path.push_back(Substream::ObjectSharedData);
        auto shared_data_substream_data = SubstreamData(deserialize_state->shared_data_serialization)
                                              .withType(DataTypeObject::getTypeOfSharedData())
                                              .withColumn(column_object ? column_object->getSharedDataPtr() : nullptr)
                                              .withSerializationInfo(data.serialization_info)
                                              .withDeserializeState(deserialize_state ? deserialize_state->shared_data_state : nullptr);
        settings.path.back().data = shared_data_substream_data;
        deserialize_state->shared_data_serialization->enumerateStreams(settings, callback, shared_data_substream_data);
        settings.path.pop_back();
    }

    settings.path.pop_back();
}

void SerializationSubObject::serializeBinaryBulkStatePrefix(const IColumn &, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationSubObject");
}

void SerializationSubObject::serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationSubObject");
}


void SerializationSubObject::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto structure_state = SerializationObject::deserializeObjectStructureStatePrefix(settings, cache);
    if (!structure_state)
        return;

    auto * structure_state_concrete = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(structure_state);
    auto sub_object_state = std::make_shared<DeserializeBinaryBulkStateSubObject>();
    settings.path.push_back(Substream::ObjectData);
    for (const auto & [path, serialization] : typed_paths_serializations)
    {
        settings.path.push_back(Substream::ObjectTypedPath);
        settings.path.back().object_path_name = path;
        serialization->deserializeBinaryBulkStatePrefix(settings, sub_object_state->typed_path_states[path], cache);
        settings.path.pop_back();
    }

    for (const auto & dynamic_path : *structure_state_concrete->sorted_dynamic_paths)
    {
        /// Save only dynamic paths with requested prefix.
        if (dynamic_path.starts_with(paths_prefix))
        {
            settings.path.push_back(Substream::ObjectDynamicPath);
            settings.path.back().object_path_name = dynamic_path;
            dynamic_serialization->deserializeBinaryBulkStatePrefix(settings, sub_object_state->dynamic_path_states[dynamic_path], cache);
            settings.path.pop_back();
            sub_object_state->dynamic_paths.push_back(dynamic_path);
            sub_object_state->dynamic_sub_paths.push_back(dynamic_path.substr(paths_prefix.size()));
        }
    }

    settings.path.push_back(Substream::ObjectSharedData);
    sub_object_state->shared_data_serialization = std::make_shared<SerializationSubObjectSharedData>(
        structure_state_concrete->shared_data_serialization_version,
        structure_state_concrete->shared_data_buckets,
        paths_prefix,
        dynamic_type);
    sub_object_state->shared_data_serialization->deserializeBinaryBulkStatePrefix(settings, sub_object_state->shared_data_state, cache);
    settings.path.pop_back();

    settings.path.pop_back();
    state = std::move(sub_object_state);
}

void SerializationSubObject::serializeBinaryBulkWithMultipleStreams(const IColumn &, size_t, size_t, SerializeBinaryBulkSettings &, SerializeBinaryBulkStatePtr &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationSubObject");
}

void SerializationSubObject::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & result_column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * sub_object_state = checkAndGetState<DeserializeBinaryBulkStateSubObject>(state);
    auto mutable_column = result_column->assumeMutable();
    auto & column_object = assert_cast<ColumnObject &>(*mutable_column);
    /// If it's a new object column, set dynamic paths and statistics.
    if (column_object.empty())
        column_object.setDynamicPaths(sub_object_state->dynamic_sub_paths);

    auto & typed_paths = column_object.getTypedPaths();
    auto & dynamic_paths = column_object.getDynamicPaths();

    settings.path.push_back(Substream::ObjectData);
    for (const auto & [path, serialization] : typed_paths_serializations)
    {
        settings.path.push_back(Substream::ObjectTypedPath);
        settings.path.back().object_path_name = path;
        serialization->deserializeBinaryBulkWithMultipleStreams(typed_paths[path.substr(paths_prefix.size())], rows_offset, limit, settings, sub_object_state->typed_path_states[path], cache);
        settings.path.pop_back();
    }

    for (const auto & path : sub_object_state->dynamic_paths)
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        dynamic_serialization->deserializeBinaryBulkWithMultipleStreams(dynamic_paths[path.substr(paths_prefix.size())], rows_offset, limit, settings, sub_object_state->dynamic_path_states[path], cache);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::ObjectSharedData);
    sub_object_state->shared_data_serialization->deserializeBinaryBulkWithMultipleStreams(column_object.getSharedDataPtr(), rows_offset, limit, settings, sub_object_state->shared_data_state, cache);
    settings.path.pop_back();

    settings.path.pop_back();
}

}
