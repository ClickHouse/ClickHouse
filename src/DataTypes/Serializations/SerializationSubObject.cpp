#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationSubObject.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationSubObject::SerializationSubObject(
    const String & path_prefix_, const std::unordered_map<String, SerializationPtr> & typed_paths_serializations_)
    : path_prefix(path_prefix_)
    , typed_paths_serializations(typed_paths_serializations_)
    , dynamic_serialization(std::make_shared<SerializationDynamic>())
    , shared_data_serialization(SerializationObject::getTypeOfSharedData()->getDefaultSerialization())
{
}

struct DeserializeBinaryBulkStateSubObject : public ISerialization::DeserializeBinaryBulkState
{
    std::unordered_map<String, ISerialization::DeserializeBinaryBulkStatePtr> typed_path_states;
    std::unordered_map<String, ISerialization::DeserializeBinaryBulkStatePtr> dynamic_path_states;
    std::vector<String> dynamic_paths;
    std::vector<String> dynamic_sub_paths;
    ISerialization::DeserializeBinaryBulkStatePtr shared_data_state;
    ColumnPtr shared_data;
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
                             .withType(type_object ? type_object->getTypedPaths().at(path.substr(path_prefix.size() + 1)) : nullptr)
                             .withColumn(column_object ? column_object->getTypedPaths().at(path.substr(path_prefix.size() + 1)) : nullptr)
                             .withSerializationInfo(data.serialization_info)
                             .withDeserializeState(deserialize_state ? deserialize_state->typed_path_states.at(path) : nullptr);
        settings.path.back().data = path_data;
        serialization->enumerateStreams(settings, callback, path_data);
        settings.path.pop_back();
    }

    /// We will need to read shared data to find all paths with requested prefix.
    settings.path.push_back(Substream::ObjectSharedData);
    auto shared_data_substream_data = SubstreamData(shared_data_serialization)
                                          .withType(data.type ? SerializationObject::getTypeOfSharedData() : nullptr)
                                          .withColumn(data.column ? SerializationObject::getTypeOfSharedData()->createColumn() : nullptr)
                                          .withSerializationInfo(data.serialization_info)
                                          .withDeserializeState(deserialize_state ? deserialize_state->shared_data_state : nullptr);
    settings.path.back().data = shared_data_substream_data;
    shared_data_serialization->enumerateStreams(settings, callback, shared_data_substream_data);
    settings.path.pop_back();

    /// If deserialize state is provided, enumerate streams for dynamic paths.
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

namespace
{

/// Return sub-path by specified prefix.
/// For example, for prefix a.b:
/// a.b.c.d -> c.d, a.b.c -> c
String getSubPath(const String & path, const String & prefix)
{
    return path.substr(prefix.size() + 1);
}

std::string_view getSubPath(const std::string_view & path, const String & prefix)
{
    return path.substr(prefix.size() + 1);
}

}

void SerializationSubObject::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto structure_state = SerializationObject::deserializeObjectStructureStatePrefix(settings, cache);
    if (!structure_state)
        return;

    auto sub_object_state = std::make_shared<DeserializeBinaryBulkStateSubObject>();
    settings.path.push_back(Substream::ObjectData);
    for (const auto & [path, serialization] : typed_paths_serializations)
    {
        settings.path.push_back(Substream::ObjectTypedPath);
        settings.path.back().object_path_name = path;
        serialization->deserializeBinaryBulkStatePrefix(settings, sub_object_state->typed_path_states[path], cache);
        settings.path.pop_back();
    }

    for (const auto & dynamic_path : checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(structure_state)->sorted_dynamic_paths)
    {
        /// Save only dynamic paths with requested prefix.
        if (dynamic_path.starts_with(path_prefix) && dynamic_path.size() != path_prefix.size())
        {
            settings.path.push_back(Substream::ObjectDynamicPath);
            settings.path.back().object_path_name = dynamic_path;
            dynamic_serialization->deserializeBinaryBulkStatePrefix(settings, sub_object_state->dynamic_path_states[dynamic_path], cache);
            settings.path.pop_back();
            sub_object_state->dynamic_paths.push_back(dynamic_path);
            sub_object_state->dynamic_sub_paths.push_back(getSubPath(dynamic_path, path_prefix));
        }
    }

    settings.path.push_back(Substream::ObjectSharedData);
    shared_data_serialization->deserializeBinaryBulkStatePrefix(settings, sub_object_state->shared_data_state, cache);
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
        serialization->deserializeBinaryBulkWithMultipleStreams(typed_paths[getSubPath(path, path_prefix)], limit, settings, sub_object_state->typed_path_states[path], cache);
        settings.path.pop_back();
    }

    for (const auto & path : sub_object_state->dynamic_paths)
    {
        settings.path.push_back(Substream::ObjectDynamicPath);
        settings.path.back().object_path_name = path;
        dynamic_serialization->deserializeBinaryBulkWithMultipleStreams(dynamic_paths[getSubPath(path, path_prefix)], limit, settings, sub_object_state->dynamic_path_states[path], cache);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::ObjectSharedData);
    /// If it's a new object column, reinitialize column for shared data.
    if (result_column->empty())
        sub_object_state->shared_data = SerializationObject::getTypeOfSharedData()->createColumn();
    size_t prev_size = column_object.size();
    shared_data_serialization->deserializeBinaryBulkWithMultipleStreams(sub_object_state->shared_data, limit, settings, sub_object_state->shared_data_state, cache);
    settings.path.pop_back();

    auto & sub_object_shared_data = column_object.getSharedDataColumn();
    const auto & offsets = assert_cast<const ColumnArray &>(*sub_object_state->shared_data).getOffsets();
    /// Check if there is no data in shared data in current range.
    if (offsets.back() == offsets[ssize_t(prev_size) - 1])
    {
        sub_object_shared_data.insertManyDefaults(limit);
    }
    else
    {
        const auto & shared_data_array = assert_cast<const ColumnArray &>(*sub_object_state->shared_data);
        const auto & shared_data_offsets = shared_data_array.getOffsets();
        const auto & shared_data_tuple = assert_cast<const ColumnTuple &>(shared_data_array.getData());
        const auto & shared_data_paths = assert_cast<const ColumnString &>(shared_data_tuple.getColumn(0));
        const auto & shared_data_values = assert_cast<const ColumnString &>(shared_data_tuple.getColumn(1));

        auto & sub_object_data_offsets = column_object.getSharedDataOffsets();
        auto [sub_object_shared_data_paths, sub_object_shared_data_values] = column_object.getSharedDataPathsAndValues();
        StringRef prefix_ref(path_prefix);
        for (size_t i = prev_size; i != shared_data_offsets.size(); ++i)
        {
            size_t start = shared_data_offsets[ssize_t(i) - 1];
            size_t end = shared_data_offsets[ssize_t(i)];
            size_t lower_bound_index = ColumnObject::findPathLowerBoundInSharedData(prefix_ref, shared_data_paths, start, end);
            for (; lower_bound_index != end; ++lower_bound_index)
            {
                auto path = shared_data_paths.getDataAt(lower_bound_index).toView();
                if (!path.starts_with(path_prefix))
                    break;

                /// Don't include path that is equal to the prefix.
                if (path.size() != path_prefix.size())
                {
                    auto sub_path = getSubPath(path, path_prefix);
                    sub_object_shared_data_paths->insertData(sub_path.data(), sub_path.size());
                    sub_object_shared_data_values->insertFrom(shared_data_values, lower_bound_index);
                }
            }
            sub_object_data_offsets.push_back(sub_object_shared_data_paths->size());
        }
    }
    settings.path.pop_back();
}

}
