#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectDynamicPath.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectDynamicPath::SerializationObjectDynamicPath(
    const DB::SerializationPtr & nested_, const String & path_, const String & path_subcolumn_, size_t max_dynamic_types_)
    : SerializationWrapper(nested_)
    , path(path_)
    , path_subcolumn(path_subcolumn_)
    , dynamic_serialization(std::make_shared<SerializationDynamic>())
    , shared_data_serialization(SerializationObject::getTypeOfSharedData()->getDefaultSerialization())
    , max_dynamic_types(max_dynamic_types_)
{
}

struct DeserializeBinaryBulkStateObjectDynamicPath : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr structure_state;
    ISerialization::DeserializeBinaryBulkStatePtr nested_state;
    bool read_from_shared_data;
    ColumnPtr shared_data;
};

void SerializationObjectDynamicPath::enumerateStreams(
    DB::ISerialization::EnumerateStreamsSettings & settings,
    const DB::ISerialization::StreamCallback & callback,
    const DB::ISerialization::SubstreamData & data) const
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
        auto shared_data_substream_data = SubstreamData(shared_data_serialization)
                                              .withType(data.type ? SerializationObject::getTypeOfSharedData() : nullptr)
                                              .withColumn(data.column ? SerializationObject::getTypeOfSharedData()->createColumn() : nullptr)
                                              .withSerializationInfo(data.serialization_info)
                                              .withDeserializeState(deserialize_state->nested_state);
        settings.path.back().data = shared_data_substream_data;
        shared_data_serialization->enumerateStreams(settings, callback, shared_data_substream_data);
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
    dynamic_path_state->read_from_shared_data = !checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(dynamic_path_state->structure_state)->dynamic_paths.contains(path);
    settings.path.push_back(Substream::ObjectData);
    if (dynamic_path_state->read_from_shared_data)
    {
        settings.path.push_back(Substream::ObjectSharedData);
        shared_data_serialization->deserializeBinaryBulkStatePrefix(settings, dynamic_path_state->nested_state, cache);
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
        nested_serialization->deserializeBinaryBulkWithMultipleStreams(result_column, limit, settings, dynamic_path_state->nested_state, cache);
        settings.path.pop_back();
    }
    /// Otherwise, read the whole shared data column and extract requested path from it.
    /// TODO: We can read several subcolumns of the same path located in the shared data
    ///       and right now we extract the whole path column from shared data every time
    ///       and then extract the requested subcolumns. We can optimize it and use substreams
    ///       cache here to avoid extracting the same path from shared data several times.
    ///
    /// TODO: We can change the serialization of shared data to optimize reading paths from it.
    ///       Right now we cannot know if shared data contains our path in current range or not,
    ///       but we can change the serialization and write the list of all paths stored in shared
    ///       data before each granule, and then replace the column that stores paths with column
    ///       with indexes in this list. It can also reduce the storage, because we will store
    ///       each path only once and can replace UInt64 string offset column with indexes column
    ///       that can have smaller type depending on the number of paths in the list.
    else
    {
        settings.path.push_back(Substream::ObjectSharedData);
        /// Initialize shared_data column if needed.
        if (result_column->empty())
            dynamic_path_state->shared_data = SerializationObject::getTypeOfSharedData()->createColumn();
        size_t prev_size = result_column->size();
        shared_data_serialization->deserializeBinaryBulkWithMultipleStreams(dynamic_path_state->shared_data, limit, settings, dynamic_path_state->nested_state, cache);
        /// If we need to read a subcolumn from Dynamic column, create an empty Dynamic column, fill it and extract subcolumn.
        MutableColumnPtr dynamic_column = path_subcolumn.empty() ? result_column->assumeMutable() : ColumnDynamic::create(max_dynamic_types)->getPtr();
        /// Check if we don't have any paths in shared data in current range.
        const auto & offsets = assert_cast<const ColumnArray &>(*dynamic_path_state->shared_data).getOffsets();
        if (offsets.back() == offsets[ssize_t(prev_size) - 1])
            dynamic_column->insertManyDefaults(limit);
        else
            ColumnObject::fillPathColumnFromSharedData(*dynamic_column, path, dynamic_path_state->shared_data, prev_size, dynamic_path_state->shared_data->size());

        /// Extract subcolumn from Dynamic column if needed.
        if (!path_subcolumn.empty())
        {
            auto subcolumn = std::make_shared<DataTypeDynamic>(max_dynamic_types)->getSubcolumn(path_subcolumn, dynamic_column->getPtr());
            result_column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }

        settings.path.pop_back();
    }

    settings.path.pop_back();
}

}
