#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObject.h>
#include <DataTypes/Serializations/SerializationObjectDistinctPaths.h>
#include <DataTypes/Serializations/SerializationObjectSharedData.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectDistinctPaths::SerializationObjectDistinctPaths(const std::vector<String> & typed_paths_)
    : typed_paths(typed_paths_)
{
    const auto & shared_data_type = DataTypeObject::getTypeOfSharedData();
    shared_data_paths_serialization = shared_data_type->getSubcolumnSerialization("paths", shared_data_type->getDefaultSerialization());
}

struct DeserializeBinaryBulkStateObjectDistinctPaths : public ISerialization::DeserializeBinaryBulkState
{
    /// State of the whole Object column structure.
    ISerialization::DeserializeBinaryBulkStatePtr object_structure_state;
    /// State of the shared data in MAP serialization.
    ISerialization::DeserializeBinaryBulkStatePtr shared_data_paths_state;
    /// States of the bucketed shared data in MAP_WITH_BUCKETS serialization.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_shared_data_paths_state;
    /// States of the bucketed shared data in ADVANCED serialization.
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_shared_data_structure_states;

    /// We want to insert dynamic and typed paths only once per deserialization.
    bool insert_dynamic_and_typed_paths = true;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectDistinctPaths>(*this);
        new_state->object_structure_state = object_structure_state ? object_structure_state->clone() : nullptr;
        new_state->shared_data_paths_state = shared_data_paths_state ? shared_data_paths_state->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_shared_data_paths_state.size(); ++bucket)
            new_state->bucket_shared_data_paths_state[bucket] = bucket_shared_data_paths_state[bucket] ? bucket_shared_data_paths_state[bucket]->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_shared_data_structure_states.size(); ++bucket)
            new_state->bucket_shared_data_structure_states[bucket] = bucket_shared_data_structure_states[bucket] ? bucket_shared_data_structure_states[bucket]->clone() : nullptr;
        return new_state;
    }
};

void SerializationObjectDistinctPaths::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    settings.path.push_back(Substream::ObjectStructure);
    callback(settings.path);
    settings.path.pop_back();

    if (!data.deserialize_state)
        return;

    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectSharedData);

    const auto * deserialize_state = checkAndGetState<DeserializeBinaryBulkStateObjectDistinctPaths>(data.deserialize_state);
    const auto * object_structure_state = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(deserialize_state->object_structure_state);
    switch (object_structure_state->shared_data_serialization_version.value)
    {
        case SerializationObjectSharedData::SerializationVersion::MAP:
        {
            auto shared_data_paths_data = data;
            shared_data_paths_data.deserialize_state = deserialize_state->shared_data_paths_state;
            shared_data_paths_serialization->enumerateStreams(settings, callback, shared_data_paths_data);
            break;
        }
        case SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS:
        {
            for (size_t bucket = 0; bucket < object_structure_state->shared_data_buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;
                auto shared_data_paths_data = data;
                shared_data_paths_data.deserialize_state = deserialize_state->bucket_shared_data_paths_state[bucket];
                shared_data_paths_serialization->enumerateStreams(settings, callback, shared_data_paths_data);
                settings.path.pop_back();
            }
            break;
        }
        case SerializationObjectSharedData::SerializationVersion::ADVANCED:
        {
            for (size_t bucket = 0; bucket < object_structure_state->shared_data_buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;

                if (settings.use_specialized_prefixes_and_suffixes_substreams)
                {
                    addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructurePrefix);
                    addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructureSuffix);
                }
                else
                {
                    addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructure);
                }

                settings.path.pop_back();
            }
            break;
        }
    }

    settings.path.pop_back();
    settings.path.pop_back();
}

void SerializationObjectDistinctPaths::serializeBinaryBulkStatePrefix(
    const IColumn &, ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectDistinctPaths");
}

void SerializationObjectDistinctPaths::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectDistinctPaths");
}

void SerializationObjectDistinctPaths::serializeBinaryBulkStateSuffix(
    ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectDistinctPaths");
}


void SerializationObjectDistinctPaths::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto object_distinct_paths_state = std::make_shared<DeserializeBinaryBulkStateObjectDistinctPaths>();
    object_distinct_paths_state->object_structure_state = SerializationObject::deserializeObjectStructureStatePrefix(settings, cache);
    if (!object_distinct_paths_state->object_structure_state)
        return;

    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectSharedData);

    const auto * object_structure_state = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(object_distinct_paths_state->object_structure_state);
    switch (object_structure_state->shared_data_serialization_version.value)
    {
        case SerializationObjectSharedData::SerializationVersion::MAP:
        {
            shared_data_paths_serialization->deserializeBinaryBulkStatePrefix(settings, object_distinct_paths_state->shared_data_paths_state, cache);
            break;
        }
        case SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS:
        {
            object_distinct_paths_state->bucket_shared_data_paths_state.resize(object_structure_state->shared_data_buckets);
            for (size_t bucket = 0; bucket != object_structure_state->shared_data_buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;
                shared_data_paths_serialization->deserializeBinaryBulkStatePrefix(settings, object_distinct_paths_state->bucket_shared_data_paths_state[bucket], cache);
                settings.path.pop_back();
            }
            break;
        }
        case SerializationObjectSharedData::SerializationVersion::ADVANCED:
        {
            object_distinct_paths_state->bucket_shared_data_structure_states.resize(object_structure_state->shared_data_buckets);
            for (size_t bucket = 0; bucket != object_structure_state->shared_data_buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;
                object_distinct_paths_state->bucket_shared_data_structure_states[bucket] = SerializationObjectSharedData::deserializeStructureStatePrefix(settings, cache);
                auto * shared_data_structure_state_concrete = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(object_distinct_paths_state->bucket_shared_data_structure_states[bucket]);
                /// Specify that we need the list of all paths.
                shared_data_structure_state_concrete->need_all_paths = true;
                settings.path.pop_back();
            }
            break;
        }
    }

    settings.path.pop_back();
    settings.path.pop_back();
    state = std::move(object_distinct_paths_state);
}

void SerializationObjectDistinctPaths::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    if (!state)
        return;

    if (rows_offset + limit == 0)
        return;

    auto & array_column = assert_cast<ColumnArray &>(*column->assumeMutable());
    auto & paths_column = assert_cast<ColumnString &>(array_column.getData());
    auto * object_distinct_paths_state = checkAndGetState<DeserializeBinaryBulkStateObjectDistinctPaths>(state);
    auto * object_structure_state = checkAndGetState<SerializationObject::DeserializeBinaryBulkStateObjectStructure>(object_distinct_paths_state->object_structure_state);
    if (object_distinct_paths_state->insert_dynamic_and_typed_paths)
    {
        for (const auto & path : typed_paths)
            paths_column.insertData(path.data(), path.size());
        for (const auto & path : *object_structure_state->sorted_dynamic_paths)
            paths_column.insertData(path.data(), path.size());
        object_distinct_paths_state->insert_dynamic_and_typed_paths = false;
    }

    size_t num_new_rows = 0;
    settings.path.push_back(Substream::ObjectData);
    settings.path.push_back(Substream::ObjectSharedData);
    switch (object_structure_state->shared_data_serialization_version.value)
    {
        case SerializationObjectSharedData::SerializationVersion::MAP:
        {
            ColumnPtr shared_data_paths_column = column->cloneEmpty();
            auto settings_copy = settings;
            settings_copy.insert_only_rows_in_current_range_from_substreams_cache = true;
            shared_data_paths_serialization->deserializeBinaryBulkWithMultipleStreams(
                shared_data_paths_column,
                rows_offset,
                limit,
                settings_copy,
                object_distinct_paths_state->shared_data_paths_state,
                cache);

            const auto & shared_data_paths_array_column = assert_cast<const ColumnArray &>(*shared_data_paths_column);
            paths_column.insertRangeFrom(shared_data_paths_array_column.getData(), 0, shared_data_paths_array_column.getData().size());
            num_new_rows = shared_data_paths_column->size();
            break;
        }
        case SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS:
        {
            for (size_t bucket = 0; bucket < object_structure_state->shared_data_buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;
                ColumnPtr bucket_shared_data_paths_column = column->cloneEmpty();
                shared_data_paths_serialization->deserializeBinaryBulkWithMultipleStreams(
                    bucket_shared_data_paths_column,
                    rows_offset,
                    limit,
                    settings,
                    object_distinct_paths_state->bucket_shared_data_paths_state[bucket],
                    cache);
                settings.path.pop_back();

                const auto & shared_data_paths_array_column = assert_cast<const ColumnArray &>(*bucket_shared_data_paths_column);
                paths_column.insertRangeFrom(shared_data_paths_array_column.getData(), 0, shared_data_paths_array_column.getData().size());

                if (bucket == 0)
                    num_new_rows = bucket_shared_data_paths_column->size();
            }
            break;
        }
        case SerializationObjectSharedData::SerializationVersion::ADVANCED:
        {
            for (size_t bucket = 0; bucket < object_structure_state->shared_data_buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;

                auto * shared_data_structure_state = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(object_distinct_paths_state->bucket_shared_data_structure_states[bucket]);
                auto structure_granules = SerializationObjectSharedData::deserializeStructure(rows_offset, limit, settings, *shared_data_structure_state, cache);
                for (const auto & structure_granule : *structure_granules)
                {
                    for (const auto & path : structure_granule.all_paths)
                        paths_column.insertData(path.data(), path.size());

                    if (bucket == 0)
                        num_new_rows += structure_granule.limit;
                }
                settings.path.pop_back();
            }
            break;
        }
    }

    array_column.getOffsets().push_back(paths_column.size());
    array_column.insertManyDefaults(num_new_rows - 1);

    settings.path.pop_back();
    settings.path.pop_back();
}

}
