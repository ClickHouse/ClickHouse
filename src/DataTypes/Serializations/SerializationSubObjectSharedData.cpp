#include <DataTypes/Serializations/SerializationSubObjectSharedData.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/DataTypeObject.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnObject.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationSubObjectSharedData::SerializationSubObjectSharedData(
    SerializationObjectSharedData::SerializationVersion serialization_version_,
    size_t buckets_,
    const String & paths_prefix_,
    const DataTypePtr & dynamic_type_)
    : serialization_version(serialization_version_)
    , buckets(buckets_)
    , paths_prefix(paths_prefix_)
    , dynamic_type(dynamic_type_)
    , dynamic_serialization(dynamic_type->getDefaultSerialization())
    , serialization_map(DataTypeObject::getTypeOfSharedData()->getDefaultSerialization())
{
}

struct DeserializeBinaryBulkStateSubObjectSharedData : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr map_state;
    ColumnPtr map_column;

    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_map_states;
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_structure_states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateSubObjectSharedData>(*this);
        for (size_t bucket = 0; bucket != bucket_map_states.size(); ++bucket)
            new_state->bucket_map_states[bucket] = bucket_map_states[bucket] ? bucket_map_states[bucket]->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_structure_states.size(); ++bucket)
            new_state->bucket_structure_states[bucket] = bucket_structure_states[bucket] ? bucket_structure_states[bucket]->clone() : nullptr;
        return new_state;
    }
};

void SerializationSubObjectSharedData::enumerateStreams(
    ISerialization::EnumerateStreamsSettings & settings,
    const ISerialization::StreamCallback & callback,
    const ISerialization::SubstreamData & data) const
{
    const auto * sub_object_shared_data_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateSubObjectSharedData>(data.deserialize_state) : nullptr;

    if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP)
    {
        auto map_data = SubstreamData(serialization_map)
                            .withColumn(data.column)
                            .withType(data.type)
                            .withSerializationInfo(data.serialization_info)
                            .withDeserializeState(sub_object_shared_data_state ? sub_object_shared_data_state->map_state : nullptr);
        serialization_map->enumerateStreams(settings, callback, map_data);
        return;
    }

    /// Other 2 serializations MAP_WITH_BUCKETS and ADVAMCED support buckets.
    /// Paths with our prefix can be in any bucket, so we will need to check them all.
    for (size_t bucket = 0; bucket != buckets; ++bucket)
    {
        settings.path.push_back(Substream::ObjectSharedDataBucket);
        settings.path.back().object_shared_data_bucket = bucket;
        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
        {
            auto map_data = SubstreamData(serialization_map)
                                .withColumn(data.column)
                                .withType(data.type)
                                .withSerializationInfo(data.serialization_info)
                                .withDeserializeState(sub_object_shared_data_state ? sub_object_shared_data_state->bucket_map_states[bucket] : nullptr);
            serialization_map->enumerateStreams(settings, callback, map_data);
        }
        else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED)
        {
            if (settings.use_specialized_prefixes_and_suffixes_substreams)
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructurePrefix);
            else
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructure);

            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataData);
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataPathsMarks);
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataSubstreams);
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataSubstreamsMarks);
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataPathsSubstreamsMetadata);

            if (settings.use_specialized_prefixes_and_suffixes_substreams)
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructureSuffix);
        }
        else
        {
            /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "enumerateStreams is not implemented for shared data serialization version {}", serialization_version.value);
        }

        settings.path.pop_back();
    }
}

void SerializationSubObjectSharedData::serializeBinaryBulkStatePrefix(
    const IColumn &, ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationSubObjectSharedData");
}

void SerializationSubObjectSharedData::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationSubObjectSharedData");
}

void SerializationSubObjectSharedData::serializeBinaryBulkStateSuffix(
    ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationSubObjectSharedData");
}

void SerializationSubObjectSharedData::deserializeBinaryBulkStatePrefix(
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    ISerialization::SubstreamsDeserializeStatesCache * cache) const
{
    auto shared_data_state = std::make_shared<DeserializeBinaryBulkStateSubObjectSharedData>();

    if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP)
    {
        serialization_map->deserializeBinaryBulkStatePrefix(settings, shared_data_state->map_state, cache);
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
    {
        shared_data_state->bucket_map_states.resize(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            serialization_map->deserializeBinaryBulkStatePrefix(settings, shared_data_state->bucket_map_states[bucket], cache);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED)
    {
        shared_data_state->bucket_structure_states.resize(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            shared_data_state->bucket_structure_states[bucket] = SerializationObjectSharedData::deserializeStructureStatePrefix(settings, cache);
            auto * structure_state_concrete = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_state->bucket_structure_states[bucket]);
            structure_state_concrete->requested_paths_prefixes.push_back(paths_prefix);
            /// All paths that matches the prefix will be read, so if we requested subcolumns of such paths, remove them from
            /// requested_paths_subcolumns so we don't deserialize them but extract from path data in memory.
            std::erase_if(structure_state_concrete->requested_paths_subcolumns, [&](const auto & pair) { return pair.first.starts_with(paths_prefix); });
            settings.path.pop_back();
        }
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "deserializeBinaryBulkStatePrefix is not implemented for shared data serialization version {}", serialization_version.value);
    }

    state = std::move(shared_data_state);
}

void SerializationSubObjectSharedData::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    ISerialization::SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * sub_object_shared_data_state = checkAndGetState<DeserializeBinaryBulkStateSubObjectSharedData>(state);

    if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP)
    {
        /// Initialize map column if needed.
        if (column->empty() || !sub_object_shared_data_state->map_column)
            sub_object_shared_data_state->map_column = DataTypeObject::getTypeOfSharedData()->createColumn();

        /// Read shared data map column.
        size_t num_read_rows = 0;
        /// Check if we have map column in cache.
        if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
        {
            std::tie(sub_object_shared_data_state->map_column, num_read_rows) = *cached_column_with_num_read_rows;
        }
        /// If we don't have it in cache, deserialize and put deserialized map in cache.
        else
        {
            size_t prev_size = sub_object_shared_data_state->map_column->size();
            serialization_map->deserializeBinaryBulkWithMultipleStreams(sub_object_shared_data_state->map_column, rows_offset, limit, settings, sub_object_shared_data_state->map_state, cache);
            num_read_rows = sub_object_shared_data_state->map_column->size() - prev_size;
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, sub_object_shared_data_state->map_column, num_read_rows);
        }

        size_t map_column_offset = sub_object_shared_data_state->map_column->size() - num_read_rows;

        /// Check if we don't have any paths in shared data in current range.
        const auto & offsets = assert_cast<const ColumnArray &>(*sub_object_shared_data_state->map_column).getOffsets();
        if (offsets.back() == offsets[ssize_t(map_column_offset) - 1])
        {
            column->assumeMutable()->insertManyDefaults(limit);
        }
        /// Iterate over new rows in map column and extract paths with requested prefix.
        else
        {
            auto [src_shared_data_paths, src_shared_data_values, src_shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(*sub_object_shared_data_state->map_column);
            auto [dst_shared_data_paths, dst_shared_data_values, dst_shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(*column->assumeMutable());
            StringRef prefix_ref(paths_prefix);
            for (size_t i = map_column_offset; i != sub_object_shared_data_state->map_column->size(); ++i)
            {
                size_t start = (*src_shared_data_offsets)[ssize_t(i) - 1];
                size_t end = (*src_shared_data_offsets)[ssize_t(i)];
                /// Paths are sorted inside each row in shared data map column.
                size_t lower_bound_index = ColumnObject::findPathLowerBoundInSharedData(prefix_ref, *src_shared_data_paths, start, end);
                for (; lower_bound_index != end; ++lower_bound_index)
                {
                    auto path = src_shared_data_paths->getDataAt(lower_bound_index).toView();
                    if (!path.starts_with(paths_prefix))
                        break;

                    auto sub_path = path.substr(paths_prefix.size());
                    dst_shared_data_paths->insertData(sub_path.data(), sub_path.size());
                    dst_shared_data_values->insertFrom(*src_shared_data_values, lower_bound_index);
                }
                dst_shared_data_offsets->push_back(dst_shared_data_paths->size());
            }
        }
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
    {
        std::vector<ColumnPtr> bucket_map_columns(buckets);
        /// Read shared data map column from each bucket.
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;

            /// Check if we have map column in cache.
            if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
            {
                bucket_map_columns[bucket] = cached_column_with_num_read_rows->first;
            }
            /// If we don't have it in cache, deserialize and put deserialized map in cache.
            else
            {
                bucket_map_columns[bucket] = DataTypeObject::getTypeOfSharedData()->createColumn();
                serialization_map->deserializeBinaryBulkWithMultipleStreams(bucket_map_columns[bucket], rows_offset, limit, settings, sub_object_shared_data_state->bucket_map_states[bucket], cache);
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, bucket_map_columns[bucket], bucket_map_columns[bucket]->size());
            }

            settings.path.pop_back();
        }

        /// Now we have map column from each bucket and can collect all paths with specified prefix from them.
        collectSharedDataFromBuckets(bucket_map_columns, *column->assumeMutable(), &paths_prefix);
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED)
    {
        std::vector<std::shared_ptr<SerializationObjectSharedData::PathsDataGranules>> bucket_paths_data_granules(buckets);
        /// We need to remember offset and limit from each granule to know which rows to insert in the result.
        std::vector<std::pair<size_t, size_t>> granules_offset_and_limit;
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;

            auto * shared_data_structure_state = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(sub_object_shared_data_state->bucket_structure_states[bucket]);
            auto structure_granules = SerializationObjectSharedData::deserializeStructure(rows_offset, limit, settings, *shared_data_structure_state, cache);
            auto paths_infos_granules = SerializationObjectSharedData::deserializePathsInfos(*structure_granules, *shared_data_structure_state, settings, cache);
            bucket_paths_data_granules[bucket] = SerializationObjectSharedData::deserializePathsData(*structure_granules, *paths_infos_granules, *shared_data_structure_state, settings, dynamic_type, dynamic_serialization, cache);

            /// Init offset and limit for each granule
            if (bucket == 0)
            {
                granules_offset_and_limit.reserve(structure_granules->size());
                for (size_t granule = 0; granule != structure_granules->size(); ++granule)
                    granules_offset_and_limit.emplace_back((*structure_granules)[granule].offset, (*structure_granules)[granule].limit);
            }
            settings.path.pop_back();
        }

        auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(*column->assumeMutable());
        for (size_t granule = 0; granule != bucket_paths_data_granules[0]->size(); ++granule)
        {
            /// Collect list of all paths that match prefix from all buckets in this granule.
            std::vector<std::pair<String, const ColumnDynamic *>> all_paths;
            for (const auto & paths_data_granules : bucket_paths_data_granules)
            {
                const auto & paths_data = (*paths_data_granules)[granule].paths_data;
                for (const auto & [path, path_column] : paths_data)
                {
                    if (path.starts_with(paths_prefix))
                    {
                        auto sub_path = path.substr(paths_prefix.size());
                        all_paths.emplace_back(sub_path, assert_cast<const ColumnDynamic *>(path_column.get()));
                    }
                }
            }

            /// Paths in shared data column are stored in sorted order.
            std::sort(all_paths.begin(), all_paths.end());
            auto [granule_offset, granule_limit] = granules_offset_and_limit[granule];
            size_t granule_end = granule_offset + granule_limit;
            for (size_t i = granule_offset; i != granule_end; ++i)
            {
                for (const auto & [path, path_column] : all_paths)
                    ColumnObject::serializePathAndValueIntoSharedData(shared_data_paths, shared_data_values, path, *path_column, i);
                shared_data_offsets->push_back(shared_data_paths->size());
            }
        }
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "deserializeBinaryBulkWithMultipleStreams is not implemented for shared data serialization version {}", serialization_version.value);
    }
}

}
