#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObjectSharedDataPath.h>
#include <DataTypes/Serializations/getSubcolumnsDeserializationOrder.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnObject.h>
#include <Common/logger_useful.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

SerializationObjectSharedDataPath::SerializationObjectSharedDataPath(
    const SerializationPtr & nested_,
    SerializationObjectSharedData::SerializationVersion serialization_version_,
    const String & path_,
    const String & path_subcolumn_,
    const DataTypePtr & dynamic_type_,
    const DataTypePtr & subcolumn_type_,
    size_t bucket_)
    : SerializationWrapper(nested_)
    , serialization_version(serialization_version_)
    , serialization_map(DataTypeObject::getTypeOfSharedData()->getDefaultSerialization())
    , path(path_)
    , path_subcolumn(path_subcolumn_)
    , dynamic_type(dynamic_type_)
    , subcolumn_type(subcolumn_type_)
    , dynamic_serialization(dynamic_type_->getDefaultSerialization())
    , bucket(bucket_)
{
}

struct DeserializeBinaryBulkStateObjectSharedDataPath : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr map_state;
    ColumnPtr map_column;

    ISerialization::DeserializeBinaryBulkStatePtr structure_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectSharedDataPath>(*this);
        new_state->map_state = map_state ? map_state->clone() : nullptr;
        new_state->structure_state = structure_state ? structure_state->clone() : nullptr;
        return new_state;
    }
};

void SerializationObjectSharedDataPath::enumerateStreams(
    ISerialization::EnumerateStreamsSettings & settings,
    const ISerialization::StreamCallback & callback,
    const ISerialization::SubstreamData & data) const
{
    if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP || serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
    {
        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
        }

        const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataPath>(data.deserialize_state) : nullptr;
        auto map_data = SubstreamData(serialization_map)
                            .withType(data.type ? DataTypeObject::getTypeOfSharedData() : nullptr)
                            .withColumn(data.column ? DataTypeObject::getTypeOfSharedData()->createColumn() : nullptr)
                            .withSerializationInfo(data.serialization_info)
                            .withDeserializeState(deserialize_state ? deserialize_state->map_state : nullptr);
        serialization_map->enumerateStreams(settings, callback, map_data);

        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
            settings.path.pop_back();
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED)
    {
        settings.path.push_back(Substream::ObjectSharedDataBucket);
        settings.path.back().object_shared_data_bucket = bucket;

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

        settings.path.pop_back();
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "enumerateStreams is not implemented for shared data serialization version {}", serialization_version.value);
    }
}

void SerializationObjectSharedDataPath::serializeBinaryBulkStatePrefix(
    const IColumn &, ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStatePrefix is not implemented for SerializationObjectSharedDataPath");
}

void SerializationObjectSharedDataPath::serializeBinaryBulkWithMultipleStreams(
    const IColumn &, size_t, size_t, ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Method serializeBinaryBulkWithMultipleStreams is not implemented for SerializationObjectSharedDataPath");
}

void SerializationObjectSharedDataPath::serializeBinaryBulkStateSuffix(
    ISerialization::SerializeBinaryBulkSettings &, ISerialization::SerializeBinaryBulkStatePtr &) const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED, "Method serializeBinaryBulkStateSuffix is not implemented for SerializationObjectSharedDataPath");
}

void SerializationObjectSharedDataPath::deserializeBinaryBulkStatePrefix(
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    ISerialization::SubstreamsDeserializeStatesCache * cache) const
{
    auto shared_data_path_state = std::make_shared<DeserializeBinaryBulkStateObjectSharedDataPath>();

    if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP || serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
    {
        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
        }

        serialization_map->deserializeBinaryBulkStatePrefix(settings, shared_data_path_state->map_state, cache);

        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
            settings.path.pop_back();
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED)
    {
        settings.path.push_back(Substream::ObjectSharedDataBucket);
        settings.path.back().object_shared_data_bucket = bucket;

        shared_data_path_state->structure_state = SerializationObjectSharedData::deserializeStructureStatePrefix(settings, cache);
        auto * structure_state_concrete = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_path_state->structure_state);
        if (structure_state_concrete->requested_paths.contains(path))
        {
            /// We already requested the whole path, even if here we read only subcolumn,
            /// we will just extract it in memory from whole path column.
        }
        /// If no subcolumn requested or path matches any requested prefixes, request the whole path.
        else if (path_subcolumn.empty() || structure_state_concrete->checkIfPathMatchesAnyRequestedPrefix(path))
        {
            structure_state_concrete->requested_paths.insert(path);
            /// Remove all subcolumns of this path if any. We will read the whole path and extract all subcolumns in memory.
            structure_state_concrete->requested_paths_subcolumns.erase(path);
        }
        /// Otherwise request only subcolumn of this path.
        else
        {
            structure_state_concrete->requested_paths_subcolumns[path].emplace_back(path_subcolumn, subcolumn_type, nested_serialization);
        }

        settings.path.pop_back();
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "enumerateStreams is not implemented for shared data serialization version {}", serialization_version.value);
    }

    state = std::move(shared_data_path_state);
}

void SerializationObjectSharedDataPath::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    ISerialization::SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * shared_data_path_state = checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataPath>(state);

    if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP || serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
    {
        ColumnPtr map_column;
        size_t num_read_rows = 0;
        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP)
        {
            /// Initialize map column if needed.
            if (column->empty() || !shared_data_path_state->map_column)
                shared_data_path_state->map_column = DataTypeObject::getTypeOfSharedData()->createColumn();

            if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
            {
                std::tie(shared_data_path_state->map_column, num_read_rows) = *cached_column_with_num_read_rows;
            }
            /// If we don't have it in cache, deserialize and put deserialized map in cache.
            else
            {
                size_t prev_size = shared_data_path_state->map_column->size();
                serialization_map->deserializeBinaryBulkWithMultipleStreams(shared_data_path_state->map_column, rows_offset, limit, settings, shared_data_path_state->map_state, cache);
                num_read_rows = shared_data_path_state->map_column->size() - prev_size;
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, shared_data_path_state->map_column, num_read_rows);
            }

            map_column = shared_data_path_state->map_column;
        }
        else
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;

            if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
            {
                map_column = cached_column_with_num_read_rows->first;
            }
            /// If we don't have it in cache, deserialize and put deserialized map in cache.
            else
            {
                /// Don't use shared_data_path_state->map_column here, because map column from bucket is never present in the result columns,
                /// so we don't need to preserve old rows to keep valid usage of substreams cache (when column in cache is also present in the result columns).
                /// Here we can store only rows from current deserialization even in cache.
                map_column = DataTypeObject::getTypeOfSharedData()->createColumn();
                serialization_map->deserializeBinaryBulkWithMultipleStreams(map_column, rows_offset, limit, settings, shared_data_path_state->map_state, cache);
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, map_column, map_column->size());
            }

            num_read_rows = map_column->size();
            settings.path.pop_back();
        }

        size_t map_column_offset = map_column->size() - num_read_rows;

        /// If we need to read a subcolumn from Dynamic column, create an empty Dynamic column, fill it and extract subcolumn.
        MutableColumnPtr dynamic_column = path_subcolumn.empty() ? column->assumeMutable() : dynamic_type->createColumn();
        /// Check if we don't have any paths in shared data in current range.
        const auto & offsets = assert_cast<const ColumnArray &>(*map_column).getOffsets();
        if (offsets.back() == offsets[ssize_t(map_column_offset) - 1])
            dynamic_column->insertManyDefaults(limit);
        else
            ColumnObject::fillPathColumnFromSharedData(*dynamic_column, path, map_column, map_column_offset, map_column->size());

        /// Extract subcolumn from Dynamic column if needed.
        if (!path_subcolumn.empty())
        {
            auto subcolumn = dynamic_type->getSubcolumn(path_subcolumn, dynamic_column->getPtr());
            column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED)
    {
        settings.path.push_back(Substream::ObjectSharedDataBucket);
        settings.path.back().object_shared_data_bucket = bucket;

        auto * shared_data_structure_state = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_path_state->structure_state);
        auto structure_granules = SerializationObjectSharedData::deserializeStructure(rows_offset, limit, settings, *shared_data_structure_state, cache);
        auto paths_infos_granules = SerializationObjectSharedData::deserializePathsInfos(*structure_granules, *shared_data_structure_state, settings, cache);
        auto paths_data_granules = SerializationObjectSharedData::deserializePathsData(*structure_granules, *paths_infos_granules, *shared_data_structure_state, settings, dynamic_type, dynamic_serialization, cache);

        for (size_t granule = 0; granule != structure_granules->size(); ++granule)
        {
            const auto & structure_granule = (*structure_granules)[granule];
            const auto & paths_info_granule = (*paths_infos_granules)[granule];
            const auto & paths_data_granule = (*paths_data_granules)[granule];

            /// Skip granule if there is nothing to read from it.
            if (!structure_granule.limit)
                continue;

            /// Skip granule if it doesn't have requested path.
            if (!paths_info_granule.path_to_info.contains(path))
            {
                column->assumeMutable()->insertManyDefaults(structure_granule.limit);
                continue;
            }

            auto path_data_it = paths_data_granule.paths_data.find(path);
            /// Check if we have data of the whole path.
            if (path_data_it != paths_data_granule.paths_data.end())
            {
                /// If no subcolumn is requested, just insert path data into destination column.
                if (path_subcolumn.empty())
                {
                    column->assumeMutable()->insertRangeFrom(*path_data_it->second, structure_granule.offset, structure_granule.limit);
                }
                /// If subcolumn is requested, extract it from the path data.
                else
                {
                    auto subcolumn = dynamic_type->getSubcolumn(path_subcolumn, path_data_it->second);
                    column->assumeMutable()->insertRangeFrom(*subcolumn, structure_granule.offset, structure_granule.limit);
                }
            }
            /// If no subcolumn is requested we must have path data.
            else if (path_subcolumn.empty())
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Data of path {} is not deserialized", path);
            }
            /// Otherwise we must have subcolumn in paths subcolumns data.
            else
            {
                auto path_subcolumns_data_it = paths_data_granule.paths_subcolumns_data.find(path);
                if (path_subcolumns_data_it == paths_data_granule.paths_subcolumns_data.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Subcolumns data of path {} is not deserialized", path);

                auto subcolumn_data_it = path_subcolumns_data_it->second.find(path_subcolumn);
                if (subcolumn_data_it == path_subcolumns_data_it->second.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Data of subcolumn {} of path {} is not deserialized", path_subcolumn, path);

                column->assumeMutable()->insertRangeFrom(*subcolumn_data_it->second, structure_granule.offset, structure_granule.limit);
            }
        }

        settings.path.pop_back();
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "enumerateStreams is not implemented for shared data serialization version {}", serialization_version.value);
    }
}

}
