#include <Common/SipHash.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObjectSharedDataPath.h>
#include <DataTypes/Serializations/getSubcolumnsDeserializationOrder.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnVariant.h>
#include <Common/logger_useful.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// With `DEFAULT PATH TYPE T` shared data values have type `T`. Fills either a `T` column
/// (a missing path is `default(T)`) or a `Variant(T)` column (a missing path is NULL).
void fillDefaultPathTypeColumnFromSharedData(IColumn & path_column, std::string_view path, const IColumn & shared_data_column, size_t start, size_t end)
{
    auto * variant_column = typeid_cast<ColumnVariant *>(&path_column);
    const auto [shared_data_paths, shared_data_values, shared_data_offsets] = ColumnObject::getSharedDataPathsValuesAndOffsets(shared_data_column);
    for (size_t i = start; i != end; ++i)
    {
        size_t paths_start = (*shared_data_offsets)[ssize_t(i) - 1];
        size_t paths_end = (*shared_data_offsets)[ssize_t(i)];
        size_t index = ColumnObject::findPathLowerBoundInSharedData(path, *shared_data_paths, paths_start, paths_end);
        if (index == paths_end || shared_data_paths->getDataAt(index) != path)
            path_column.insertDefault();
        else if (variant_column)
            variant_column->insertIntoVariantFrom(0, *shared_data_values, index);
        else
            path_column.insertFrom(*shared_data_values, index);
    }
}

}

SerializationObjectSharedDataPath::SerializationObjectSharedDataPath(
    const SerializationPtr & nested_,
    SerializationObjectSharedData::SerializationVersion serialization_version_,
    const String & path_,
    const String & path_subcolumn_,
    const DataTypePtr & dynamic_type_,
    const SerializationPtr & dynamic_serialization_,
    const DataTypePtr & subcolumn_type_,
    size_t bucket_,
    const DataTypePtr & default_path_type_)
    : SerializationWrapper(nested_)
    , serialization_version(serialization_version_)
    , serialization_map(DataTypeObject::getTypeOfSharedData(default_path_type_)->getDefaultSerialization())
    , path(path_)
    , path_subcolumn(path_subcolumn_)
    , dynamic_type(dynamic_type_)
    , subcolumn_type(subcolumn_type_)
    , dynamic_serialization(dynamic_serialization_)
    , default_path_type(default_path_type_)
    , bucket(bucket_)
{
}

UInt128 SerializationObjectSharedDataPath::getHash(const SerializationPtr & nested_, SerializationObjectSharedData::SerializationVersion serialization_version_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, const DataTypePtr & subcolumn_type_, size_t bucket_, const DataTypePtr & default_path_type_)
{
    SipHash hash;
    hash.update("ObjectSharedDataPath");
    hash.update(nested_->getHash());
    hash.update(static_cast<int>(serialization_version_.value));
    hash.update(path_.size());
    hash.update(path_);
    hash.update(path_subcolumn_.size());
    hash.update(path_subcolumn_);
    auto dynamic_type_name = dynamic_type_->getName();
    hash.update(dynamic_type_name.size());
    hash.update(dynamic_type_name);
    hash.update(dynamic_serialization_->getHash());
    auto subcolumn_type_name = subcolumn_type_->getName();
    hash.update(subcolumn_type_name.size());
    hash.update(subcolumn_type_name);
    hash.update(bucket_);
    auto default_path_type_name = default_path_type_ ? default_path_type_->getName() : "";
    hash.update(default_path_type_name.size());
    hash.update(default_path_type_name);
    return hash.get128();
}

SerializationPtr SerializationObjectSharedDataPath::create(const SerializationPtr & nested_, SerializationObjectSharedData::SerializationVersion serialization_version_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, const DataTypePtr & subcolumn_type_, size_t bucket, const DataTypePtr & default_path_type_)
{
    if (!nested_->supportsPooling() || !dynamic_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationObjectSharedDataPath(nested_, serialization_version_, path_, path_subcolumn_, dynamic_type_, dynamic_serialization_, subcolumn_type_, bucket, default_path_type_));
    return ISerialization::pooled(getHash(nested_, serialization_version_, path_, path_subcolumn_, dynamic_type_, dynamic_serialization_, subcolumn_type_, bucket, default_path_type_), [&] { return new SerializationObjectSharedDataPath(nested_, serialization_version_, path_, path_subcolumn_, dynamic_type_, dynamic_serialization_, subcolumn_type_, bucket, default_path_type_); });
}

struct DeserializeBinaryBulkStateObjectSharedDataPath : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr map_state;

    ISerialization::DeserializeBinaryBulkStatePtr structure_state;

    ISerialization::DeserializeBinaryBulkStatePtr cloneImpl(ISerialization::DeserializeBinaryBulkStateCloneContext & context) const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectSharedDataPath>(*this);
        new_state->map_state = map_state ? map_state->clone(context) : nullptr;
        new_state->structure_state = structure_state ? structure_state->clone(context) : nullptr;
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
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
        }

        const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataPath>(data.deserialize_state) : nullptr;
        auto map_data = SubstreamData(serialization_map)
                            .withType(data.type ? DataTypeObject::getTypeOfSharedData(default_path_type) : nullptr)
                            .withColumn(data.column ? DataTypeObject::getTypeOfSharedData(default_path_type)->createColumn() : nullptr)
                            .withSerializationInfo(data.serialization_info)
                            .withDeserializeState(deserialize_state ? deserialize_state->map_state : nullptr);
        serialization_map->enumerateStreams(settings, callback, map_data);

        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
            settings.path.pop_back();
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED
             || serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED_CHUNKED)
    {
        settings.path.push_back(Substream::Bucket);
        settings.path.back().bucket = bucket;

        if (settings.use_specialized_prefixes_and_suffixes_substreams)
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructurePrefix);
        else
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructure);

        addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataData);
        addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataPathsMarks);

        /// Substreams/SubstreamsMarks/PathsSubstreamsMetadata are needed only to read an individual
        /// subcolumn of a path directly from disk. When the whole path is requested (no subcolumn),
        /// deserialization reads only Structure/PathsMarks/Data.
        if (!path_subcolumn.empty())
        {
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataSubstreams);
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataSubstreamsMarks);
            addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataPathsSubstreamsMetadata);
        }

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
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
        }

        serialization_map->deserializeBinaryBulkStatePrefix(settings, shared_data_path_state->map_state, cache);

        if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::MAP_WITH_BUCKETS)
            settings.path.pop_back();
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED
             || serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED_CHUNKED)
    {
        settings.path.push_back(Substream::Bucket);
        settings.path.back().bucket = bucket;

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
    IColumn & column,
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
            if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
            {
                std::tie(map_column, num_read_rows) = *cached_column_with_num_read_rows;
            }
            /// Otherwise deserialize the whole shared data map and cache it for other path subcolumns.
            else
            {
                auto mutable_map_column = DataTypeObject::getTypeOfSharedData(default_path_type)->createColumn();
                serialization_map->deserializeBinaryBulkWithMultipleStreams(*mutable_map_column, limit, settings, shared_data_path_state->map_state, cache);
                num_read_rows = mutable_map_column->size();
                map_column = std::move(mutable_map_column);
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, map_column, num_read_rows);
            }
        }
        else
        {
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;

            if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
            {
                map_column = cached_column_with_num_read_rows->first;
            }
            /// If we don't have it in cache, deserialize the bucket's map into a fresh column and cache it.
            else
            {
                auto mutable_map_column = DataTypeObject::getTypeOfSharedData(default_path_type)->createColumn();
                serialization_map->deserializeBinaryBulkWithMultipleStreams(*mutable_map_column, limit, settings, shared_data_path_state->map_state, cache);
                map_column = std::move(mutable_map_column);
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, map_column, map_column->size());
            }

            num_read_rows = map_column->size();
            settings.path.pop_back();
        }

        size_t map_column_offset = map_column->size() - num_read_rows;

        /// If we need to read a subcolumn from Dynamic column, create an empty Dynamic column, fill it and extract subcolumn.
        /// With `DEFAULT PATH TYPE T` path values have type `T`.
        const auto & path_type = default_path_type ? default_path_type : dynamic_type;
        auto temp_dynamic_column = path_subcolumn.empty() ? MutableColumnPtr{} : path_type->createColumn();
        IColumn & dynamic_column = path_subcolumn.empty() ? column : *temp_dynamic_column;
        /// Check if we don't have any paths in shared data in current range.
        const auto & offsets = assert_cast<const ColumnArray &>(*map_column).getOffsets();
        if (offsets.back() == offsets[ssize_t(map_column_offset) - 1])
            dynamic_column.insertManyDefaults(num_read_rows);
        else if (default_path_type)
            fillDefaultPathTypeColumnFromSharedData(dynamic_column, path, *map_column, map_column_offset, map_column->size());
        else
            ColumnObject::fillPathColumnFromSharedData(dynamic_column, path, map_column, map_column_offset, map_column->size());

        /// Extract subcolumn from Dynamic column if needed.
        if (!path_subcolumn.empty())
        {
            auto subcolumn = path_type->getSubcolumn(path_subcolumn, dynamic_column.getPtr());
            column.insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }
    }
    else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED
             || serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED_CHUNKED)
    {
        settings.path.push_back(Substream::Bucket);
        settings.path.back().bucket = bucket;

        auto * shared_data_structure_state = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_path_state->structure_state);
        auto chunk_structures = SerializationObjectSharedData::deserializeStructure(limit, settings, *shared_data_structure_state, cache);
        auto paths_infos_chunks = SerializationObjectSharedData::deserializePathsInfos(*chunk_structures, *shared_data_structure_state, settings, cache);
        /// With `DEFAULT PATH TYPE T` path values are stored with type `T`.
        const auto & path_type = default_path_type ? default_path_type : dynamic_type;
        auto paths_data_chunks = SerializationObjectSharedData::deserializePathsData(
            *chunk_structures, *paths_infos_chunks, *shared_data_structure_state, settings,
            path_type, default_path_type ? default_path_type->getDefaultSerialization() : dynamic_serialization, cache);

        for (size_t chunk_idx = 0; chunk_idx != chunk_structures->size(); ++chunk_idx)
        {
            const auto & chunk_structure = (*chunk_structures)[chunk_idx];
            const auto & paths_info_chunk = (*paths_infos_chunks)[chunk_idx];
            const auto & paths_data_chunk = (*paths_data_chunks)[chunk_idx];

            /// Skip chunk if there is nothing to read from it.
            if (!chunk_structure.limit)
                continue;

            /// Skip chunk if it doesn't have requested path.
            if (!paths_info_chunk.path_to_info.contains(path))
            {
                column.insertManyDefaults(chunk_structure.limit);
                continue;
            }

            auto path_data_it = paths_data_chunk.paths_data.find(path);
            /// Check if we have data of the whole path.
            if (path_data_it != paths_data_chunk.paths_data.end())
            {
                /// With `DEFAULT PATH TYPE T` the path is requested as `Variant(T)` by `SerializationObjectCombinedPath`.
                /// The stored path data is `T` where a missing path is `default(T)`, so treat defaults as missing.
                if (auto * variant_column = path_subcolumn.empty() ? typeid_cast<ColumnVariant *>(&column) : nullptr)
                {
                    const auto & path_data = *path_data_it->second;
                    for (size_t i = chunk_structure.offset; i != chunk_structure.offset + chunk_structure.limit; ++i)
                    {
                        if (path_data.isDefaultAt(i))
                            variant_column->insertDefault();
                        else
                            variant_column->insertIntoVariantFrom(0, path_data, i);
                    }
                }
                /// If no subcolumn is requested, just insert path data into destination column.
                else if (path_subcolumn.empty())
                {
                    column.insertRangeFrom(*path_data_it->second, chunk_structure.offset, chunk_structure.limit);
                }
                /// If subcolumn is requested, extract it from the path data.
                else
                {
                    auto subcolumn = path_type->getSubcolumn(path_subcolumn, path_data_it->second);
                    column.insertRangeFrom(*subcolumn, chunk_structure.offset, chunk_structure.limit);
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
                auto path_subcolumns_data_it = paths_data_chunk.paths_subcolumns_data.find(path);
                if (path_subcolumns_data_it == paths_data_chunk.paths_subcolumns_data.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Subcolumns data of path {} is not deserialized", path);

                auto subcolumn_data_it = path_subcolumns_data_it->second.find(path_subcolumn);
                if (subcolumn_data_it == path_subcolumns_data_it->second.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Data of subcolumn {} of path {} is not deserialized", path_subcolumn, path);

                column.insertRangeFrom(*subcolumn_data_it->second, chunk_structure.offset, chunk_structure.limit);
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

size_t SerializationObjectSharedDataPath::allocatedBytes() const
{
    return sizeof(*this) + path.capacity() + path_subcolumn.capacity();
}

}
