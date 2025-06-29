#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/SerializationObjectSharedDataPath.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnObject.h>
#include <Common/logger_useful.h>
#include <Core/NamesAndTypes.h>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectSharedDataPath::SerializationObjectSharedDataPath(
    const SerializationPtr & nested_, SerializationObjectSharedData::Mode mode_, const String & path_, const String & path_subcolumn_, const DataTypePtr & dynamic_type_, size_t bucket_)
    : SerializationWrapper(nested_)
    , mode(mode_)
    , path(path_)
    , path_subcolumn(path_subcolumn_)
    , dynamic_type(dynamic_type_)
    , dynamic_serialization(dynamic_type_->getDefaultSerialization())
    , bucket(bucket_)
{
    if (mode == SerializationObjectSharedData::Mode::MAP)
        serialization_map = DataTypeObject::getTypeOfSharedData()->getDefaultSerialization();
}

struct DeserializeBinaryBulkStateObjectSharedDataPath : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr map_state;
    ColumnPtr shared_data_map;
    size_t shared_data_map_size = 0;

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
    if (mode == SerializationObjectSharedData::Mode::MAP)
    {
        settings.path.push_back(Substream::ObjectSharedDataBucket);
        settings.path.back().object_shared_data_bucket = bucket;
        const auto * deserialize_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataPath>(data.deserialize_state) : nullptr;
        auto shared_data_substream_data = SubstreamData(serialization_map)
                                              .withType(data.type ? DataTypeObject::getTypeOfSharedData() : nullptr)
                                              .withColumn(data.column ? DataTypeObject::getTypeOfSharedData()->createColumn() : nullptr)
                                              .withSerializationInfo(data.serialization_info)
                                              .withDeserializeState(deserialize_state->map_state);
        settings.path.back().data = shared_data_substream_data;
        serialization_map->enumerateStreams(settings, callback, data);
        settings.path.pop_back();
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "enumerateStreams is not implemented for mode {}", mode);
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
    settings.path.push_back(Substream::ObjectSharedDataBucket);
    settings.path.back().object_shared_data_bucket = bucket;
    if (mode == SerializationObjectSharedData::Mode::MAP)
    {
        serialization_map->deserializeBinaryBulkStatePrefix(settings, shared_data_path_state->map_state, cache);
    }
    else
    {
        shared_data_path_state->structure_state = SerializationObjectSharedData::deserializeStructureStatePrefix(settings, cache);
        auto * structure_state_concrete = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_path_state->structure_state);
        if (!structure_state_concrete->read_all_paths)
            structure_state_concrete->requested_paths.insert(path);
    }
    settings.path.pop_back();

    state = std::move(shared_data_path_state);
}

std::shared_ptr<SerializationObjectSharedDataPath::PathsInfosGranules> SerializationObjectSharedDataPath::deserializePathsInfos(
    const SerializationObjectSharedData::StructureGranules & structure_granules,
    DB::ISerialization::DeserializeBinaryBulkSettings & settings,
    DB::ISerialization::SubstreamsCache * cache) const
{
    auto paths_infos_path = settings.path;
    paths_infos_path.push_back(Substream::ObjectSharedDataPathsInfos);
    /// First check if we already deserialized paths infos and have it in cache.
    if (auto * cached_paths_infos = getElementFromSubstreamsCache(cache, paths_infos_path))
        return assert_cast<SubstreamsCachePathsInfossElement *>(cached_paths_infos)->paths_infos_granules;

    /// Deserialize paths infos granule by granule.
    auto paths_infos_granules = std::make_shared<PathsInfosGranules>();
    paths_infos_granules->reserve(structure_granules.size());
    for (size_t granule = 0; granule != structure_granules.size(); ++granule)
    {
        auto & path_to_info = (*paths_infos_granules).emplace_back().path_to_info;

        /// If there is nothing to read from this granule, just skip it.
        if (structure_granules[granule].limit == 0 || structure_granules[granule].paths.empty())
            continue;

        /// If we don't read any subcolumns, we need only paths marks in the data stream.
        if (mode == SerializationObjectSharedData::Mode::SEPARATE_PATHS || path_subcolumn.empty())
        {
            settings.path.push_back(Substream::ObjectSharedDataPathsMarks);
            auto * paths_marks_stream = settings.getter(settings.path);

            if (!paths_marks_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths marks");

            /// We don't read data from marks stream continuously, so we need to seek to the start of this granule.
            settings.seek_stream_to_mark_callback(settings.path, structure_granules[granule].paths_marks_stream_mark);

            for (size_t i = 0; i != structure_granules[granule].num_paths; ++i)
            {
                auto pos_it = structure_granules[granule].original_position_to_local_position.find(i);
                /// Skip marks of not requested paths.
                if (pos_it == structure_granules[granule].original_position_to_local_position.end())
                {
                    paths_marks_stream->ignore(2 * sizeof(UInt64));
                }
                else
                {
                    auto & path_info = path_to_info[structure_granules[granule].paths[pos_it->second]];
                    readBinaryLittleEndian(path_info.data_mark.offset_in_compressed_file, *paths_marks_stream);
                    readBinaryLittleEndian(path_info.data_mark.offset_in_decompressed_block, *paths_marks_stream);
                }
            }

            settings.path.pop_back();
        }
        /// If we read subcolumn of a path, we need to read all information about subcolumns of requested paths.
        else
        {
            /// Read metadata about paths subcolumns.
            settings.path.push_back(Substream::ObjectSharedDataPathsSubstreamsMetadata);
            auto * paths_substreams_metadata_stream = settings.getter(settings.path);

            if (!paths_substreams_metadata_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams metadata");

            /// We don't read data from marks stream continuously, so we need to seek to the start of this granule.
            settings.seek_stream_to_mark_callback(settings.path, structure_granules[granule].paths_substreams_metadata_stream_mark);
            for (size_t i = 0; i != structure_granules[granule].num_paths; ++i)
            {
                auto pos_it = structure_granules[granule].original_position_to_local_position.find(i);
                /// Skip metadata of not requested paths.
                if (pos_it == structure_granules[granule].original_position_to_local_position.end())
                {
                    paths_substreams_metadata_stream->ignore(4 * sizeof(UInt64));
                }
                else
                {
                    auto & path_info = path_to_info[structure_granules[granule].paths[pos_it->second]];
                    readBinaryLittleEndian(path_info.substreams_mark.offset_in_compressed_file, *paths_substreams_metadata_stream);
                    readBinaryLittleEndian(path_info.substreams_mark.offset_in_decompressed_block, *paths_substreams_metadata_stream);
                    readBinaryLittleEndian(path_info.substreams_marks_mark.offset_in_compressed_file, *paths_substreams_metadata_stream);
                    readBinaryLittleEndian(path_info.substreams_marks_mark.offset_in_decompressed_block, *paths_substreams_metadata_stream);
                }
            }

            settings.path.pop_back();

            /// Read list of substreams for each requested path.
            settings.path.push_back(Substream::ObjectSharedDataSubstreams);
            auto * paths_substreams_stream = settings.getter(settings.path);

            if (!paths_substreams_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams");

            for (const auto & path_in_granule : structure_granules[granule].paths)
            {
                auto & path_info = path_to_info[path_in_granule];
                /// Seek to the start of the substreams list for this path.
                settings.seek_stream_to_mark_callback(settings.path, path_info.substreams_mark);
                size_t num_substreams;
                readVarUInt(num_substreams, *paths_substreams_stream);
                path_info.substreams.reserve(num_substreams);
                for (size_t i = 0; i != num_substreams; ++i)
                {
                    path_info.substreams.emplace_back();
                    readStringBinary(path_info.substreams.back(), *paths_substreams_stream);
                }
            }

            settings.path.pop_back();

            /// Read mark in the data stream for each substream of each requested path.
            settings.path.push_back(Substream::ObjectSharedDataSubstreamsMarks);
            auto * paths_substreams_marks_stream = settings.getter(settings.path);

            if (!paths_substreams_marks_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams marks");

            for (const auto & path_in_granule : structure_granules[granule].paths)
            {
                auto & path_info = path_to_info[path_in_granule];
                /// Seek to the start of the substreams marks for this path.
                settings.seek_stream_to_mark_callback(settings.path, path_info.substreams_marks_mark);
                for (size_t i = 0; i != path_info.substreams.size(); ++i)
                {
                    MarkInCompressedFile substream_mark;
                    readBinaryLittleEndian(substream_mark.offset_in_compressed_file, *paths_substreams_marks_stream);
                    readBinaryLittleEndian(substream_mark.offset_in_decompressed_block, *paths_substreams_marks_stream);
                    path_info.substream_to_mark[path_info.substreams[i]] = substream_mark;
                }
            }

            settings.path.pop_back();
        }
    }

    addElementToSubstreamsCache(cache, paths_infos_path, std::make_unique<SubstreamsCachePathsInfossElement>(paths_infos_granules));
    return paths_infos_granules;
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

    settings.path.push_back(Substream::ObjectSharedDataBucket);
    settings.path.back().object_shared_data_bucket = bucket;
    if (mode == SerializationObjectSharedData::Mode::MAP)
    {
        /// Initialize shared_data column if needed.
        if (column->empty() || !shared_data_path_state->shared_data_map)
        {
            shared_data_path_state->shared_data_map = DataTypeObject::getTypeOfSharedData()->createColumn();
            shared_data_path_state->shared_data_map_size = 0;
        }
        serialization_map->deserializeBinaryBulkWithMultipleStreams(shared_data_path_state->shared_data_map, rows_offset, limit, settings, shared_data_path_state->map_state, cache);
        /// If we need to read a subcolumn from Dynamic column, create an empty Dynamic column, fill it and extract subcolumn.
        MutableColumnPtr dynamic_column = path_subcolumn.empty() ? column->assumeMutable() : dynamic_type->createColumn();
        /// Check if we don't have any paths in shared data in current range.
        const auto & offsets = assert_cast<const ColumnArray &>(*shared_data_path_state->shared_data_map).getOffsets();
        if (offsets.back() == offsets[ssize_t(shared_data_path_state->shared_data_map_size) - 1])
            dynamic_column->insertManyDefaults(limit);
        else
            ColumnObject::fillPathColumnFromSharedData(*dynamic_column, path, shared_data_path_state->shared_data_map, shared_data_path_state->shared_data_map_size, shared_data_path_state->shared_data_map->size());

        /// Extract subcolumn from Dynamic column if needed.
        if (!path_subcolumn.empty())
        {
            auto subcolumn = dynamic_type->getSubcolumn(path_subcolumn, dynamic_column->getPtr());
            column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }

        shared_data_path_state->shared_data_map_size = shared_data_path_state->shared_data_map->size();
    }
    else if (mode == SerializationObjectSharedData::Mode::SEPARATE_PATHS || mode == SerializationObjectSharedData::Mode::SEPARATE_SUBSTREAMS)
    {
        if (!settings.seek_stream_to_mark_callback)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read object shared data path because seek_stream_to_mark_callback is not initialized");

        auto * shared_data_structure_state = checkAndGetState<SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_path_state->structure_state);
        auto structure_granules = SerializationObjectSharedData::deserializeStructure(rows_offset, limit, settings, *shared_data_structure_state, cache, mode);
        auto paths_infos_granules = deserializePathsInfos(*structure_granules, settings, cache);

        /// Now we have all required information to read the data of requested path and subcolumn (if any).
        settings.path.push_back(Substream::ObjectSharedDataData);
        auto * data_stream = settings.getter(settings.path);
        if (!data_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data data");

        ColumnPtr dynamic_column = path_subcolumn.empty() ? column->assumeMutable() : dynamic_type->createColumn();

        DeserializeBinaryBulkSettings deserialization_settings;
        deserialization_settings.object_and_dynamic_read_statistics = false;
        deserialization_settings.position_independent_encoding = true;
        deserialization_settings.use_specialized_prefixes_and_suffixes_substreams = true;
        deserialization_settings.data_part_type = MergeTreeDataPartType::Compact;
        deserialization_settings.seek_stream_to_mark_callback = [&](const SubstreamPath &, const MarkInCompressedFile & mark)
        {
            settings.seek_stream_to_mark_callback(settings.path, mark);
        };

        for (size_t granule = 0; granule != structure_granules->size(); ++granule)
        {
            const auto & structure_granule = (*structure_granules)[granule];

            /// Skip granule if there is nothing to read from it.
            if (!structure_granule.limit)
                continue;

            /// Skip granule if it doesn't have requested path.
            if (!structure_granule.paths_set.contains(path))
            {
                dynamic_column->assumeMutable()->insertManyDefaults(structure_granule.limit);
                continue;
            }

            auto path_info_it = (*paths_infos_granules)[granule].path_to_info.find(path);
            if (path_info_it == (*paths_infos_granules)[granule].path_to_info.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Info for path {} is not deserialized", path);

            const auto & path_info = path_info_it->second;

            /// If we have subcolumn we read only requireed substreams.
            if (mode == SerializationObjectSharedData::Mode::SEPARATE_SUBSTREAMS && !path_subcolumn.empty())
            {
                deserialization_settings.seek_stream_to_current_mark_callback = [&](const SubstreamPath & substream_path)
                {
                    auto stream_name = ISerialization::getFileNameForStream(NameAndTypePair("", dynamic_type), substream_path);

                    auto it = path_info.substream_to_mark.find(stream_name);
                    if (it == path_info.substream_to_mark.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Substream {} for path {} and subcolumn {} is requested but not found in substreams list", stream_name, path, path_subcolumn);

                    /// Seek to the requested substream in the data stream.
                    settings.seek_stream_to_mark_callback(settings.path, it->second);
                };

                deserialization_settings.getter = [&](const SubstreamPath & substream_path) -> ReadBuffer *
                {
                    /// Seek to the requested substream before returning the data stream.
                    deserialization_settings.seek_stream_to_current_mark_callback(substream_path);
                    return data_stream;
                };

                DeserializeBinaryBulkStatePtr path_subcolumn_state;
                /// We can't read directly into the provided column, because it might have different
                /// dynamic structure from what is serialized in this granule.
                ColumnPtr subcolumn = column->cloneEmpty();
                nested_serialization->deserializeBinaryBulkStatePrefix(deserialization_settings, path_subcolumn_state, nullptr);
                nested_serialization->deserializeBinaryBulkWithMultipleStreams(subcolumn, structure_granule.offset, structure_granule.limit, deserialization_settings, path_subcolumn_state, nullptr);
                column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
            }
            else
            {
                deserialization_settings.getter = [&](const SubstreamPath &) -> ReadBuffer * { return data_stream; };
                settings.seek_stream_to_mark_callback(settings.path, path_info.data_mark);
                DeserializeBinaryBulkStatePtr path_state;
                /// We can't read directly into the provided column, because it might have different
                /// dynamic structure from what is serialized in this granule.
                ColumnPtr new_dynamic_column = dynamic_type->createColumn();
                dynamic_serialization->deserializeBinaryBulkStatePrefix(deserialization_settings, path_state, nullptr);
                dynamic_serialization->deserializeBinaryBulkWithMultipleStreams(new_dynamic_column, 0, structure_granule.num_rows, deserialization_settings, path_state, nullptr);
                dynamic_column->assumeMutable()->insertRangeFrom(*new_dynamic_column, structure_granule.offset, structure_granule.limit);
            }
        }

        if (!path_subcolumn.empty())
        {
            auto subcolumn = dynamic_type->getSubcolumn(path_subcolumn, dynamic_column->getPtr());
            column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }


        settings.path.pop_back();
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "deserializeBinaryBulkWithMultipleStreams is not implemented for mode {}", mode);
    }

    settings.path.pop_back();
}

}
