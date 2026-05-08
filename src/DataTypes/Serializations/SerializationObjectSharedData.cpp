#include <DataTypes/Serializations/SerializationObjectSharedData.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/Serializations/getSubcolumnsDeserializationOrder.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Core/NamesAndTypes.h>
#include <IO/ReadHelpers.h>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

SerializationObjectSharedData::SerializationObjectSharedData(SerializationVersion serialization_version_, const DataTypePtr & dynamic_type_, size_t buckets_)
    : serialization_version(serialization_version_)
    , dynamic_type(dynamic_type_)
    , dynamic_serialization(dynamic_type_->getDefaultSerialization())
    , buckets(buckets_)
    , serialization_map(DataTypeObject::getTypeOfSharedData()->getDefaultSerialization())
{
}

SerializationObjectSharedData::SerializationVersion::SerializationVersion(UInt64 version) : value(static_cast<Value>(version))
{
    checkVersion(version);
}

SerializationObjectSharedData::SerializationVersion::SerializationVersion(DB::MergeTreeObjectSharedDataSerializationVersion version)
{
    switch (version)
    {
        case MergeTreeObjectSharedDataSerializationVersion::MAP:
            value = MAP;
            break;
        case MergeTreeObjectSharedDataSerializationVersion::MAP_WITH_BUCKETS:
            value = MAP_WITH_BUCKETS;
            break;
        case MergeTreeObjectSharedDataSerializationVersion::ADVANCED:
            value = ADVANCED;
            break;
    }
}

void SerializationObjectSharedData::SerializationVersion::checkVersion(UInt64 version)
{
    if (version != MAP && version != MAP_WITH_BUCKETS && version != ADVANCED)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version for Object shared data serialization: {}", version);
}

struct SerializeBinaryBulkStateObjectSharedData : public ISerialization::SerializeBinaryBulkState
{
    ISerialization::SerializeBinaryBulkStatePtr map_state;
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> bucket_map_states;
};

struct DeserializeBinaryBulkStateObjectSharedData : public ISerialization::DeserializeBinaryBulkState
{
    ISerialization::DeserializeBinaryBulkStatePtr map_state;
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_map_states;
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_structure_states;
    /// Some granules can be partially read, we need to remember how many rows
    /// were already read from the last incomplete granule.
    size_t last_incomplete_granule_offset = 0;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectSharedData>(*this);
        for (size_t bucket = 0; bucket != bucket_map_states.size(); ++bucket)
            new_state->bucket_map_states[bucket] = bucket_map_states[bucket] ? bucket_map_states[bucket]->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_structure_states.size(); ++bucket)
            new_state->bucket_structure_states[bucket] = bucket_structure_states[bucket] ? bucket_structure_states[bucket]->clone() : nullptr;
        return new_state;
    }
};

void SerializationObjectSharedData::enumerateStreams(
    ISerialization::EnumerateStreamsSettings & settings,
    const ISerialization::StreamCallback & callback,
    const ISerialization::SubstreamData & data) const
{
    const auto * shared_data_state = data.deserialize_state ? checkAndGetState<DeserializeBinaryBulkStateObjectSharedData>(data.deserialize_state) : nullptr;

    if (serialization_version.value == SerializationVersion::MAP)
    {
        auto map_data = SubstreamData(serialization_map)
                            .withColumn(data.column)
                            .withType(data.type)
                            .withSerializationInfo(data.serialization_info)
                            .withDeserializeState(shared_data_state ? shared_data_state->map_state : nullptr);

        serialization_map->enumerateStreams(settings, callback, map_data);
        return;
    }

    /// Other 2 serializations MAP_WITH_BUCKETS and ADVAMCED support buckets.
    for (size_t bucket = 0; bucket != buckets; ++bucket)
    {
        settings.path.push_back(Substream::ObjectSharedDataBucket);
        settings.path.back().object_shared_data_bucket = bucket;
        if (serialization_version.value == SerializationVersion::MAP_WITH_BUCKETS)
        {
            auto map_data = SubstreamData(serialization_map)
                                .withColumn(data.column)
                                .withType(data.type)
                                .withSerializationInfo(data.serialization_info)
                                .withDeserializeState(shared_data_state ? shared_data_state->bucket_map_states[bucket] : nullptr);
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

    /// Streams related to shared data copy in ADVANCED serialization.
    if (serialization_version.value == SerializationVersion::ADVANCED)
    {
        settings.path.push_back(Substream::ObjectSharedDataCopy);

        addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataCopySizes);
        addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataCopyPathsIndexes);
        addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataCopyValues);

        settings.path.pop_back();
    }
}

void SerializationObjectSharedData::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    ISerialization::SerializeBinaryBulkSettings & settings,
    ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    auto shared_data_state = std::make_shared<SerializeBinaryBulkStateObjectSharedData>();

    if (serialization_version.value == SerializationVersion::MAP)
    {
        serialization_map->serializeBinaryBulkStatePrefix(column, settings, shared_data_state->map_state);
    }
    else if (serialization_version.value == SerializationVersion::MAP_WITH_BUCKETS)
    {
        shared_data_state->bucket_map_states.resize(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            serialization_map->serializeBinaryBulkStatePrefix(column, settings, shared_data_state->bucket_map_states[bucket]);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED)
    {
        /// ADVANCED serialization doesn't have serialization prefix.
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "enumerateStreams is not implemented for shared data serialization version {}", serialization_version.value);
    }


    state = std::move(shared_data_state);
}

void SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    ISerialization::SerializeBinaryBulkSettings & settings,
    ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    auto * shared_data_state = checkAndGetState<SerializeBinaryBulkStateObjectSharedData>(state);

    if (serialization_version.value == SerializationVersion::MAP)
    {
        serialization_map->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, shared_data_state->map_state);
    }
    else if (serialization_version.value == SerializationVersion::MAP_WITH_BUCKETS)
    {
        size_t end = limit && offset + limit < column.size() ? offset + limit : column.size();
        auto shared_data_buckets = splitSharedDataPathsToBuckets(column, offset, end, buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            serialization_map->serializeBinaryBulkWithMultipleStreams(*shared_data_buckets[bucket], 0, 0, settings, shared_data_state->bucket_map_states[bucket]);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED)
    {
        size_t end = limit && offset + limit < column.size() ? offset + limit : column.size();
        /// First we need to flatten all paths stored in the shared data and separate them into buckets.
        auto flattened_paths_buckets = flattenAndBucketSharedDataPaths(column, offset, end, dynamic_type, buckets);
        /// Second, write paths in each bucket separately.
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            const auto & flattened_paths = flattened_paths_buckets[bucket];
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;

            /// Write structure of this granule.
            Substream structure_stream_type = settings.use_specialized_prefixes_and_suffixes_substreams ? Substream::ObjectSharedDataStructurePrefix
                                                                                           : Substream::ObjectSharedDataStructure;
            settings.path.push_back(structure_stream_type);
            auto * structure_stream = settings.getter(settings.path);
            settings.path.pop_back();

            if (!structure_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data structure in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            /// Write number of rows in this granule and list of all paths that we will serialize.
            writeVarUInt(end - offset, *structure_stream);
            writeVarUInt(flattened_paths.size(), *structure_stream);
            for (const auto & [path, _] : flattened_paths)
                writeStringBinary(path, *structure_stream);

            /// Write data of flattened paths.
            settings.path.push_back(Substream::ObjectSharedDataData);
            auto * data_stream = settings.getter(settings.path);

            if (!data_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data data in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            if (!settings.stream_mark_getter)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark getter is not set for ADVANCED shared data serialization");

            /// Remember the mark of the ObjectSharedDataData stream, we will write it in the structure stream later.
            MarkInCompressedFile data_stream_mark = settings.stream_mark_getter(settings.path);

            /// Collect mark of the ObjectSharedDataData stream for each path.
            std::vector<MarkInCompressedFile> paths_marks;
            paths_marks.reserve(flattened_paths.size());
            /// Collect list of substreams for each path.
            std::vector<std::vector<String>> paths_substreams;
            paths_substreams.reserve(flattened_paths.size());
            /// Collect mark of the ObjectSharedDataData stream for each substream of each path.
            std::vector<std::vector<MarkInCompressedFile>> paths_substreams_marks;
            paths_substreams_marks.reserve(flattened_paths.size());

            /// Configure serialization settings as in Compact part.
            SerializeBinaryBulkSettings data_serialization_settings;
            data_serialization_settings.data_part_type = MergeTreeDataPartType::Compact;
            data_serialization_settings.position_independent_encoding = true;
            data_serialization_settings.low_cardinality_max_dictionary_size = 0;
            data_serialization_settings.use_specialized_prefixes_and_suffixes_substreams = true;
            data_serialization_settings.use_compact_variant_discriminators_serialization = true;
            data_serialization_settings.dynamic_serialization_version = MergeTreeDynamicSerializationVersion::V3;
            data_serialization_settings.object_serialization_version = MergeTreeObjectSerializationVersion::V3;
            /// Also use ADVANCED serialization for nested Object types.
            data_serialization_settings.object_shared_data_serialization_version = MergeTreeObjectSharedDataSerializationVersion::ADVANCED;
            /// Don't write any dynamic statistics.
            data_serialization_settings.object_and_dynamic_write_statistics = ISerialization::SerializeBinaryBulkSettings::ObjectAndDynamicStatisticsMode::NONE;
            data_serialization_settings.stream_mark_getter = [&](const SubstreamPath &) -> MarkInCompressedFile { return settings.stream_mark_getter(settings.path); };

            StreamFileNameSettings stream_file_name_settings;
            stream_file_name_settings.escape_variant_substreams = false;

            for (const auto & [path, path_column] : flattened_paths)
            {
                paths_substreams.emplace_back();
                paths_substreams_marks.emplace_back();
                data_serialization_settings.getter = [&](const SubstreamPath & substream_path) -> WriteBuffer *
                {
                    /// Add new substream and its mark for current path.
                    paths_substreams.back().push_back(ISerialization::getFileNameForStream(NameAndTypePair("", dynamic_type), substream_path, stream_file_name_settings));
                    paths_substreams_marks.back().push_back(settings.stream_mark_getter(settings.path));
                    return data_stream;
                };

                SerializeBinaryBulkStatePtr path_state;
                /// Remember the mark of ObjectSharedDataData stream for this path before writing any data.
                paths_marks.push_back(settings.stream_mark_getter(settings.path));
                dynamic_serialization->serializeBinaryBulkStatePrefix(*path_column, data_serialization_settings, path_state);
                dynamic_serialization->serializeBinaryBulkWithMultipleStreams(*path_column, 0, 0, data_serialization_settings, path_state);
                dynamic_serialization->serializeBinaryBulkStateSuffix(data_serialization_settings, path_state);
            }

            /// End ObjectSharedDataData stream.
            settings.path.pop_back();

            /// Write paths marks of the ObjectSharedDataData stream.
            settings.path.push_back(Substream::ObjectSharedDataPathsMarks);
            auto * paths_marks_stream = settings.getter(settings.path);

            if (!paths_marks_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data paths marks in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            /// Remember the mark of the ObjectSharedDataPathsMarks stream, we will write it in the structure stream later.
            MarkInCompressedFile paths_marks_stream_mark = settings.stream_mark_getter(settings.path);

            for (const auto & mark : paths_marks)
            {
                writeBinaryLittleEndian(mark.offset_in_compressed_file, *paths_marks_stream);
                writeBinaryLittleEndian(mark.offset_in_decompressed_block, *paths_marks_stream);
            }

            /// End ObjectSharedDataPathsMarks stream.
            settings.path.pop_back();

            /// Write paths substreams.
            settings.path.push_back(Substream::ObjectSharedDataSubstreams);
            auto * paths_substreams_stream = settings.getter(settings.path);

            if (!paths_substreams_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data paths substreams in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            /// Collect marks of the ObjectSharedDataSubstreams stream for each path so
            /// we will be able to seek to the start of the substreams list of the specific path.
            std::vector<MarkInCompressedFile> marks_of_paths_substreams;
            marks_of_paths_substreams.reserve(paths_substreams.size());
            for (const auto & path_substreams : paths_substreams)
            {
                marks_of_paths_substreams.push_back(settings.stream_mark_getter(settings.path));
                /// Write number of substreams of this path and the list of substreams.
                writeVarUInt(path_substreams.size(), *paths_substreams_stream);
                for (const auto & substream : path_substreams)
                    writeStringBinary(substream, *paths_substreams_stream);
            }

            /// End ObjectSharedDataSubstreams stream.
            settings.path.pop_back();

            /// Write paths substreams marks of the ObjectSharedDataData stream.
            settings.path.push_back(Substream::ObjectSharedDataSubstreamsMarks);
            auto * substreams_marks_stream = settings.getter(settings.path);

            if (!substreams_marks_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data substreams marks in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            /// Collect mark of the ObjectSharedDataSubstreamsMarks stream for each path so
            /// we will be able to seek to the start of the substreams marks of the specific path.
            std::vector<MarkInCompressedFile> marks_of_paths_substreams_marks;
            marks_of_paths_substreams_marks.reserve(paths_substreams_marks.size());
            for (const auto & substreams_marks : paths_substreams_marks)
            {
                marks_of_paths_substreams_marks.push_back(settings.stream_mark_getter(settings.path));
                for (const auto & mark : substreams_marks)
                {
                    writeBinaryLittleEndian(mark.offset_in_compressed_file, *substreams_marks_stream);
                    writeBinaryLittleEndian(mark.offset_in_decompressed_block, *substreams_marks_stream);
                }
            }

            /// End ObjectSharedDataSubstreamsMarks stream.
            settings.path.pop_back();

            /// For each path write mark of its substreams in ObjectSharedDataSubstreams/ObjectSharedDataSubstreamsMarks streams.
            settings.path.push_back(Substream::ObjectSharedDataPathsSubstreamsMetadata);
            auto * paths_substreams_metadata_stream = settings.getter(settings.path);

            if (!paths_substreams_metadata_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data paths substreams metadata in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            MarkInCompressedFile paths_substreams_metadata_stream_mark = settings.stream_mark_getter(settings.path);
            for (size_t i = 0; i != marks_of_paths_substreams.size(); ++i)
            {
                writeBinaryLittleEndian(marks_of_paths_substreams[i].offset_in_compressed_file, *paths_substreams_metadata_stream);
                writeBinaryLittleEndian(marks_of_paths_substreams[i].offset_in_decompressed_block, *paths_substreams_metadata_stream);
                writeBinaryLittleEndian(marks_of_paths_substreams_marks[i].offset_in_compressed_file, *paths_substreams_metadata_stream);
                writeBinaryLittleEndian(marks_of_paths_substreams_marks[i].offset_in_decompressed_block, *paths_substreams_metadata_stream);
            }

            /// End ObjectSharedDataPathsSubstreamsMetadata stream.
            settings.path.pop_back();

            /// Write collected marks of other streams into structure stream.
            structure_stream_type = settings.use_specialized_prefixes_and_suffixes_substreams ? Substream::ObjectSharedDataStructureSuffix
                                                                                 : Substream::ObjectSharedDataStructure;
            settings.path.push_back(structure_stream_type);
            structure_stream = settings.getter(settings.path);
            settings.path.pop_back();

            if (!structure_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data structure in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            writeBinaryLittleEndian(data_stream_mark.offset_in_compressed_file, *structure_stream);
            writeBinaryLittleEndian(data_stream_mark.offset_in_decompressed_block, *structure_stream);
            writeBinaryLittleEndian(paths_marks_stream_mark.offset_in_compressed_file, *structure_stream);
            writeBinaryLittleEndian(paths_marks_stream_mark.offset_in_decompressed_block, *structure_stream);
            writeBinaryLittleEndian(paths_substreams_metadata_stream_mark.offset_in_compressed_file, *structure_stream);
            writeBinaryLittleEndian(paths_substreams_metadata_stream_mark.offset_in_decompressed_block, *structure_stream);

            settings.path.pop_back();
        }

        /// Write a modified copy of the shared data for faster shared data reading.
        settings.path.push_back(Substream::ObjectSharedDataCopy);

        const auto & shared_data_array_column = assert_cast<const ColumnArray &>(column);
        const auto & shared_data_tuple_column = assert_cast<const ColumnTuple &>(shared_data_array_column.getData());
        const auto & shared_data_offsets_column = shared_data_array_column.getOffsetsColumn();
        const auto & shared_data_offsets = shared_data_array_column.getOffsets();

        /// Write array sizes.
        settings.path.push_back(Substream::ObjectSharedDataCopySizes);
        SerializationArray::serializeOffsetsBinaryBulk(shared_data_offsets_column, offset, limit, settings);
        settings.path.pop_back();

        size_t nested_offset = offset ? shared_data_offsets[offset - 1] : 0;
        size_t nested_limit = limit ? shared_data_offsets[end - 1] - nested_offset : shared_data_offsets.back() - nested_offset;
        size_t nested_end = nested_offset + nested_limit;

        settings.path.push_back(Substream::ObjectSharedDataCopyPathsIndexes);
        auto * copy_indexes_stream = settings.getter(settings.path);

        if (!copy_indexes_stream)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Got empty stream for shared data copy indexes in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

        /// We already serialized all paths in the structure streams of buckets.
        /// Instead of writing all paths again we create a column that contains indexes
        /// of paths in the total list of paths that we serialized for buckets.
        std::unordered_map<std::string_view, size_t> path_to_index;
        size_t index = 0;
        for (const auto & [path, _] : flattened_paths_buckets | std::views::join)
        {
            path_to_index[path] = index;
            ++index;
        }

        auto [indexes_column, indexes_type] = createPathsIndexes(path_to_index, shared_data_tuple_column.getColumn(0), nested_offset, nested_end);
        indexes_type->getDefaultSerialization()->serializeBinaryBulk(*indexes_column, *copy_indexes_stream, 0, nested_limit);
        settings.path.pop_back();

        /// Write paths values.
        settings.path.push_back(Substream::ObjectSharedDataCopyValues);
        auto * copy_values_stream = settings.getter(settings.path);

        if (!copy_values_stream)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Got empty stream for shared data copy values in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

        const auto & values_column = shared_data_tuple_column.getColumn(1);
        if (nested_limit)
            SerializationString().serializeBinaryBulk(values_column, *copy_values_stream, nested_offset, nested_limit);
        settings.path.pop_back();

        settings.path.pop_back();
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "serializeBinaryBulkWithMultipleStreams is not implemented for shared data serialization version {}", serialization_version.value);
    }
}

void SerializationObjectSharedData::serializeBinaryBulkStateSuffix(
    ISerialization::SerializeBinaryBulkSettings & settings, ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    auto * shared_data_state = checkAndGetState<SerializeBinaryBulkStateObjectSharedData>(state);

    if (serialization_version.value == SerializationVersion::MAP)
    {
        serialization_map->serializeBinaryBulkStateSuffix(settings, shared_data_state->map_state);
    }
    else if (serialization_version.value == SerializationVersion::MAP_WITH_BUCKETS)
    {
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            serialization_map->serializeBinaryBulkStateSuffix(settings, shared_data_state->bucket_map_states[bucket]);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED)
    {
        /// ADVANCED doesn't have suffix.
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "serializeBinaryBulkStateSuffix is not implemented for shared data serialization version {}", serialization_version.value);
    }

}

void SerializationObjectSharedData::deserializeBinaryBulkStatePrefix(
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    ISerialization::SubstreamsDeserializeStatesCache * cache) const
{
    auto shared_data_state = std::make_shared<DeserializeBinaryBulkStateObjectSharedData>();

    if (serialization_version.value == SerializationVersion::MAP)
    {
        serialization_map->deserializeBinaryBulkStatePrefix(settings, shared_data_state->map_state, cache);
    }
    else if (serialization_version.value == SerializationVersion::MAP_WITH_BUCKETS)
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
    else if (serialization_version.value == SerializationVersion::ADVANCED)
    {
        shared_data_state->bucket_structure_states.resize(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            shared_data_state->bucket_structure_states[bucket] = deserializeStructureStatePrefix(settings, cache);
            auto * structure_state_concrete = checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_state->bucket_structure_states[bucket]);
            structure_state_concrete->need_all_paths = true;
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

ISerialization::DeserializeBinaryBulkStatePtr SerializationObjectSharedData::deserializeStructureStatePrefix(DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache)
{
    settings.path.push_back(Substream::ObjectSharedDataStructure);
    DeserializeBinaryBulkStatePtr state = nullptr;
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
    {
        state = cached_state;
    }
    else
    {
        state = std::make_shared<DeserializeBinaryBulkStateObjectSharedDataStructure>();
        /// Add state to cache so all columns/subcolumns that read from this stream will share the same state.
        addToSubstreamsDeserializeStatesCache(cache, settings.path, state);
    }

    settings.path.pop_back();
    return state;
}

void SerializationObjectSharedData::deserializeStructureGranulePrefix(
    ReadBuffer & buf,
    SerializationObjectSharedData::StructureGranule & structure_granule,
    const DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state)
{
    /// Read number of rows in this granule.
    readVarUInt(structure_granule.num_rows, buf);
    String path;
    /// Read number of paths stored in this granule.
    readVarUInt(structure_granule.num_paths, buf);

    if (structure_state.need_all_paths)
        structure_granule.all_paths.reserve(structure_granule.num_paths);

    /// Read list of paths.
    for (size_t i = 0; i != structure_granule.num_paths; ++i)
    {
        readStringBinary(path, buf);
        if (structure_state.requested_paths.contains(path) || structure_state.requested_paths_subcolumns.contains(path) || structure_state.checkIfPathMatchesAnyRequestedPrefix(path))
            structure_granule.position_to_requested_path[i] = path;

        if (structure_state.need_all_paths)
            structure_granule.all_paths.push_back(path);
    }
}

void SerializationObjectSharedData::deserializeStructureGranuleSuffix(ReadBuffer & buf, SerializationObjectSharedData::StructureGranule & structure_granule)
{
    readBinaryLittleEndian(structure_granule.data_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(structure_granule.data_stream_mark.offset_in_decompressed_block, buf);
    readBinaryLittleEndian(structure_granule.paths_marks_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(structure_granule.paths_marks_stream_mark.offset_in_decompressed_block, buf);
    readBinaryLittleEndian(structure_granule.paths_substreams_metadata_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(structure_granule.paths_substreams_metadata_stream_mark.offset_in_decompressed_block, buf);
}

std::shared_ptr<SerializationObjectSharedData::StructureGranules> SerializationObjectSharedData::deserializeStructure(
    size_t rows_offset,
    size_t limit,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
    ISerialization::SubstreamsCache * cache)
{
    /// First check if we already deserialized data from structure steam and have it in the cache.
    auto structure_path = settings.path;
    structure_path.push_back(Substream::ObjectSharedDataStructure);
    if (const auto * cached_structure = getElementFromSubstreamsCache(cache, structure_path))
    {
        return assert_cast<const SubstreamsCacheStructureElement *>(cached_structure)->structure_granules;
    }

    auto result = std::make_shared<StructureGranules>();

    /// In Compact part we always read one whole granule, so we don't need to worry about reading structure from multiple granules.
    if (settings.data_part_type == MergeTreeDataPartType::Compact)
    {
        if (!settings.use_specialized_prefixes_and_suffixes_substreams)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Compact part must use specialized substreams for prefixes and suffixes");

        StructureGranule structure_granule;

        /// Read prefix of the structure stream
        settings.path.push_back(Substream::ObjectSharedDataStructurePrefix);
        auto * structure_prefix_stream = settings.getter(settings.path);
        if (!structure_prefix_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty structure prefix stream for object shared data data");

        deserializeStructureGranulePrefix(*structure_prefix_stream, structure_granule, structure_state);
        settings.path.pop_back();

        if (structure_granule.num_rows != rows_offset + limit)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected reading a single granule with {} rows, requested {} rows in Compact part", structure_granule.num_rows, rows_offset + limit);

        structure_granule.offset = rows_offset;
        structure_granule.limit = limit;

        /// Read suffix of the structure stream.
        settings.path.push_back(Substream::ObjectSharedDataStructureSuffix);
        auto * structure_suffix_stream = settings.getter(settings.path);
        if (!structure_suffix_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty structure suffix stream for object shared data");

        deserializeStructureGranuleSuffix(*structure_suffix_stream, structure_granule);
        settings.path.pop_back();

        result->push_back(std::move(structure_granule));
    }
    /// In Wide part we can read multiple granules together and read only part of last granule.
    else
    {
        auto * structure_stream = settings.getter(structure_path);

        if (!structure_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty structure stream for object shared data");

        /// Reset last read granule state if we don't continue reading from it.
        if (!settings.continuous_reading)
            structure_state.last_granule_structure.clear();

        size_t rows_to_read = limit + rows_offset;
        while (rows_to_read != 0)
        {
            auto & current_granule = structure_state.last_granule_structure;

            /// Calculate remaining rows in current granule that can be read.
            size_t remaining_rows_in_granule = current_granule.num_rows - current_granule.limit - current_granule.offset;

            /// If there is nothing to read from current granule - read the next one.
            if (remaining_rows_in_granule == 0)
            {
                /// Finish if there is no more data in structure stream.
                if (structure_stream->eof())
                    break;

                current_granule.clear();
                deserializeStructureGranulePrefix(*structure_stream, current_granule, structure_state);
                deserializeStructureGranuleSuffix(*structure_stream, current_granule);
                remaining_rows_in_granule = current_granule.num_rows;
            }

            /// Check if we need to read the whole granule.
            if (remaining_rows_in_granule <= rows_to_read)
            {
                /// Check if we need to skip all rows in this granule.
                if (rows_offset >= remaining_rows_in_granule)
                {
                    current_granule.offset = current_granule.num_rows;
                    current_granule.limit = 0;
                    rows_offset -= remaining_rows_in_granule;
                }
                /// Otherwise some rows from this granule will be read.
                else
                {
                    /// offset and limit in current granule may be non 0 if we already read from this granule before.
                    /// We need to start reading starting from last read row (offset + limit)
                    current_granule.offset += current_granule.limit + rows_offset;
                    current_granule.limit = current_granule.num_rows - current_granule.offset;
                    rows_offset = 0;
                }

                rows_to_read -= remaining_rows_in_granule;
            }
            /// Otherwise we read only the part of the ganule.
            else
            {
                /// offset and limit in current granule may be non 0 if we already read from this granule before.
                /// We need to start reading starting from last read row (offset + limit)
                current_granule.offset += current_granule.limit + rows_offset;
                current_granule.limit = rows_to_read - rows_offset;
                rows_offset = 0;
                rows_to_read = 0;
            }

            result->push_back(current_granule);
        }
    }

    /// Add deserialized data into cache.
    addElementToSubstreamsCache(cache, structure_path, std::make_unique<SubstreamsCacheStructureElement>(result));
    return result;
}

std::shared_ptr<SerializationObjectSharedData::PathsInfosGranules> SerializationObjectSharedData::deserializePathsInfos(
    const SerializationObjectSharedData::StructureGranules & structure_granules,
    const SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::SubstreamsCache * cache)
{
    auto paths_infos_path = settings.path;
    paths_infos_path.push_back(Substream::ObjectSharedDataPathsInfos);
    /// First check if we already deserialized paths infos and have it in cache.
    if (auto * cached_paths_infos = getElementFromSubstreamsCache(cache, paths_infos_path))
        return assert_cast<SubstreamsCachePathsInfosElement *>(cached_paths_infos)->paths_infos_granules;

    /// Deserialize paths infos granule by granule.
    auto paths_infos_granules = std::make_shared<PathsInfosGranules>();
    paths_infos_granules->reserve(structure_granules.size());
    for (const auto & structure_granule : structure_granules)
    {
        auto & path_to_info = (*paths_infos_granules).emplace_back().path_to_info;

        /// If there is nothing to read from this granule, just skip it.
        if (structure_granule.limit == 0 || structure_granule.position_to_requested_path.empty())
            continue;

        bool need_paths_marks = false;
        bool need_subcolumns_info = false;
        for (const auto & [_, requested_path] : structure_granule.position_to_requested_path)
        {
            /// For paths inside requested_paths_subcolumns we will need to read only subcolumns
            /// and don't need paths marks.
            if (structure_state.requested_paths_subcolumns.contains(requested_path))
                need_subcolumns_info = true;
            else
                need_paths_marks = true;
        }

        if (!settings.seek_stream_to_mark_callback)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read paths from object shared data with ADVANCED serialization version because seek_stream_to_mark_callback is not initialized");

        if (need_paths_marks)
        {
            settings.path.push_back(Substream::ObjectSharedDataPathsMarks);
            auto * paths_marks_stream = settings.getter(settings.path);

            if (!paths_marks_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths marks");

            /// We don't read data from marks stream continuously, so we need to seek to the start of this granule.
            settings.seek_stream_to_mark_callback(settings.path, structure_granule.paths_marks_stream_mark);

            for (size_t i = 0; i != structure_granule.num_paths; ++i)
            {
                auto path_it = structure_granule.position_to_requested_path.find(i);
                /// Skip marks of not requested paths.
                if (path_it == structure_granule.position_to_requested_path.end())
                {
                    paths_marks_stream->ignore(2 * sizeof(UInt64));
                }
                else
                {
                    auto & path_info = path_to_info[path_it->second];
                    readBinaryLittleEndian(path_info.data_mark.offset_in_compressed_file, *paths_marks_stream);
                    readBinaryLittleEndian(path_info.data_mark.offset_in_decompressed_block, *paths_marks_stream);
                }
            }

            settings.path.pop_back();
        }

        if (need_subcolumns_info)
        {
            /// Read metadata about paths subcolumns.
            settings.path.push_back(Substream::ObjectSharedDataPathsSubstreamsMetadata);
            auto * paths_substreams_metadata_stream = settings.getter(settings.path);

            if (!paths_substreams_metadata_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams metadata");

            /// We don't read data from marks stream continuously, so we need to seek to the start of this granule.
            settings.seek_stream_to_mark_callback(settings.path, structure_granule.paths_substreams_metadata_stream_mark);
            for (size_t i = 0; i != structure_granule.num_paths; ++i)
            {
                auto path_it = structure_granule.position_to_requested_path.find(i);
                /// Skip metadata of not requested paths.
                if (path_it == structure_granule.position_to_requested_path.end())
                {
                    paths_substreams_metadata_stream->ignore(4 * sizeof(UInt64));
                }
                else
                {
                    auto & path_info = path_to_info[path_it->second];
                    readBinaryLittleEndian(path_info.substreams_mark.offset_in_compressed_file, *paths_substreams_metadata_stream);
                    readBinaryLittleEndian(path_info.substreams_mark.offset_in_decompressed_block, *paths_substreams_metadata_stream);
                    readBinaryLittleEndian(path_info.substreams_marks_mark.offset_in_compressed_file, *paths_substreams_metadata_stream);
                    readBinaryLittleEndian(path_info.substreams_marks_mark.offset_in_decompressed_block, *paths_substreams_metadata_stream);
                }
            }

            settings.path.pop_back();

            /// Read list of substreams for each path with requested subcolumns.
            settings.path.push_back(Substream::ObjectSharedDataSubstreams);
            auto * paths_substreams_stream = settings.getter(settings.path);

            if (!paths_substreams_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams");

            for (const auto & [_, requested_path] : structure_granule.position_to_requested_path)
            {
                if (!structure_state.requested_paths_subcolumns.contains(requested_path))
                    continue;

                auto & path_info = path_to_info[requested_path];
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

            /// Read mark in the data stream for each substream of each path with requested subcolumns.
            settings.path.push_back(Substream::ObjectSharedDataSubstreamsMarks);
            auto * paths_substreams_marks_stream = settings.getter(settings.path);

            if (!paths_substreams_marks_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams marks");

            for (const auto & [_, requested_path] : structure_granule.position_to_requested_path)
            {
                if (!structure_state.requested_paths_subcolumns.contains(requested_path))
                    continue;

                auto & path_info = path_to_info[requested_path];
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

    addElementToSubstreamsCache(cache, paths_infos_path, std::make_unique<SubstreamsCachePathsInfosElement>(paths_infos_granules));
    return paths_infos_granules;
}

std::shared_ptr<SerializationObjectSharedData::PathsDataGranules> SerializationObjectSharedData::deserializePathsData(
    const SerializationObjectSharedData::StructureGranules & structure_granules,
    const PathsInfosGranules & paths_infos_granules,
    const SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    const DataTypePtr & dynamic_type,
    const SerializationPtr & dynamic_serialization,
    ISerialization::SubstreamsCache * cache)
{
    settings.path.push_back(Substream::ObjectSharedDataData);
    /// First check if we already deserialized paths data and have it in cache.
    if (auto * cached_paths_data = getElementFromSubstreamsCache(cache, settings.path))
    {
        settings.path.pop_back();
        return assert_cast<SubstreamsCachePathsDataElement *>(cached_paths_data)->paths_data_granules;
    }

    /// Deserialize paths data granule by granule.
    auto * data_stream = settings.getter(settings.path);
    if (!data_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data data");

    auto paths_data_granules = std::make_shared<PathsDataGranules>();
    paths_data_granules->reserve(structure_granules.size());

    if (!settings.seek_stream_to_mark_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read paths from object shared data with ADVANCED serialization version because seek_stream_to_mark_callback is not initialized");

    DeserializeBinaryBulkSettings deserialization_settings;
    deserialization_settings.object_and_dynamic_read_statistics = false;
    deserialization_settings.position_independent_encoding = true;
    deserialization_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    deserialization_settings.data_part_type = MergeTreeDataPartType::Compact;
    deserialization_settings.seek_stream_to_mark_callback = [&](const SubstreamPath &, const MarkInCompressedFile & mark)
    {
        settings.seek_stream_to_mark_callback(settings.path, mark);
    };

    StreamFileNameSettings stream_file_name_settings;
    stream_file_name_settings.escape_variant_substreams = false;

    for (size_t granule = 0; granule != structure_granules.size(); ++granule)
    {
        const auto & structure_granule = structure_granules[granule];
        const auto & path_to_info = paths_infos_granules[granule].path_to_info;
        auto & paths_data_granule = (*paths_data_granules).emplace_back();

        /// Skip granule if there is nothing to read from it.
        if (!structure_granule.limit || path_to_info.empty())
            continue;

        for (const auto & [_, requested_path] : structure_granule.position_to_requested_path)
        {
            auto path_info_it = path_to_info.find(requested_path);
            if (path_info_it == path_to_info.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Info for path {} is not deserialized", requested_path);

            const auto & path_info = path_info_it->second;
            /// Reset callbacks that might be different for different paths.
            deserialization_settings.seek_stream_to_current_mark_callback = {};
            deserialization_settings.getter = {};

            /// If we have only subcolumns requested for this path, read all subcolumns.
            auto paths_subcolumns_it = structure_state.requested_paths_subcolumns.find(requested_path);
            if (paths_subcolumns_it != structure_state.requested_paths_subcolumns.end())
            {
                const auto & subcolumns_infos = paths_subcolumns_it->second;
                std::vector<SubstreamData> subcolumns_substream_data;
                subcolumns_substream_data.reserve(subcolumns_infos.size());
                for (const auto & subcolumn_info : subcolumns_infos)
                    subcolumns_substream_data.push_back(SubstreamData(subcolumn_info.serialization).withType(subcolumn_info.type));

                deserialization_settings.seek_stream_to_current_mark_callback = [&](const SubstreamPath & substream_path)
                {
                    auto stream_name = ISerialization::getFileNameForStream(NameAndTypePair("", dynamic_type), substream_path, stream_file_name_settings);

                    auto it = path_info.substream_to_mark.find(stream_name);
                    if (it == path_info.substream_to_mark.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Substream {} for path {} is requested but not found in substreams list", stream_name, requested_path);

                    /// Seek to the requested substream in the data stream.
                    settings.seek_stream_to_mark_callback(settings.path, it->second);
                };

                deserialization_settings.getter = [&](const SubstreamPath & substream_path) -> ReadBuffer *
                {
                    /// Seek to the requested substream before returning the data stream.
                    deserialization_settings.seek_stream_to_current_mark_callback(substream_path);
                    return data_stream;
                };

                /// First, deserialize prefixes for all subcolumns.
                SubstreamsDeserializeStatesCache deserialize_states_cache;
                for (auto & data : subcolumns_substream_data)
                    data.serialization->deserializeBinaryBulkStatePrefix(deserialization_settings, data.deserialize_state, &deserialize_states_cache);

                /// Second, determine the order of subcolumns deserialization if we have multiple subcolumns of the same path.
                std::vector<size_t> order;
                if (subcolumns_infos.size() == 1)
                {
                    order.push_back(0);
                }
                else
                {
                    EnumerateStreamsSettings enumerate_settings;
                    enumerate_settings.data_part_type = MergeTreeDataPartType::Compact;
                    enumerate_settings.use_specialized_prefixes_and_suffixes_substreams = true;
                    order = getSubcolumnsDeserializationOrder("", subcolumns_substream_data, path_info.substreams, enumerate_settings, stream_file_name_settings);
                }

                /// Finally, deserialize data of subcolumns in determined order.
                SubstreamsCache cache_for_subcolumns;
                for (auto pos : order)
                {
                    ColumnPtr subcolumn = subcolumns_infos[pos].type->createColumn();
                    subcolumns_substream_data[pos].serialization->deserializeBinaryBulkWithMultipleStreams(subcolumn, 0, structure_granule.num_rows, deserialization_settings, subcolumns_substream_data[pos].deserialize_state, &cache_for_subcolumns);
                    paths_data_granule.paths_subcolumns_data[requested_path][subcolumns_infos[pos].name] = std::move(subcolumn);
                }
            }
            /// Otherwise read the whole path data.
            else
            {
                deserialization_settings.getter = [&](const SubstreamPath &) -> ReadBuffer * { return data_stream; };
                settings.seek_stream_to_mark_callback(settings.path, path_info.data_mark);
                DeserializeBinaryBulkStatePtr path_state;
                ColumnPtr dynamic_column = dynamic_type->createColumn();
                dynamic_serialization->deserializeBinaryBulkStatePrefix(deserialization_settings, path_state, nullptr);
                dynamic_serialization->deserializeBinaryBulkWithMultipleStreams(dynamic_column, 0, structure_granule.num_rows, deserialization_settings, path_state, nullptr);
                paths_data_granule.paths_data[requested_path] = std::move(dynamic_column);
            }
        }
    }

    addElementToSubstreamsCache(cache, settings.path, std::make_unique<SubstreamsCachePathsDataElement>(paths_data_granules));
    settings.path.pop_back();

    return paths_data_granules;
}


void SerializationObjectSharedData::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    ISerialization::SubstreamsCache * cache) const
{
    if (!state)
        return;

    auto * shared_data_state = checkAndGetState<DeserializeBinaryBulkStateObjectSharedData>(state);

    if (serialization_version.value == SerializationVersion::MAP)
    {
        /// If we don't have it in cache, deserialize and put deserialized map in cache.
        if (!insertDataFromSubstreamsCacheIfAny(cache, settings, column))
        {
            size_t prev_size = column->size();
            serialization_map->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, shared_data_state->map_state, cache);
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, column->size() - prev_size);
        }
    }
    else if (serialization_version.value == SerializationVersion::MAP_WITH_BUCKETS)
    {
        std::vector<ColumnPtr> shared_data_buckets(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            /// Check if we have map column for this bucket in cache.
            /// Map column for bucket from cache must contain only rows from current deserialization.
            if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
            {
                shared_data_buckets[bucket] = cached_column_with_num_read_rows->first;
            }
            /// If we don't have it in cache, deserialize and put deserialized map in cache.
            else
            {
                shared_data_buckets[bucket] = column->cloneEmpty();
                serialization_map->deserializeBinaryBulkWithMultipleStreams(shared_data_buckets[bucket], rows_offset, limit, settings, shared_data_state->bucket_map_states[bucket], cache);
                addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, shared_data_buckets[bucket], shared_data_buckets[bucket]->size());
            }
            settings.path.pop_back();
        }

        collectSharedDataFromBuckets(shared_data_buckets, *column->assumeMutable());
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED)
    {
        /// Check if we have shared data column in cache.
        if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
            return;

        size_t prev_size = column->size();

        /// In Compact part we always read one whole granule, so we don't need to worry about reading data from multiple granules.
        if (settings.data_part_type == MergeTreeDataPartType::Compact)
        {
            std::vector<String> paths;

            /// Collect all paths stored in this granule in all buckets.
            for (size_t bucket = 0; bucket != buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;

                if (!settings.use_specialized_prefixes_and_suffixes_substreams)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Compact part must use specialized prefixes substreams");

                settings.path.push_back(Substream::ObjectSharedDataStructurePrefix);
                auto * structure_prefix_stream = settings.getter(settings.path);
                if (!structure_prefix_stream)
                    return;

                /// If we have seek_stream_to_current_mark_callback callback, we can seek to the start of each structure
                /// stream in each bucket and don't need to deserialize flattened paths data/marks/substreams/etc.
                if (settings.seek_stream_to_current_mark_callback)
                    settings.seek_stream_to_current_mark_callback(settings.path);

                auto * structure_state = checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_state->bucket_structure_states[bucket]);
                StructureGranule structure_granule;
                deserializeStructureGranulePrefix(*structure_prefix_stream, structure_granule, *structure_state);

                if (structure_granule.num_rows != rows_offset + limit)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected reading a single granule with {} rows, requested {} rows in Compact part in bucket {}", structure_granule.num_rows, rows_offset + limit, bucket);

                paths.insert(paths.end(), structure_granule.all_paths.begin(), structure_granule.all_paths.end());
                settings.path.pop_back();

                /// Skip deserialization of flattened paths data/marks/substreams/etc if we can.
                if (settings.seek_stream_to_current_mark_callback)
                {
                    settings.path.pop_back();
                    continue;
                }

                /// Ignore all other data in all other streams.
                settings.path.push_back(Substream::ObjectSharedDataData);
                auto * data_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!data_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data data");

                DeserializeBinaryBulkSettings deserialization_settings;
                deserialization_settings.object_and_dynamic_read_statistics = false;
                deserialization_settings.position_independent_encoding = true;
                deserialization_settings.use_specialized_prefixes_and_suffixes_substreams = true;
                deserialization_settings.data_part_type = MergeTreeDataPartType::Compact;
                deserialization_settings.getter = [&](const SubstreamPath &) -> ReadBuffer * { return data_stream; };
                for (size_t i = 0; i != structure_granule.num_paths; ++i)
                {
                    ColumnPtr path_column = dynamic_type->createColumn();
                    DeserializeBinaryBulkStatePtr path_state;
                    dynamic_serialization->deserializeBinaryBulkStatePrefix(deserialization_settings, path_state, nullptr);
                    dynamic_serialization->deserializeBinaryBulkWithMultipleStreams(path_column, structure_granule.num_rows, 0, deserialization_settings, path_state, nullptr);
                }

                settings.path.push_back(Substream::ObjectSharedDataPathsMarks);
                auto * paths_marks_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!paths_marks_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths marks");

                paths_marks_stream->ignore(sizeof(UInt64) * structure_granule.num_paths * 2);

                settings.path.push_back(Substream::ObjectSharedDataSubstreams);
                auto * paths_substreams_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!paths_substreams_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths substreams");

                size_t num_substreams;
                size_t total_number_of_substreams = 0;
                for (size_t i = 0; i != structure_granule.num_paths; ++i)
                {
                    readVarUInt(num_substreams, *paths_substreams_stream);
                    total_number_of_substreams += num_substreams;
                    for (size_t j = 0; j != num_substreams; ++j)
                        skipStringBinary(*paths_substreams_stream);
                }

                settings.path.push_back(Substream::ObjectSharedDataSubstreamsMarks);
                auto * substreams_marks_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!substreams_marks_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths substreams marks");

                substreams_marks_stream->ignore(sizeof(UInt64) * total_number_of_substreams * 2);

                settings.path.push_back(Substream::ObjectSharedDataPathsSubstreamsMetadata);
                auto * paths_substreams_metadata_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!paths_substreams_metadata_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths substreams marks");

                paths_substreams_metadata_stream->ignore(sizeof(UInt64) * structure_granule.num_paths * 4);

                settings.path.push_back(Substream::ObjectSharedDataStructureSuffix);
                auto * structure_suffix_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!structure_suffix_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data structure suffix");

                deserializeStructureGranuleSuffix(*structure_suffix_stream, structure_granule);

                settings.path.pop_back();
            }

            /// Now we have the list of paths stored in this granule and can deserialize shared data copy with paths indexes and values.
            auto & shared_data_array_column = assert_cast<ColumnArray &>(*column->assumeMutable());
            auto & shared_data_tuple_column = assert_cast<ColumnTuple &>(shared_data_array_column.getData());
            auto & offsets_column = shared_data_array_column.getOffsetsPtr();
            auto & paths_column = shared_data_tuple_column.getColumn(0);
            auto & values_column = shared_data_tuple_column.getColumn(1);

            settings.path.push_back(Substream::ObjectSharedDataCopy);

            /// Read array sizes.
            settings.path.push_back(Substream::ObjectSharedDataCopySizes);
            if (settings.seek_stream_to_current_mark_callback)
                settings.seek_stream_to_current_mark_callback(settings.path);

            auto [skipped_nested_rows, nested_limit] = SerializationArray::deserializeOffsetsBinaryBulkAndGetNestedOffsetAndLimit(offsets_column, rows_offset, limit, settings, cache);
            settings.path.pop_back();

            /// Read paths indexes.
            settings.path.push_back(Substream::ObjectSharedDataCopyPathsIndexes);
            auto * indexes_stream = settings.getter(settings.path);
            if (!indexes_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data copy indexes");

            deserializeIndexesAndCollectPaths(paths_column, *indexes_stream, std::move(paths), skipped_nested_rows, nested_limit);
            settings.path.pop_back();

            /// Read paths values.
            settings.path.push_back(Substream::ObjectSharedDataCopyValues);
            auto * values_stream = settings.getter(settings.path);
            if (!values_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data copy values");

            SerializationString().deserializeBinaryBulk(values_column, *values_stream, skipped_nested_rows, nested_limit, 0);
            settings.path.pop_back();

            settings.path.pop_back();
        }
        /// In Wide part we can read multiple granules together and read only part of last granule.
        else
        {
            /// Collect list of paths from all buckets for each granule.
            std::vector<std::vector<String>> granules_paths;
            /// Collect offsets and limits for each granule.
            std::vector<size_t> granules_offsets;
            std::vector<size_t> granules_limits;

            if (!settings.continuous_reading)
                shared_data_state->last_incomplete_granule_offset = 0;

            for (size_t bucket = 0; bucket != buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;

                auto * structure_state = checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_state->bucket_structure_states[bucket]);
                /// Read structure for all granules in this bucket.
                auto structure_granules = deserializeStructure(rows_offset, limit, settings, *structure_state, cache);
                if (!structure_granules)
                    return;

                /// Initialize granules_paths/granules_offsets/granules_limits on first bucket.
                if (bucket == 0)
                {
                    granules_paths.resize(structure_granules->size());
                    granules_offsets.reserve(structure_granules->size());
                    granules_limits.reserve(structure_granules->size());
                    for (size_t granule = 0; granule != structure_granules->size(); ++granule)
                    {
                        granules_offsets.push_back((*structure_granules)[granule].offset);
                        /// Offset in the first granule includes rows that we could already read before.
                        if (granule == 0)
                            granules_offsets.back() -= shared_data_state->last_incomplete_granule_offset;
                        granules_limits.push_back((*structure_granules)[granule].limit);
                    }

                    if (!structure_granules->empty())
                    {
                        /// Update last_incomplete_granule_offset if there are remaining rows in the last granule.
                        const auto & last_granule = structure_granules->back();
                        if (last_granule.offset + last_granule.limit < last_granule.num_rows)
                            shared_data_state->last_incomplete_granule_offset = last_granule.offset + last_granule.limit;
                        else
                            shared_data_state->last_incomplete_granule_offset = 0;
                    }
                }

                for (size_t granule = 0; granule != structure_granules->size(); ++granule)
                    granules_paths[granule].insert(granules_paths[granule].end(), (*structure_granules)[granule].all_paths.begin(), (*structure_granules)[granule].all_paths.end());

                settings.path.pop_back();
            }

            /// Now we have a list of all paths stored in this granule. Read shared data copy with paths indexes and values.
            settings.path.push_back(Substream::ObjectSharedDataCopy);

            auto & shared_data_array_column = assert_cast<ColumnArray &>(*column->assumeMutable());
            auto & shared_data_tuple_column = assert_cast<ColumnTuple &>(shared_data_array_column.getData());
            auto & offsets_column = shared_data_array_column.getOffsetsPtr();
            auto & paths_column = shared_data_tuple_column.getColumn(0);
            auto & values_column = shared_data_tuple_column.getColumn(1);

            size_t prev_last_offset = shared_data_array_column.getOffsets().back();
            size_t prev_offset_size = shared_data_array_column.getOffsets().size();

            /// Read array sizes.
            settings.path.push_back(Substream::ObjectSharedDataCopySizes);
            if (!SerializationArray::deserializeOffsetsBinaryBulk(offsets_column, rows_offset + limit, settings, cache))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data copy sizes");

            settings.path.pop_back();

            /// Read paths indexes.
            settings.path.push_back(Substream::ObjectSharedDataCopyPathsIndexes);
            auto * indexes_stream = settings.getter(settings.path);

            if (!indexes_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data copy indexes");

            /// Each granule has its own set of indexes, we should deserialize them granule by granule.
            size_t offsets_current_granule_start = prev_offset_size;
            auto & offsets = shared_data_array_column.getOffsets();
            for (size_t granule = 0; granule != granules_paths.size(); ++granule)
            {
                /// Calculate how many rows should be skipped in this granule.
                size_t nested_offset = offsets[offsets_current_granule_start + granules_offsets[granule] - ssize_t(1)] - offsets[offsets_current_granule_start - ssize_t(1)];
                /// Calculate how many rows should be read in this granule.
                size_t nested_limit
                    = offsets[offsets_current_granule_start + granules_offsets[granule] + granules_limits[granule] - ssize_t(1)]
                    - offsets[offsets_current_granule_start + granules_offsets[granule] - ssize_t(1)];
                /// Read indexes and collect paths into paths_column.
                deserializeIndexesAndCollectPaths(paths_column, *indexes_stream, std::move(granules_paths[granule]), nested_offset, nested_limit);
                offsets_current_granule_start += granules_offsets[granule] + granules_limits[granule];
            }
            settings.path.pop_back();

            /// Values can be read as usual String column from multiple granules.
            /// We need to calculate offset and limit for it based on offsets.
            size_t nested_offset = 0;
            if (rows_offset)
            {
                size_t skipped_idx = std::min(prev_offset_size + rows_offset, offsets.size()) - 1;
                nested_offset = offsets[skipped_idx] - prev_last_offset;

                for (auto i = prev_offset_size; i + rows_offset < offsets.size(); ++i)
                    offsets[i] = offsets[i + rows_offset] - nested_offset;

                offsets_column->assumeMutable()->popBack(rows_offset);
            }

            size_t nested_limit = offsets.back() - prev_last_offset;

            /// Read values.
            settings.path.push_back(Substream::ObjectSharedDataCopyValues);
            auto * values_stream = settings.getter(settings.path);
            SerializationString().deserializeBinaryBulk(values_column, *values_stream, nested_offset, nested_limit, 0);
            settings.path.pop_back();

            settings.path.pop_back();
        }

        addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, column, column->size() - prev_size);
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "deserializeBinaryBulkStatePrefix is not implemented for shared data serialization version {}", serialization_version.value);
    }
}

}
