#include <DataTypes/Serializations/SerializationObjectSharedData.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/Serializations/SerializationString.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Common/logger_useful.h>
#include <Core/NamesAndTypes.h>
#include <boost/algorithm/string/join.hpp>

namespace DB
{

SerializationObjectSharedData::SerializationObjectSharedData(SerializationObjectSharedData::Mode mode_, const DataTypePtr & dynamic_type_, size_t buckets_)
    : mode(mode_)
    , dynamic_type(dynamic_type_)
    , dynamic_serialization(dynamic_type_->getDefaultSerialization())
    , buckets(buckets_)
{
    if (mode == Mode::MAP)
        serialization_map = DataTypeObject::getTypeOfSharedData()->getDefaultSerialization();
}

void SerializationObjectSharedData::enumerateStreams(
    ISerialization::EnumerateStreamsSettings & settings,
    const ISerialization::StreamCallback & callback,
    const ISerialization::SubstreamData & data) const
{
    for (size_t bucket = 0; bucket != buckets; ++bucket)
    {
        settings.path.push_back(Substream::ObjectSharedDataBucket);
        settings.path.back().object_shared_data_bucket = bucket;
        if (mode == Mode::MAP)
        {
            serialization_map->enumerateStreams(settings, callback, data);
        }
        else
        {
            Substream structure_stream_type = settings.use_specialized_prefixes_and_suffixes_substreams ? Substream::ObjectSharedDataStructurePrefix
                                                                                           : Substream::ObjectSharedDataStructure;
            settings.path.push_back(structure_stream_type);
            callback(settings.path);
            settings.path.pop_back();

            settings.path.push_back(Substream::ObjectSharedDataData);
            callback(settings.path);
            settings.path.pop_back();

            settings.path.push_back(Substream::ObjectSharedDataPathsMarks);
            callback(settings.path);
            settings.path.pop_back();

            if (mode == Mode::SEPARATE_SUBSTREAMS)
            {
                settings.path.push_back(Substream::ObjectSharedDataSubstreams);
                callback(settings.path);
                settings.path.pop_back();

                settings.path.push_back(Substream::ObjectSharedDataSubstreamsMarks);
                callback(settings.path);
                settings.path.pop_back();

                settings.path.push_back(Substream::ObjectSharedDataPathsSubstreamsMetadata);
                callback(settings.path);
                settings.path.pop_back();
            }

            if (settings.use_specialized_prefixes_and_suffixes_substreams)
            {
                settings.path.push_back(Substream::ObjectSharedDataStructureSuffix);
                callback(settings.path);
                settings.path.pop_back();
            }
        }

        settings.path.pop_back();
    }

    /// Streams related to shared data copy.
    if (mode == Mode::SEPARATE_PATHS || mode == Mode::SEPARATE_SUBSTREAMS)
    {
        settings.path.push_back(Substream::ObjectSharedDataCopy);

        settings.path.push_back(Substream::ObjectSharedDataCopySizes);
        callback(settings.path);
        settings.path.pop_back();

        settings.path.push_back(Substream::ObjectSharedDataCopyIndexes);
        callback(settings.path);
        settings.path.pop_back();

        settings.path.push_back(Substream::ObjectSharedDataCopyValues);
        callback(settings.path);
        settings.path.pop_back();

        settings.path.pop_back();
    }
}

struct SerializeBinaryBulkStateObjectSharedData : public ISerialization::SerializeBinaryBulkState
{
    std::vector<ISerialization::SerializeBinaryBulkStatePtr> bucket_map_states;
};

struct DeserializeBinaryBulkStateObjectSharedData : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_map_states;
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> bucket_structure_states;

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


void SerializationObjectSharedData::serializeBinaryBulkStatePrefix(
    const IColumn & column,
    ISerialization::SerializeBinaryBulkSettings & settings,
    ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    auto shared_data_state = std::make_shared<SerializeBinaryBulkStateObjectSharedData>();
    if (mode == Mode::MAP)
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
    if (mode == Mode::MAP)
    {
        if (buckets == 1)
        {
            serialization_map->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, shared_data_state->bucket_map_states[0]);
        }
        else
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
    }
    else if (mode == Mode::SEPARATE_PATHS || mode == Mode::SEPARATE_SUBSTREAMS)
    {
        size_t end = limit && offset + limit < column.size() ? offset + limit : column.size();
        auto flattened_paths_buckets = flattenAndBucketSharedDataPaths(column, offset, end, dynamic_type, buckets);

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
            auto data_stream_path = settings.path;
            settings.path.pop_back();

            if (!data_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data data in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            if (!settings.stream_mark_getter)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark getter is not set for SEPARATE_PATHS shared data serialization");

            /// Remember the mark of the data stream, we will write it in the structure stream later.
            MarkInCompressedFile data_stream_mark = settings.stream_mark_getter(data_stream_path);

            /// Collect mark of the data stream for each path.
            std::vector<MarkInCompressedFile> paths_marks;
            paths_marks.reserve(flattened_paths.size());
            /// Collect list of substreams for each path.
            std::vector<std::vector<String>> paths_substreams;
            /// Collect mark of the data stream for each stream of each path.
            std::vector<std::vector<MarkInCompressedFile>> paths_substreams_marks;

            SerializeBinaryBulkSettings data_serialization_settings;
            data_serialization_settings.position_independent_encoding = true;
            data_serialization_settings.low_cardinality_max_dictionary_size = 0;
            data_serialization_settings.use_specialized_prefixes_and_suffixes_substreams = true;
            data_serialization_settings.use_compact_variant_discriminators_serialization = true;
            data_serialization_settings.dynamic_serialization_version = MergeTreeDynamicSerializationVersion::V3;
            if (mode == Mode::SEPARATE_PATHS)
                data_serialization_settings.object_serialization_version = MergeTreeObjectSerializationVersion::V2;
            else
                data_serialization_settings.object_serialization_version = MergeTreeObjectSerializationVersion::V4;
            data_serialization_settings.object_and_dynamic_write_statistics = ISerialization::SerializeBinaryBulkSettings::ObjectAndDynamicStatisticsMode::NONE;
            data_serialization_settings.stream_mark_getter = [&](const SubstreamPath &) -> MarkInCompressedFile { return settings.stream_mark_getter(data_stream_path); };

            for (const auto & [path, path_column] : flattened_paths)
            {
                if (mode == Mode::SEPARATE_SUBSTREAMS)
                {
                    paths_substreams.emplace_back();
                    paths_substreams_marks.emplace_back();
                    data_serialization_settings.getter = [&](const SubstreamPath & substream_path) -> WriteBuffer *
                    {
                        /// Add new substream and its mark for current path.
                        paths_substreams.back().push_back(ISerialization::getFileNameForStream(NameAndTypePair("", dynamic_type), substream_path));
                        paths_substreams_marks.back().push_back(settings.stream_mark_getter(data_stream_path));
                        return data_stream;
                    };
                }
                else
                {
                    data_serialization_settings.getter = [&](const SubstreamPath &) -> WriteBuffer * { return data_stream; };
                }

                SerializeBinaryBulkStatePtr path_state;
                /// Remember the mark of data stream for this path before writing any data.
                paths_marks.push_back(settings.stream_mark_getter(data_stream_path));
                dynamic_serialization->serializeBinaryBulkStatePrefix(*path_column, data_serialization_settings, path_state);
                dynamic_serialization->serializeBinaryBulkWithMultipleStreams(*path_column, 0, 0, data_serialization_settings, path_state);
                dynamic_serialization->serializeBinaryBulkStateSuffix(data_serialization_settings, path_state);
            }

            /// Write paths marks of the data stream.
            settings.path.push_back(Substream::ObjectSharedDataPathsMarks);
            auto * paths_marks_stream = settings.getter(settings.path);
            auto paths_marks_stream_path = settings.path;
            settings.path.pop_back();

            if (!paths_marks_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data paths marks in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            /// Remember the mark of the paths marks stream, we will write it in the structure stream later
            MarkInCompressedFile paths_marks_stream_mark = settings.stream_mark_getter(paths_marks_stream_path);

            for (const auto & mark : paths_marks)
            {
                writeBinaryLittleEndian(mark.offset_in_compressed_file, *paths_marks_stream);
                writeBinaryLittleEndian(mark.offset_in_decompressed_block, *paths_marks_stream);
            }

            MarkInCompressedFile paths_substreams_metadata_stream_mark;
            if (mode == Mode::SEPARATE_SUBSTREAMS)
            {
                /// Write paths substreams.
                settings.path.push_back(Substream::ObjectSharedDataSubstreams);
                auto * paths_substreams_stream = settings.getter(settings.path);
                auto paths_substreams_stream_path = settings.path;
                settings.path.pop_back();

                if (!paths_substreams_stream)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Got empty stream for shared data paths substreams in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

                /// Collect mark of the substreams stream for each path..
                std::vector<MarkInCompressedFile> marks_of_paths_substreams;
                for (const auto & path_substreams : paths_substreams)
                {
                    marks_of_paths_substreams.push_back(settings.stream_mark_getter(paths_substreams_stream_path));
                    /// Write number of substreams of this path and the list of substreams.
                    writeVarUInt(path_substreams.size(), *paths_substreams_stream);
                    for (const auto & substream : path_substreams)
                        writeStringBinary(substream, *paths_substreams_stream);
                }

                /// Write substreams marks.
                settings.path.push_back(Substream::ObjectSharedDataSubstreamsMarks);
                auto * substreams_marks_stream = settings.getter(settings.path);
                auto substreams_marks_stream_path = settings.path;
                settings.path.pop_back();

                if (!substreams_marks_stream)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Got empty stream for shared data substreams marks in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

                /// Collect mark of the substreams marks stream for each path.
                std::vector<MarkInCompressedFile> marks_of_paths_substreams_marks;
                for (const auto & substreams_marks : paths_substreams_marks)
                {
                    marks_of_paths_substreams_marks.push_back(settings.stream_mark_getter(substreams_marks_stream_path));
                    for (const auto & mark : substreams_marks)
                    {
                        writeBinaryLittleEndian(mark.offset_in_compressed_file, *substreams_marks_stream);
                        writeBinaryLittleEndian(mark.offset_in_decompressed_block, *substreams_marks_stream);
                    }
                }

                /// For each path write mark of its substreams list and mark of their marks.
                settings.path.push_back(Substream::ObjectSharedDataPathsSubstreamsMetadata);
                auto * paths_substreams_metadata_stream = settings.getter(settings.path);
                auto paths_substreams_metadata_stream_path = settings.path;
                settings.path.pop_back();

                if (!paths_substreams_metadata_stream)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Got empty stream for shared data paths substreams metadata in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

                paths_substreams_metadata_stream_mark = settings.stream_mark_getter(paths_substreams_metadata_stream_path);
                for (size_t i = 0; i != marks_of_paths_substreams.size(); ++i)
                {
                    writeBinaryLittleEndian(marks_of_paths_substreams[i].offset_in_compressed_file, *paths_substreams_metadata_stream);
                    writeBinaryLittleEndian(marks_of_paths_substreams[i].offset_in_decompressed_block, *paths_substreams_metadata_stream);
                    writeBinaryLittleEndian(marks_of_paths_substreams_marks[i].offset_in_compressed_file, *paths_substreams_metadata_stream);
                    writeBinaryLittleEndian(marks_of_paths_substreams_marks[i].offset_in_decompressed_block, *paths_substreams_metadata_stream);
                }
            }

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

            if (mode == Mode::SEPARATE_SUBSTREAMS)
            {
                writeBinaryLittleEndian(paths_substreams_metadata_stream_mark.offset_in_compressed_file, *structure_stream);
                writeBinaryLittleEndian(paths_substreams_metadata_stream_mark.offset_in_decompressed_block, *structure_stream);
            }

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
        auto * copy_sizes_stream = settings.getter(settings.path);

        if (!copy_sizes_stream)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Got empty stream for shared data copy sizes in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

        SerializationArray::serializeOffsetsBinaryBulk(shared_data_offsets_column, offset, limit, settings);
        settings.path.pop_back();

        size_t nested_offset = offset ? shared_data_offsets[offset - 1] : 0;
        size_t nested_limit = limit ? shared_data_offsets[end - 1] - nested_offset : shared_data_offsets.back() - nested_offset;
        size_t nested_end = nested_offset + nested_limit;

        settings.path.push_back(Substream::ObjectSharedDataCopyIndexes);
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
        for (const auto & bucket_paths : flattened_paths_buckets)
        {
            for (const auto & [path, _] : bucket_paths)
            {
                path_to_index[path] = index;
                ++index;
            }
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
        SerializationString().serializeBinaryBulk(values_column, *copy_values_stream, nested_offset, nested_limit);
        settings.path.pop_back();

        settings.path.pop_back();
    }
}

void SerializationObjectSharedData::serializeBinaryBulkStateSuffix(
    ISerialization::SerializeBinaryBulkSettings & settings, ISerialization::SerializeBinaryBulkStatePtr & state) const
{
    auto * shared_data_state = checkAndGetState<SerializeBinaryBulkStateObjectSharedData>(state);
    if (mode == Mode::MAP)
    {
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            serialization_map->serializeBinaryBulkStateSuffix(settings, shared_data_state->bucket_map_states[bucket]);
            settings.path.pop_back();
        }
    }
}

void SerializationObjectSharedData::deserializeBinaryBulkStatePrefix(
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    ISerialization::SubstreamsDeserializeStatesCache * cache) const
{
    auto shared_data_state = std::make_shared<DeserializeBinaryBulkStateObjectSharedData>();
    if (mode == Mode::MAP)
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
    else
    {
        shared_data_state->bucket_structure_states.resize(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::ObjectSharedDataBucket);
            settings.path.back().object_shared_data_bucket = bucket;
            shared_data_state->bucket_structure_states[bucket] = deserializeStructureStatePrefix(settings, cache);
            auto * structure_state_concrete = checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_state->bucket_structure_states[bucket]);
            structure_state_concrete->read_all_paths = true;
            structure_state_concrete->requested_paths.clear();
            settings.path.pop_back();
        }
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
    const std::unordered_set<String> & requested_paths,
    bool read_all_paths)
{
    /// Read number of rows in this granule.
    readVarUInt(structure_granule.num_rows, buf);
    String path;
    /// Read number of paths stored in this granule.
    readVarUInt(structure_granule.num_paths, buf);
    /// Read list of paths.
    for (size_t i = 0; i != structure_granule.num_paths; ++i)
    {
        readStringBinary(path, buf);
        if (read_all_paths || requested_paths.contains(path))
        {
            structure_granule.paths.push_back(path);
            structure_granule.paths_set.insert(path);
            structure_granule.original_position_to_local_position[i] = structure_granule.paths.size() - 1;
        }
    }
}

void SerializationObjectSharedData::deserializeStructureGranuleSuffix(ReadBuffer & buf, SerializationObjectSharedData::StructureGranule & structure_granule, Mode mode)
{
    readBinaryLittleEndian(structure_granule.data_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(structure_granule.data_stream_mark.offset_in_decompressed_block, buf);
    readBinaryLittleEndian(structure_granule.paths_marks_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(structure_granule.paths_marks_stream_mark.offset_in_decompressed_block, buf);
    if (mode == Mode::SEPARATE_SUBSTREAMS)
    {
        readBinaryLittleEndian(structure_granule.paths_substreams_metadata_stream_mark.offset_in_compressed_file, buf);
        readBinaryLittleEndian(structure_granule.paths_substreams_metadata_stream_mark.offset_in_decompressed_block, buf);
    }
}

std::shared_ptr<SerializationObjectSharedData::StructureGranules> SerializationObjectSharedData::deserializeStructure(
    size_t rows_offset,
    size_t limit,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
    ISerialization::SubstreamsCache * cache,
    Mode mode)
{
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
        deserializeStructureGranulePrefix(*structure_prefix_stream, structure_granule, structure_state.requested_paths, structure_state.read_all_paths);
        settings.path.pop_back();

        if (structure_granule.num_rows != rows_offset + limit)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected reading a single granule with {} rows, requested {} rows in Compact part", structure_granule.num_rows, rows_offset + limit);

        /// Read suffix of the structure stream.
        settings.path.push_back(Substream::ObjectSharedDataStructureSuffix);
        auto * structure_suffix_stream = settings.getter(settings.path);
        deserializeStructureGranuleSuffix(*structure_suffix_stream, structure_granule, mode);
        settings.path.pop_back();

        result->push_back(std::move(structure_granule));
    }
    /// In Wide part we can read multiple granules together and read only part of last granule.
    else
    {
        settings.path.push_back(Substream::ObjectSharedDataStructure);
        /// First check if we already deserialized data from structure steam and have it in the cache.
        if (auto * cached_structure = getElementFromSubstreamsCache(cache, settings.path))
        {
            settings.path.pop_back();
            return assert_cast<SubstreamsCacheStructureElement *>(cached_structure)->structure_granules;
        }

        auto * structure_stream = settings.getter(settings.path);

        /// Reset last read granule state if we don't continue reading from it.
        if (!settings.continuous_reading)
            structure_state.last_granule_structure.clear();

        size_t rows_to_read = limit + rows_offset;
        StructureGranule current_granule;
        std::swap(structure_state.last_granule_structure, current_granule);
        while (rows_to_read != 0)
        {
            /// Calculate remaining rows in current granule that can be read.
            size_t remaining_rows_in_granule = current_granule.num_rows - current_granule.limit - current_granule.offset;

            /// If there is nothing to read from current granule - read the next one.
            if (remaining_rows_in_granule == 0)
            {
                /// Finish if there is no more data in structure stream.
                if (structure_stream->eof())
                    break;

                current_granule.clear();
                deserializeStructureGranulePrefix(*structure_stream, current_granule, structure_state.requested_paths, structure_state.read_all_paths);
                deserializeStructureGranuleSuffix(*structure_stream, current_granule, mode);
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
            current_granule.clear();
        }

        /// Remember the state of the last read granule as it can be only partially read.
        if (!result->empty())
            structure_state.last_granule_structure = result->back();

        addElementToSubstreamsCache(cache, settings.path, std::make_unique<SubstreamsCacheStructureElement>(result));
        settings.path.pop_back();
    }

    return result;
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

    if (mode == Mode::MAP)
    {
        if (buckets == 1)
        {
            serialization_map->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, shared_data_state->bucket_map_states[0], cache);
        }
        else
        {
            std::vector<ColumnPtr> shared_data_buckets(buckets);
            for (size_t bucket = 0; bucket != buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;
                shared_data_buckets[bucket] = column->cloneEmpty();
                serialization_map->deserializeBinaryBulkWithMultipleStreams(shared_data_buckets[bucket], rows_offset, limit, settings, shared_data_state->bucket_map_states[bucket], cache);
                settings.path.pop_back();
            }

            collectSharedDataFromBuckets(shared_data_buckets, *column->assumeMutable());
        }
    }
    else if (mode == Mode::SEPARATE_PATHS || mode == Mode::SEPARATE_SUBSTREAMS)
    {
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

                StructureGranule structure_granule;
                deserializeStructureGranulePrefix(*structure_prefix_stream, structure_granule, {}, true);

                if (structure_granule.num_rows != rows_offset + limit)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected reading a single granule with {} rows, requested {} rows in Compact part", structure_granule.num_rows, rows_offset + limit);

                for (const auto & path : structure_granule.paths)
                    paths.push_back(path);

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

                if (mode == Mode::SEPARATE_SUBSTREAMS)
                {
                    settings.path.push_back(Substream::ObjectSharedDataSubstreams);
                    auto * paths_substreams_stream = settings.getter(settings.path);
                    settings.path.pop_back();

                    if (!paths_substreams_stream)
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths substreams");

                    size_t num_substreams;
                    String substream;
                    size_t total_number_of_substreams = 0;
                    for (size_t i = 0; i != structure_granule.num_paths; ++i)
                    {
                        readVarUInt(num_substreams, *paths_substreams_stream);
                        total_number_of_substreams += num_substreams;
                        for (size_t j = 0; j != num_substreams; ++j)
                            readStringBinary(substream, *paths_substreams_stream);
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
                }

                settings.path.push_back(Substream::ObjectSharedDataStructureSuffix);
                auto * structure_suffix_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!structure_suffix_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data structure suffix");

                deserializeStructureGranuleSuffix(*structure_suffix_stream, structure_granule, mode);

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
            settings.path.push_back(Substream::ObjectSharedDataCopyIndexes);
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

            for (size_t bucket = 0; bucket != buckets; ++bucket)
            {
                settings.path.push_back(Substream::ObjectSharedDataBucket);
                settings.path.back().object_shared_data_bucket = bucket;

                auto * structure_state = checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_state->bucket_structure_states[bucket]);
                /// Remember how many rows were already read from last granule if it's incomplete.
                size_t rows_read_in_last_granule = 0;
                if (settings.continuous_reading && structure_state->last_granule_structure.offset + structure_state->last_granule_structure.limit < structure_state->last_granule_structure.num_rows)
                    rows_read_in_last_granule = structure_state->last_granule_structure.offset + structure_state->last_granule_structure.limit;

                /// Read structure for all granules in this bucket.
                auto structure_granules = deserializeStructure(rows_offset, limit, settings, *structure_state, cache, mode);
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
                        /// Offset in the first granule includes rows that were already read before.
                        if (granule == 0)
                            granules_offsets.back() -= rows_read_in_last_granule;
                        granules_limits.push_back((*structure_granules)[granule].limit);
                    }
                }

                for (size_t granule = 0; granule != structure_granules->size(); ++granule)
                {
                    for (const auto & path : (*structure_granules)[granule].paths)
                        granules_paths[granule].push_back(path);
                }

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
            SerializationArray::deserializeOffsetsBinaryBulk(offsets_column, rows_offset + limit, settings, cache);
            settings.path.pop_back();

            /// Read paths indexes.
            settings.path.push_back(Substream::ObjectSharedDataCopyIndexes);
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
                    offsets[i] = offsets[i + rows_offset] - skipped_nested_rows;

                offsets_column->assumeMutable()->popBack(rows_offset);
            }

            size_t nested_limit = offsets.back() - prev_last_offset;

            /// Read values.
            settings.path.push_back(Substream::ObjectSharedDataCopyValues);
            auto * values_stream = settings.getter(settings.path);
            SerializationString().deserializeBinaryBulk(values_column, *values_stream, skipped_nested_rows, nested_limit, 0);
            settings.path.pop_back();

            settings.path.pop_back();
        }
    }
}

}
