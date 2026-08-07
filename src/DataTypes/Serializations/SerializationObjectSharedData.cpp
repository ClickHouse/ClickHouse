#include <Common/SipHash.h>
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
#include <Core/Defines.h>
#include <Core/NamesAndTypes.h>
#include <IO/ReadHelpers.h>
#include <algorithm>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// A per-granule count (the number of paths, or the number of substreams of a path) is read from a
/// possibly-untrusted stream (e.g. a corrupted on-disk `Object` part) and used only as a sizing hint
/// before the corresponding items are read one by one. It must not be handed to a container's
/// `reserve` directly, for the same reasons as the outer path lists (see `reserveOrThrowTooManyPaths`
/// in `SerializationObject.cpp`):
///   * A count the container cannot hold (`> max_size()`, close to `SIZE_MAX`) would escape as an
///     uncaught non-`DB::Exception` (`std::length_error`), so reject it as corruption up front.
///   * A large-but-representable count (e.g. `100000000`) is far below `max_size()` for a
///     `std::vector<String>`, yet handing it to `reserve` would allocate gigabytes before a single
///     byte of payload is read and fail as `std::bad_alloc` / OOM.
/// So cap the hint at `DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS`: the caller's read loop appends each
/// item as it is decoded (growing the container on demand for a legitimately large count), while a
/// corrupted over-count trips a normal read error at end of stream instead of a huge allocation.
template <typename Container>
void reserveOrThrowTooMany(Container & container, size_t count, const char * what)
{
    if (count > container.max_size())
        throw Exception(ErrorCodes::INCORRECT_DATA, "JSON/Object column has too many {}: {}", what, count);
    container.reserve(std::min(count, DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS));
}

}

SerializationObjectSharedData::SerializationObjectSharedData(SerializationVersion serialization_version_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, size_t buckets_)
    : serialization_version(serialization_version_)
    , dynamic_type(dynamic_type_)
    , dynamic_serialization(dynamic_serialization_)
    , buckets(buckets_)
    , serialization_map(DataTypeObject::getTypeOfSharedData()->getDefaultSerialization())
{
}

UInt128 SerializationObjectSharedData::getHash(SerializationVersion serialization_version_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, size_t buckets_)
{
    SipHash hash;
    hash.update("ObjectSharedData");
    hash.update(static_cast<int>(serialization_version_.value));
    auto dynamic_type_name = dynamic_type_->getName();
    hash.update(dynamic_type_name.size());
    hash.update(dynamic_type_name);
    hash.update(dynamic_serialization_->getHash());
    hash.update(buckets_);
    return hash.get128();
}

SerializationPtr SerializationObjectSharedData::create(SerializationVersion serialization_version_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, size_t buckets_)
{
    if (!dynamic_serialization_->supportsPooling())
        return std::shared_ptr<ISerialization>(new SerializationObjectSharedData(serialization_version_, dynamic_type_, dynamic_serialization_, buckets_));
    return ISerialization::pooled(getHash(serialization_version_, dynamic_type_, dynamic_serialization_, buckets_), [&] { return new SerializationObjectSharedData(serialization_version_, dynamic_type_, dynamic_serialization_, buckets_); });
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
        case MergeTreeObjectSharedDataSerializationVersion::ADVANCED_CHUNKED:
            value = ADVANCED_CHUNKED;
            break;
    }
}

void SerializationObjectSharedData::SerializationVersion::checkVersion(UInt64 version)
{
    if (version != MAP && version != MAP_WITH_BUCKETS && version != ADVANCED && version != ADVANCED_CHUNKED)
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
    /// Some chunks can be partially read, we need to remember how many rows
    /// were already read from the last incomplete chunk.
    size_t last_incomplete_chunk_offset = 0;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto new_state = std::make_shared<DeserializeBinaryBulkStateObjectSharedData>(*this);
        new_state->map_state = map_state ? map_state->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_map_states.size(); ++bucket)
            new_state->bucket_map_states[bucket] = bucket_map_states[bucket] ? bucket_map_states[bucket]->clone() : nullptr;
        for (size_t bucket = 0; bucket != bucket_structure_states.size(); ++bucket)
            new_state->bucket_structure_states[bucket] = bucket_structure_states[bucket] ? bucket_structure_states[bucket]->clone() : nullptr;
        return new_state;
    }

    void forEachNestedState(const std::function<void(const ISerialization::DeserializeBinaryBulkStatePtr &)> & callback) const override
    {
        if (map_state)
            callback(map_state);
        for (const auto & bucket_map_state : bucket_map_states)
        {
            if (bucket_map_state)
                callback(bucket_map_state);
        }
        for (const auto & bucket_structure_state : bucket_structure_states)
        {
            if (bucket_structure_state)
                callback(bucket_structure_state);
        }
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

    /// Other 3 serializations MAP_WITH_BUCKETS, ADVANCED and ADVANCED_CHUNKED support buckets.
    for (size_t bucket = 0; bucket != buckets; ++bucket)
    {
        settings.path.push_back(Substream::Bucket);
        settings.path.back().bucket = bucket;
        if (serialization_version.value == SerializationVersion::MAP_WITH_BUCKETS)
        {
            auto map_data = SubstreamData(serialization_map)
                                .withColumn(data.column)
                                .withType(data.type)
                                .withSerializationInfo(data.serialization_info)
                                .withDeserializeState(shared_data_state ? shared_data_state->bucket_map_states[bucket] : nullptr);
            serialization_map->enumerateStreams(settings, callback, map_data);
        }
        else if (serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED
                 || serialization_version.value == SerializationObjectSharedData::SerializationVersion::ADVANCED_CHUNKED)
        {
            if (settings.use_specialized_prefixes_and_suffixes_substreams)
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructurePrefix);
            else
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataStructure);

            /// When deserialize state is present, it means the whole shared data will be read
            /// via deserializeBinaryBulkWithMultipleStreams, which only uses Structure + Copy streams.
            /// Per-bucket Data/PathsMarks/Substreams/SubstreamsMarks/PathsSubstreamsMetadata are only
            /// needed when writing or reading individual paths via SerializationObjectSharedDataPath (separate class).
            /// Skip them to avoid unnecessary mark file loads and file opens during prefetching.
            if (!shared_data_state)
            {
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataData);
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataPathsMarks);
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataSubstreams);
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataSubstreamsMarks);
                addSubstreamAndCallCallback(settings.path, callback, Substream::ObjectSharedDataPathsSubstreamsMetadata);
            }

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

    /// Streams related to shared data copy in ADVANCED/ADVANCED_CHUNKED serialization.
    if (serialization_version.value == SerializationVersion::ADVANCED
        || serialization_version.value == SerializationVersion::ADVANCED_CHUNKED)
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
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            serialization_map->serializeBinaryBulkStatePrefix(column, settings, shared_data_state->bucket_map_states[bucket]);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED
             || serialization_version.value == SerializationVersion::ADVANCED_CHUNKED)
    {
        /// ADVANCED/ADVANCED_CHUNKED serialization doesn't have serialization prefix.
    }
    else
    {
        /// If we add new serialization version in future and forget to implement something, better to get an exception instead of doing nothing.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "serializeBinaryBulkStatePrefix is not implemented for shared data serialization version {}", serialization_version.value);
    }

    state = std::move(shared_data_state);
}

namespace
{

/// Metadata collected during serialization of one chunk+bucket's data in ADVANCED/ADVANCED_CHUNKED serialization.
/// Used to write metadata substreams (PathsMarks, Substreams, SubstreamsMarks, PathsSubstreamsMetadata, StructureSuffix).
struct ChunkBucketSerializationMetadata
{
    MarkInCompressedFile data_stream_mark{};
    std::vector<MarkInCompressedFile> paths_marks;
    std::vector<std::vector<String>> paths_substreams;
    std::vector<std::vector<MarkInCompressedFile>> paths_substreams_marks;
};

/// Decompose a shared data column (Array(Tuple(String, String))) into its parts.
struct SharedDataColumns
{
    const IColumn & keys_column;
    const IColumn & values_column;
    const ColumnArray::Offsets & offsets;
    const IColumn & offsets_column;
};

SharedDataColumns extractSharedDataColumns(const IColumn & column)
{
    const auto & array_column = assert_cast<const ColumnArray &>(column);
    const auto & tuple_column = assert_cast<const ColumnTuple &>(array_column.getData());
    return {
        .keys_column = tuple_column.getColumn(0),
        .values_column = tuple_column.getColumn(1),
        .offsets = array_column.getOffsets(),
        .offsets_column = array_column.getOffsetsColumn(),
    };
}

/// Per-substream stream getter helpers. Each pushes the substream type onto settings.path,
/// calls settings.getter, checks for nullptr, and returns the stream.
/// The caller must pop settings.path after it is done writing to the stream.
WriteBuffer & getDataStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataData);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data data");
    return *stream;
}

WriteBuffer & getPathsMarksStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataPathsMarks);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths marks");
    return *stream;
}

WriteBuffer & getSubstreamsStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataSubstreams);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams");
    return *stream;
}

WriteBuffer & getSubstreamsMarksStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataSubstreamsMarks);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data substreams marks");
    return *stream;
}

WriteBuffer & getPathsSubstreamsMetadataStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataPathsSubstreamsMetadata);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data paths substreams metadata");
    return *stream;
}

WriteBuffer & getStructureSuffixStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    ISerialization::Substream structure_stream_type = settings.use_specialized_prefixes_and_suffixes_substreams
        ? ISerialization::Substream::ObjectSharedDataStructureSuffix
        : ISerialization::Substream::ObjectSharedDataStructure;
    settings.path.push_back(structure_stream_type);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data structure suffix");
    return *stream;
}

WriteBuffer & getCopyPathsIndexesStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataCopyPathsIndexes);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data copy indexes");
    return *stream;
}

WriteBuffer & getCopyValuesStream(ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataCopyValues);
    auto * stream = settings.getter(settings.path);
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data copy values");
    return *stream;
}

ChunkBucketSerializationMetadata serializeChunkBucketData(
    const std::vector<std::pair<std::string_view, ColumnPtr>> & flattened_paths,
    WriteBuffer & data_stream,
    ISerialization::SerializeBinaryBulkSettings & settings,
    const DataTypePtr & dynamic_type_,
    const SerializationPtr & dynamic_serialization_,
    MergeTreeObjectSharedDataSerializationVersion nested_shared_data_version)
{
    ChunkBucketSerializationMetadata metadata;

    if (!settings.stream_mark_getter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark getter is not set for ADVANCED shared data serialization");

    /// Remember the mark of the ObjectSharedDataData stream.
    metadata.data_stream_mark = settings.stream_mark_getter(settings.path);

    metadata.paths_marks.reserve(flattened_paths.size());
    metadata.paths_substreams.reserve(flattened_paths.size());
    metadata.paths_substreams_marks.reserve(flattened_paths.size());

    /// Configure serialization settings as in Compact part.
    ISerialization::SerializeBinaryBulkSettings data_serialization_settings;
    data_serialization_settings.data_part_type = MergeTreeDataPartType::Compact;
    data_serialization_settings.position_independent_encoding = true;
    data_serialization_settings.low_cardinality_max_dictionary_size = 0;
    data_serialization_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    data_serialization_settings.use_compact_variant_discriminators_serialization = true;
    data_serialization_settings.dynamic_serialization_version = MergeTreeDynamicSerializationVersion::V3;
    data_serialization_settings.object_serialization_version = MergeTreeObjectSerializationVersion::V3;
    /// Also use the same serialization version for nested Object types
    data_serialization_settings.object_shared_data_serialization_version = nested_shared_data_version;
    data_serialization_settings.object_shared_data_target_chunk_rows = settings.object_shared_data_target_chunk_rows;
    /// Don't write any dynamic statistics.
    data_serialization_settings.write_statistics = ISerialization::SerializeBinaryBulkSettings::StatisticsMode::NONE;
    data_serialization_settings.stream_mark_getter = [&](const ISerialization::SubstreamPath &) -> MarkInCompressedFile { return settings.stream_mark_getter(settings.path); };

    ISerialization::StreamFileNameSettings stream_file_name_settings;
    stream_file_name_settings.escape_variant_substreams = false;

    for (const auto & [path, path_column] : flattened_paths)
    {
        metadata.paths_substreams.emplace_back();
        metadata.paths_substreams_marks.emplace_back();
        data_serialization_settings.getter = [&](const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
        {
            /// Add new substream and its mark for current path.
            metadata.paths_substreams.back().push_back(ISerialization::getFileNameForStream(NameAndTypePair("", dynamic_type_), substream_path, stream_file_name_settings));
            metadata.paths_substreams_marks.back().push_back(settings.stream_mark_getter(settings.path));
            return &data_stream;
        };

        ISerialization::SerializeBinaryBulkStatePtr path_state;
        /// Remember the mark of ObjectSharedDataData stream for this path before writing any data.
        metadata.paths_marks.push_back(settings.stream_mark_getter(settings.path));
        dynamic_serialization_->serializeBinaryBulkStatePrefix(*path_column, data_serialization_settings, path_state);
        dynamic_serialization_->serializeBinaryBulkWithMultipleStreams(*path_column, 0, 0, data_serialization_settings, path_state);
        dynamic_serialization_->serializeBinaryBulkStateSuffix(data_serialization_settings, path_state);
    }

    return metadata;
}

MarkInCompressedFile writePathsMarks(
    const ChunkBucketSerializationMetadata & metadata,
    WriteBuffer & stream,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    /// Remember the mark of the ObjectSharedDataPathsMarks stream for the structure suffix.
    MarkInCompressedFile paths_marks_stream_mark = settings.stream_mark_getter(settings.path);

    for (const auto & mark : metadata.paths_marks)
    {
        writeBinaryLittleEndian(mark.offset_in_compressed_file, stream);
        writeBinaryLittleEndian(mark.offset_in_decompressed_block, stream);
    }

    return paths_marks_stream_mark;
}

std::vector<MarkInCompressedFile> writePathsSubstreams(
    const ChunkBucketSerializationMetadata & metadata,
    WriteBuffer & stream,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    std::vector<MarkInCompressedFile> marks_of_paths_substreams;
    marks_of_paths_substreams.reserve(metadata.paths_substreams.size());
    for (const auto & path_substreams : metadata.paths_substreams)
    {
        marks_of_paths_substreams.push_back(settings.stream_mark_getter(settings.path));
        writeVarUInt(path_substreams.size(), stream);
        for (const auto & substream : path_substreams)
            writeStringBinary(substream, stream);
    }
    return marks_of_paths_substreams;
}

std::vector<MarkInCompressedFile> writeSubstreamsMarks(
    const ChunkBucketSerializationMetadata & metadata,
    WriteBuffer & stream,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    std::vector<MarkInCompressedFile> marks_of_paths_substreams_marks;
    marks_of_paths_substreams_marks.reserve(metadata.paths_substreams_marks.size());
    for (const auto & substreams_marks : metadata.paths_substreams_marks)
    {
        marks_of_paths_substreams_marks.push_back(settings.stream_mark_getter(settings.path));
        for (const auto & mark : substreams_marks)
        {
            writeBinaryLittleEndian(mark.offset_in_compressed_file, stream);
            writeBinaryLittleEndian(mark.offset_in_decompressed_block, stream);
        }
    }
    return marks_of_paths_substreams_marks;
}

MarkInCompressedFile writePathsSubstreamsMetadata(
    const std::vector<MarkInCompressedFile> & marks_of_paths_substreams,
    const std::vector<MarkInCompressedFile> & marks_of_paths_substreams_marks,
    WriteBuffer & stream,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    MarkInCompressedFile paths_substreams_metadata_stream_mark = settings.stream_mark_getter(settings.path);
    for (size_t i = 0; i != marks_of_paths_substreams.size(); ++i)
    {
        writeBinaryLittleEndian(marks_of_paths_substreams[i].offset_in_compressed_file, stream);
        writeBinaryLittleEndian(marks_of_paths_substreams[i].offset_in_decompressed_block, stream);
        writeBinaryLittleEndian(marks_of_paths_substreams_marks[i].offset_in_compressed_file, stream);
        writeBinaryLittleEndian(marks_of_paths_substreams_marks[i].offset_in_decompressed_block, stream);
    }
    return paths_substreams_metadata_stream_mark;
}

void writeStructureSuffix(
    MarkInCompressedFile data_stream_mark,
    MarkInCompressedFile paths_marks_stream_mark,
    MarkInCompressedFile paths_substreams_metadata_stream_mark,
    WriteBuffer & stream)
{
    writeBinaryLittleEndian(data_stream_mark.offset_in_compressed_file, stream);
    writeBinaryLittleEndian(data_stream_mark.offset_in_decompressed_block, stream);
    writeBinaryLittleEndian(paths_marks_stream_mark.offset_in_compressed_file, stream);
    writeBinaryLittleEndian(paths_marks_stream_mark.offset_in_decompressed_block, stream);
    writeBinaryLittleEndian(paths_substreams_metadata_stream_mark.offset_in_compressed_file, stream);
    writeBinaryLittleEndian(paths_substreams_metadata_stream_mark.offset_in_decompressed_block, stream);
}

void writeAllChunkBucketMetadata(
    const ChunkBucketSerializationMetadata & metadata,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    auto & paths_marks_stream = getPathsMarksStream(settings);
    auto paths_marks_stream_mark = writePathsMarks(metadata, paths_marks_stream, settings);
    settings.path.pop_back();

    auto & substreams_stream = getSubstreamsStream(settings);
    auto marks_of_paths_substreams = writePathsSubstreams(metadata, substreams_stream, settings);
    settings.path.pop_back();

    auto & substreams_marks_stream = getSubstreamsMarksStream(settings);
    auto marks_of_paths_substreams_marks = writeSubstreamsMarks(metadata, substreams_marks_stream, settings);
    settings.path.pop_back();

    auto & metadata_stream = getPathsSubstreamsMetadataStream(settings);
    auto paths_substreams_metadata_stream_mark = writePathsSubstreamsMetadata(
        marks_of_paths_substreams, marks_of_paths_substreams_marks, metadata_stream, settings);
    settings.path.pop_back();

    auto & structure_suffix_stream = getStructureSuffixStream(settings);
    writeStructureSuffix(metadata.data_stream_mark, paths_marks_stream_mark, paths_substreams_metadata_stream_mark, structure_suffix_stream);
    settings.path.pop_back();
}

std::unordered_map<std::string_view, size_t> buildPathToIndex(
    const std::vector<std::vector<std::string_view>> & bucket_path_names)
{
    std::unordered_map<std::string_view, size_t> path_to_index;
    size_t index = 0;
    for (const auto & paths : bucket_path_names)
    {
        for (const auto & path : paths)
        {
            path_to_index[path] = index;
            ++index;
        }
    }
    return path_to_index;
}

void writeCopyIndexesForChunk(
    const std::vector<std::vector<std::string_view>> & bucket_path_names,
    const IColumn & keys_column,
    size_t nested_offset, size_t nested_end,
    WriteBuffer & stream)
{
    auto path_to_index = buildPathToIndex(bucket_path_names);
    auto [indexes_column, indexes_type] = createPathsIndexes(path_to_index, keys_column, nested_offset, nested_end);
    indexes_type->getDefaultSerialization()->serializeBinaryBulk(*indexes_column, stream, 0, nested_end - nested_offset);
}

void writeChunkCopySection(
    const IColumn & column,
    size_t offset, size_t end,
    const std::vector<std::vector<std::string_view>> & bucket_path_names,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataCopy);

    auto cols = extractSharedDataColumns(column);
    size_t limit = end - offset;

    /// Write array sizes.
    settings.path.push_back(ISerialization::Substream::ObjectSharedDataCopySizes);
    SerializationArray::serializeOffsetsBinaryBulk(cols.offsets_column, offset, limit, settings);
    settings.path.pop_back();

    size_t nested_offset = offset ? cols.offsets[offset - 1] : 0;
    size_t nested_end = cols.offsets[end - 1];
    size_t nested_limit = nested_end - nested_offset;

    auto & copy_indexes_stream = getCopyPathsIndexesStream(settings);
    writeCopyIndexesForChunk(bucket_path_names, cols.keys_column, nested_offset, nested_end, copy_indexes_stream);
    settings.path.pop_back();

    auto & copy_values_stream = getCopyValuesStream(settings);
    if (nested_limit)
        SerializationString::create()->serializeBinaryBulk(cols.values_column, copy_values_stream, nested_offset, nested_limit);
    settings.path.pop_back();

    settings.path.pop_back();
}

} /// anonymous namespace

/// Deserialize prefix of the chunk in ObjectSharedDataStructure(Prefix) stream.
void SerializationObjectSharedData::deserializeChunkStructurePrefix(
    ReadBuffer & buf,
    ChunkStructure & chunk_structure,
    const DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state)
{
    /// Read number of rows in this chunk.
    readVarUInt(chunk_structure.num_rows, buf);
    String path;
    /// Read number of paths stored in this chunk.
    readVarUInt(chunk_structure.num_paths, buf);

    if (structure_state.need_all_paths)
        reserveOrThrowTooMany(chunk_structure.all_paths, chunk_structure.num_paths, "paths");

    /// Read list of paths.
    for (size_t i = 0; i != chunk_structure.num_paths; ++i)
    {
        readStringBinary(path, buf);
        if (structure_state.requested_paths.contains(path) || structure_state.requested_paths_subcolumns.contains(path) || structure_state.checkIfPathMatchesAnyRequestedPrefix(path))
            chunk_structure.position_to_requested_path[i] = path;

        if (structure_state.need_all_paths)
            chunk_structure.all_paths.push_back(path);
    }
}

/// Deserialize suffix of the chunk in ObjectSharedDataStructure(Suffix) stream.
void SerializationObjectSharedData::deserializeChunkStructureSuffix(ReadBuffer & buf, ChunkStructure & chunk_structure)
{
    readBinaryLittleEndian(chunk_structure.data_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(chunk_structure.data_stream_mark.offset_in_decompressed_block, buf);
    readBinaryLittleEndian(chunk_structure.paths_marks_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(chunk_structure.paths_marks_stream_mark.offset_in_decompressed_block, buf);
    readBinaryLittleEndian(chunk_structure.paths_substreams_metadata_stream_mark.offset_in_compressed_file, buf);
    readBinaryLittleEndian(chunk_structure.paths_substreams_metadata_stream_mark.offset_in_decompressed_block, buf);
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
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            serialization_map->serializeBinaryBulkWithMultipleStreams(*shared_data_buckets[bucket], 0, 0, settings, shared_data_state->bucket_map_states[bucket]);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED)
    {
        size_t end = limit && offset + limit < column.size() ? offset + limit : column.size();
        /// Per-bucket path names for the copy section.
        /// We save them separately because each bucket's flattened columns
        /// are freed after serialization to reduce peak memory.
        std::vector<std::vector<std::string_view>> bucket_path_names(buckets);

        /// Process each bucket separately to limit peak memory —
        /// only one bucket's ColumnDynamic columns are alive at a time.
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            auto flattened_paths = flattenSharedDataPathsForBucket(
                column, offset, end, dynamic_type, bucket, buckets);

            /// Save path names for the copy section.
            bucket_path_names[bucket].reserve(flattened_paths.size());
            for (const auto & [path, _] : flattened_paths)
                bucket_path_names[bucket].push_back(path);

            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;

            /// Write structure prefix: number of rows and list of paths.
            Substream structure_stream_type = settings.use_specialized_prefixes_and_suffixes_substreams ? Substream::ObjectSharedDataStructurePrefix
                                                                                           : Substream::ObjectSharedDataStructure;
            settings.path.push_back(structure_stream_type);
            auto * structure_stream = settings.getter(settings.path);
            settings.path.pop_back();

            if (!structure_stream)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Got empty stream for shared data structure in SerializationObjectSharedData::serializeBinaryBulkWithMultipleStreams");

            writeVarUInt(end - offset, *structure_stream);
            writeVarUInt(flattened_paths.size(), *structure_stream);
            for (const auto & [path, _] : flattened_paths)
                writeStringBinary(path, *structure_stream);

            /// Serialize data and collect metadata, then write metadata substreams + structure suffix.
            auto & data_stream = getDataStream(settings);
            auto metadata = serializeChunkBucketData(flattened_paths, data_stream, settings, dynamic_type, dynamic_serialization, MergeTreeObjectSharedDataSerializationVersion::ADVANCED);
            settings.path.pop_back();
            writeAllChunkBucketMetadata(metadata, settings);

            settings.path.pop_back();
        }

        /// Write Copy section.
        writeChunkCopySection(column, offset, end, bucket_path_names, settings);
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED_CHUNKED)
    {
        size_t end = limit && offset + limit < column.size() ? offset + limit : column.size();
        size_t total_rows = end - offset;
        size_t target_chunk_rows = settings.object_shared_data_target_chunk_rows;

        /// All chunks except the last are exactly target_chunk_rows.
        /// If the last chunk would be smaller than half the target, it is merged with the
        /// previous chunk to avoid a wastefully small trailing chunk. This means the last
        /// chunk can be up to 1.5 * target_chunk_rows. For example with target=10000:
        ///   25000 rows → 10000, 10000, 5000  (5000 >= 5000, keep separate)
        ///   24000 rows → 10000, 14000        (4000 < 5000, merge into previous)
        ///   10001 rows → 10001               (1 < 5000, single chunk)
        size_t num_chunks = total_rows / target_chunk_rows;
        size_t last_chunk_remainder = total_rows % target_chunk_rows;
        if (last_chunk_remainder > 0 && last_chunk_remainder >= target_chunk_rows / 2)
            ++num_chunks;
        num_chunks = std::max<size_t>(num_chunks, 1);

        /// Compute the [start, end) row range for a given chunk index.
        /// All chunks except the last are exactly target_chunk_rows; the last chunk gets all remaining rows.
        auto get_chunk_range = [&](size_t chunk_idx) -> std::pair<size_t, size_t>
        {
            size_t chunk_start = offset + chunk_idx * target_chunk_rows;
            size_t chunk_end = (chunk_idx == num_chunks - 1) ? end : chunk_start + target_chunk_rows;
            return {chunk_start, chunk_end};
        };

        if (settings.data_part_type == MergeTreeDataPartType::Compact)
        {
            /// Compact parts: stream-then-chunks layout.
            /// For each substream, write ALL chunks contiguously.
            /// This requires 4 phases per bucket:
            /// Phase 1: Write StructurePrefix headers (scan path names, no flatten paths columns)
            /// Phase 2: Write Data (flatten + serialize one chunk at a time, buffer metadata)
            /// Phase 3: Write buffered metadata substreams
            /// Phase 4: Write Copy section in stream-then-chunks order

            /// Path names per bucket per chunk, for the copy section.
            /// chunk_bucket_path_names[chunk][bucket] = vector of path names.
            std::vector<std::vector<std::vector<std::string_view>>> chunk_bucket_path_names(num_chunks, std::vector<std::vector<std::string_view>>(buckets));

            for (size_t bucket = 0; bucket != buckets; ++bucket)
            {
                settings.path.push_back(Substream::Bucket);
                settings.path.back().bucket = bucket;

                /// Phase 1: Write all StructurePrefix chunk headers.
                Substream structure_stream_type = Substream::ObjectSharedDataStructurePrefix;
                settings.path.push_back(structure_stream_type);
                auto * structure_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!structure_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Got empty stream for shared data structure in ADVANCED_CHUNKED Compact serialization");

                for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx)
                {
                    auto [chunk_start, chunk_end] = get_chunk_range(chunk_idx);
                    auto path_names = scanPathNamesForBucket(column, chunk_start, chunk_end, bucket, buckets);

                    writeVarUInt(chunk_end - chunk_start, *structure_stream);
                    writeVarUInt(path_names.size(), *structure_stream);
                    for (const auto & path : path_names)
                        writeStringBinary(path, *structure_stream);

                    /// Save path names for the copy section.
                    chunk_bucket_path_names[chunk_idx][bucket] = std::move(path_names);
                }

                /// Phase 2: Write Data for all chunks, buffer metadata.
                std::vector<ChunkBucketSerializationMetadata> chunk_metadata;
                chunk_metadata.reserve(num_chunks);
                auto & data_stream = getDataStream(settings);
                for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx)
                {
                    auto [chunk_start, chunk_end] = get_chunk_range(chunk_idx);
                    auto flattened_paths = flattenSharedDataPathsForBucket(
                        column, chunk_start, chunk_end, dynamic_type, bucket, buckets);

                    chunk_metadata.push_back(serializeChunkBucketData(flattened_paths, data_stream, settings, dynamic_type, dynamic_serialization, MergeTreeObjectSharedDataSerializationVersion::ADVANCED_CHUNKED));
                    /// flattened_paths goes out of scope here, freeing ColumnDynamic memory.
                }
                settings.path.pop_back();

                /// Phase 3: Write buffered metadata in stream-then-chunks order.
                /// Each substream is obtained once (writing one mark in Compact parts),
                /// then all chunks' data for that substream is written contiguously.
                auto & paths_marks_stream = getPathsMarksStream(settings);
                std::vector<MarkInCompressedFile> paths_marks_stream_marks;
                paths_marks_stream_marks.reserve(num_chunks);
                for (const auto & metadata : chunk_metadata)
                    paths_marks_stream_marks.push_back(writePathsMarks(metadata, paths_marks_stream, settings));
                settings.path.pop_back();

                auto & substreams_stream = getSubstreamsStream(settings);
                std::vector<std::vector<MarkInCompressedFile>> all_marks_of_paths_substreams;
                all_marks_of_paths_substreams.reserve(num_chunks);
                for (const auto & metadata : chunk_metadata)
                    all_marks_of_paths_substreams.push_back(writePathsSubstreams(metadata, substreams_stream, settings));
                settings.path.pop_back();

                auto & substreams_marks_stream = getSubstreamsMarksStream(settings);
                std::vector<std::vector<MarkInCompressedFile>> all_marks_of_paths_substreams_marks;
                all_marks_of_paths_substreams_marks.reserve(num_chunks);
                for (const auto & metadata : chunk_metadata)
                    all_marks_of_paths_substreams_marks.push_back(writeSubstreamsMarks(metadata, substreams_marks_stream, settings));
                settings.path.pop_back();

                auto & metadata_stream = getPathsSubstreamsMetadataStream(settings);
                std::vector<MarkInCompressedFile> metadata_stream_marks;
                metadata_stream_marks.reserve(num_chunks);
                for (size_t i = 0; i < chunk_metadata.size(); ++i)
                    metadata_stream_marks.push_back(writePathsSubstreamsMetadata(
                        all_marks_of_paths_substreams[i], all_marks_of_paths_substreams_marks[i], metadata_stream, settings));
                settings.path.pop_back();

                auto & structure_suffix_stream = getStructureSuffixStream(settings);
                for (size_t i = 0; i < chunk_metadata.size(); ++i)
                    writeStructureSuffix(chunk_metadata[i].data_stream_mark, paths_marks_stream_marks[i], metadata_stream_marks[i], structure_suffix_stream);
                settings.path.pop_back();

                settings.path.pop_back();
            }

            /// Phase 4: Write Copy section in stream-then-chunks order.
            /// In Compact parts each substream is contiguous, so we write
            /// all chunks' Sizes first, then all PathsIndexes, then all Values.
            auto cols = extractSharedDataColumns(column);

            settings.path.push_back(Substream::ObjectSharedDataCopy);

            /// Write all chunks' Sizes contiguously.
            /// Get the stream once — in Compact parts each settings.getter call writes a mark.
            settings.path.push_back(Substream::ObjectSharedDataCopySizes);
            auto * sizes_stream = settings.getter(settings.path);
            if (!sizes_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data copy sizes");
            for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx)
            {
                auto [chunk_start, chunk_end] = get_chunk_range(chunk_idx);
                SerializationArray::serializeOffsetsBinaryBulk(cols.offsets_column, chunk_start, chunk_end - chunk_start, *sizes_stream, settings.position_independent_encoding);
            }
            settings.path.pop_back();

            /// Write all chunks' PathsIndexes contiguously.
            auto & copy_indexes_stream = getCopyPathsIndexesStream(settings);
            for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx)
            {
                auto [chunk_start, chunk_end] = get_chunk_range(chunk_idx);
                size_t nested_offset = chunk_start ? cols.offsets[chunk_start - 1] : 0;
                size_t nested_end = cols.offsets[chunk_end - 1];
                writeCopyIndexesForChunk(chunk_bucket_path_names[chunk_idx], cols.keys_column, nested_offset, nested_end, copy_indexes_stream);
            }
            settings.path.pop_back();

            /// Write all chunks' Values contiguously.
            auto & copy_values_stream = getCopyValuesStream(settings);
            for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx)
            {
                auto [chunk_start, chunk_end] = get_chunk_range(chunk_idx);
                size_t nested_offset = chunk_start ? cols.offsets[chunk_start - 1] : 0;
                size_t nested_end = cols.offsets[chunk_end - 1];
                size_t nested_limit = nested_end - nested_offset;
                if (nested_limit)
                    SerializationString::create()->serializeBinaryBulk(cols.values_column, copy_values_stream, nested_offset, nested_limit);
            }
            settings.path.pop_back();

            settings.path.pop_back();
        }
        else
        {
            /// Wide parts: chunk-then-streams layout.
            /// For each chunk, process all buckets (structure + data + metadata), then write copy.
            for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx)
            {
                auto [chunk_start, chunk_end] = get_chunk_range(chunk_idx);
                std::vector<std::vector<std::string_view>> bucket_path_names(buckets);

                for (size_t bucket = 0; bucket != buckets; ++bucket)
                {
                    auto flattened_paths = flattenSharedDataPathsForBucket(
                        column, chunk_start, chunk_end, dynamic_type, bucket, buckets);

                    /// Save path names for the copy section.
                    bucket_path_names[bucket].reserve(flattened_paths.size());
                    for (const auto & [path, _] : flattened_paths)
                        bucket_path_names[bucket].push_back(path);

                    settings.path.push_back(Substream::Bucket);
                    settings.path.back().bucket = bucket;

                    /// Write structure prefix: number of rows and list of paths.
                    Substream structure_stream_type = Substream::ObjectSharedDataStructure;
                    settings.path.push_back(structure_stream_type);
                    auto * structure_stream = settings.getter(settings.path);
                    settings.path.pop_back();

                    if (!structure_stream)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Got empty stream for shared data structure in ADVANCED_CHUNKED Wide serialization");

                    writeVarUInt(chunk_end - chunk_start, *structure_stream);
                    writeVarUInt(flattened_paths.size(), *structure_stream);
                    for (const auto & [path, _] : flattened_paths)
                        writeStringBinary(path, *structure_stream);

                    /// Serialize data and write metadata.
                    auto & data_stream = getDataStream(settings);
                    auto metadata = serializeChunkBucketData(flattened_paths, data_stream, settings, dynamic_type, dynamic_serialization, MergeTreeObjectSharedDataSerializationVersion::ADVANCED_CHUNKED);
                    settings.path.pop_back();
                    writeAllChunkBucketMetadata(metadata, settings);

                    settings.path.pop_back();
                    /// flattened_paths goes out of scope here, freeing ColumnDynamic memory.
                }

                /// Write Copy section for this chunk.
                writeChunkCopySection(column, chunk_start, chunk_end, bucket_path_names, settings);
            }
        }
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
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            serialization_map->serializeBinaryBulkStateSuffix(settings, shared_data_state->bucket_map_states[bucket]);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED
             || serialization_version.value == SerializationVersion::ADVANCED_CHUNKED)
    {
        /// ADVANCED/ADVANCED_CHUNKED doesn't have suffix.
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
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
            serialization_map->deserializeBinaryBulkStatePrefix(settings, shared_data_state->bucket_map_states[bucket], cache);
            settings.path.pop_back();
        }
    }
    else if (serialization_version.value == SerializationVersion::ADVANCED
             || serialization_version.value == SerializationVersion::ADVANCED_CHUNKED)
    {
        shared_data_state->bucket_structure_states.resize(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
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

std::shared_ptr<SerializationObjectSharedData::ChunkStructures> SerializationObjectSharedData::deserializeStructure(
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
        return assert_cast<const SubstreamsCacheStructureElement *>(cached_structure)->chunk_structures;
    }

    auto result = std::make_shared<ChunkStructures>();

    /// In Compact part we always read whole chunks, so we don't need to worry about reading partial data.
    /// For ADVANCED there is exactly one chunk. For ADVANCED_CHUNKED there may be multiple chunks.
    if (settings.data_part_type == MergeTreeDataPartType::Compact)
    {
        if (!settings.use_specialized_prefixes_and_suffixes_substreams)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Compact part must use specialized substreams for prefixes and suffixes");

        /// Read all chunk prefixes from StructurePrefix stream.
        settings.path.push_back(Substream::ObjectSharedDataStructurePrefix);
        auto * structure_prefix_stream = settings.getter(settings.path);
        if (!structure_prefix_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty structure prefix stream for object shared data data");

        /// A Compact granule always contains at least one chunk, and an empty (0-row) granule still
        /// writes one empty chunk. Read at least one chunk so its structure bytes are always consumed.
        size_t total_chunk_rows = 0;
        do
        {
            ChunkStructure chunk_structure;
            deserializeChunkStructurePrefix(*structure_prefix_stream, chunk_structure, structure_state);
            total_chunk_rows += chunk_structure.num_rows;
            result->push_back(std::move(chunk_structure));
        } while (total_chunk_rows < rows_offset + limit);
        settings.path.pop_back();

        if (total_chunk_rows != rows_offset + limit)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected total chunk rows {} to equal requested rows {} in Compact part",
                total_chunk_rows, rows_offset + limit);

        /// Read all chunk suffixes from StructureSuffix stream.
        settings.path.push_back(Substream::ObjectSharedDataStructureSuffix);
        auto * structure_suffix_stream = settings.getter(settings.path);
        if (!structure_suffix_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty structure suffix stream for object shared data");

        for (auto & chunk : *result)
            deserializeChunkStructureSuffix(*structure_suffix_stream, chunk);
        settings.path.pop_back();

        /// Set offset and limit for each chunk. All chunks are read fully.
        /// The rows_offset/limit apply to the overall range across all chunks.
        size_t remaining_offset = rows_offset;
        size_t remaining_limit = limit;
        for (auto & chunk : *result)
        {
            if (remaining_offset >= chunk.num_rows)
            {
                chunk.offset = chunk.num_rows;
                chunk.limit = 0;
                remaining_offset -= chunk.num_rows;
            }
            else
            {
                chunk.offset = remaining_offset;
                size_t available = chunk.num_rows - remaining_offset;
                chunk.limit = std::min(available, remaining_limit);
                remaining_limit -= chunk.limit;
                remaining_offset = 0;
            }
        }
    }
    /// In Wide part we can read multiple chunks together and read only part of last chunk.
    else
    {
        auto * structure_stream = settings.getter(structure_path);

        if (!structure_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty structure stream for object shared data");

        /// Reset last read chunk state if we don't continue reading from it.
        if (!settings.continuous_reading)
            structure_state.last_chunk_structure.clear();

        size_t rows_to_read = limit + rows_offset;
        while (rows_to_read != 0)
        {
            auto & current_chunk = structure_state.last_chunk_structure;

            /// Calculate remaining rows in current chunk that can be read.
            size_t remaining_rows_in_chunk = current_chunk.num_rows - current_chunk.limit - current_chunk.offset;

            /// If there is nothing to read from current chunk - read the next one.
            if (remaining_rows_in_chunk == 0)
            {
                /// Finish if there is no more data in structure stream.
                if (structure_stream->eof())
                    break;

                current_chunk.clear();
                deserializeChunkStructurePrefix(*structure_stream, current_chunk, structure_state);
                deserializeChunkStructureSuffix(*structure_stream, current_chunk);
                remaining_rows_in_chunk = current_chunk.num_rows;
            }

            /// Check if we need to read the whole chunk.
            if (remaining_rows_in_chunk <= rows_to_read)
            {
                /// Check if we need to skip all rows in this chunk.
                if (rows_offset >= remaining_rows_in_chunk)
                {
                    current_chunk.offset = current_chunk.num_rows;
                    current_chunk.limit = 0;
                    rows_offset -= remaining_rows_in_chunk;
                }
                /// Otherwise some rows from this chunk will be read.
                else
                {
                    /// offset and limit in current chunk may be non 0 if we already read from this chunk before.
                    /// We need to start reading starting from last read row (offset + limit)
                    current_chunk.offset += current_chunk.limit + rows_offset;
                    current_chunk.limit = current_chunk.num_rows - current_chunk.offset;
                    rows_offset = 0;
                }

                rows_to_read -= remaining_rows_in_chunk;
            }
            /// Otherwise we read only a part of the chunk.
            else
            {
                /// offset and limit in current chunk may be non 0 if we already read from this chunk before.
                /// We need to start reading starting from last read row (offset + limit)
                current_chunk.offset += current_chunk.limit + rows_offset;
                current_chunk.limit = rows_to_read - rows_offset;
                rows_offset = 0;
                rows_to_read = 0;
            }

            result->push_back(current_chunk);
        }
    }

    /// Add deserialized data into cache.
    addElementToSubstreamsCache(cache, structure_path, std::make_unique<SubstreamsCacheStructureElement>(result));
    return result;
}

std::shared_ptr<SerializationObjectSharedData::PathsInfosChunks> SerializationObjectSharedData::deserializePathsInfos(
    const SerializationObjectSharedData::ChunkStructures & chunk_structures,
    const SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
    ISerialization::DeserializeBinaryBulkSettings & settings,
    ISerialization::SubstreamsCache * cache)
{
    auto paths_infos_path = settings.path;
    paths_infos_path.push_back(Substream::ObjectSharedDataPathsInfos);
    /// First check if we already deserialized paths infos and have it in cache.
    if (auto * cached_paths_infos = getElementFromSubstreamsCache(cache, paths_infos_path))
        return assert_cast<SubstreamsCachePathsInfosElement *>(cached_paths_infos)->paths_infos_chunks;

    /// Deserialize paths infos chunk by chunk.
    auto paths_infos_chunks = std::make_shared<PathsInfosChunks>();
    paths_infos_chunks->reserve(chunk_structures.size());
    for (const auto & chunk_structure : chunk_structures)
    {
        auto & path_to_info = (*paths_infos_chunks).emplace_back().path_to_info;

        /// If there is nothing to read from this chunk, just skip it.
        if (chunk_structure.limit == 0 || chunk_structure.position_to_requested_path.empty())
            continue;

        bool need_paths_marks = false;
        bool need_subcolumns_info = false;
        for (const auto & [_, requested_path] : chunk_structure.position_to_requested_path)
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

            /// We don't read data from marks stream continuously, so we need to seek to the start of this chunk.
            settings.seek_stream_to_mark_callback(settings.path, chunk_structure.paths_marks_stream_mark);

            for (size_t i = 0; i != chunk_structure.num_paths; ++i)
            {
                auto path_it = chunk_structure.position_to_requested_path.find(i);
                /// Skip marks of not requested paths.
                if (path_it == chunk_structure.position_to_requested_path.end())
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

            /// We don't read data from marks stream continuously, so we need to seek to the start of this chunk.
            settings.seek_stream_to_mark_callback(settings.path, chunk_structure.paths_substreams_metadata_stream_mark);
            for (size_t i = 0; i != chunk_structure.num_paths; ++i)
            {
                auto path_it = chunk_structure.position_to_requested_path.find(i);
                /// Skip metadata of not requested paths.
                if (path_it == chunk_structure.position_to_requested_path.end())
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

            for (const auto & [_, requested_path] : chunk_structure.position_to_requested_path)
            {
                if (!structure_state.requested_paths_subcolumns.contains(requested_path))
                    continue;

                auto & path_info = path_to_info[requested_path];
                /// Seek to the start of the substreams list for this path.
                settings.seek_stream_to_mark_callback(settings.path, path_info.substreams_mark);
                size_t num_substreams = 0;
                readVarUInt(num_substreams, *paths_substreams_stream);
                reserveOrThrowTooMany(path_info.substreams, num_substreams, "substreams for a path");
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

            for (const auto & [_, requested_path] : chunk_structure.position_to_requested_path)
            {
                if (!structure_state.requested_paths_subcolumns.contains(requested_path))
                    continue;

                auto & path_info = path_to_info[requested_path];
                /// Seek to the start of the substreams marks for this path.
                settings.seek_stream_to_mark_callback(settings.path, path_info.substreams_marks_mark);
                for (size_t i = 0; i != path_info.substreams.size(); ++i)
                {
                    MarkInCompressedFile substream_mark{};
                    readBinaryLittleEndian(substream_mark.offset_in_compressed_file, *paths_substreams_marks_stream);
                    readBinaryLittleEndian(substream_mark.offset_in_decompressed_block, *paths_substreams_marks_stream);
                    path_info.substream_to_mark[path_info.substreams[i]] = substream_mark;
                }
            }

            settings.path.pop_back();
        }
    }

    addElementToSubstreamsCache(cache, paths_infos_path, std::make_unique<SubstreamsCachePathsInfosElement>(paths_infos_chunks));
    return paths_infos_chunks;
}

std::shared_ptr<SerializationObjectSharedData::PathsDataChunks> SerializationObjectSharedData::deserializePathsData(
    const SerializationObjectSharedData::ChunkStructures & chunk_structures,
    const PathsInfosChunks & paths_infos_chunks,
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
        return assert_cast<SubstreamsCachePathsDataElement *>(cached_paths_data)->paths_data_chunks;
    }

    /// Deserialize paths data chunk by chunk.
    auto * data_stream = settings.getter(settings.path);
    if (!data_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data data");

    auto paths_data_chunks = std::make_shared<PathsDataChunks>();
    paths_data_chunks->reserve(chunk_structures.size());

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

    for (size_t chunk_idx = 0; chunk_idx != chunk_structures.size(); ++chunk_idx)
    {
        const auto & chunk_structure = chunk_structures[chunk_idx];
        const auto & path_to_info = paths_infos_chunks[chunk_idx].path_to_info;
        auto & paths_data_chunk = (*paths_data_chunks).emplace_back();

        /// Skip chunk if there is nothing to read from it.
        if (!chunk_structure.limit || path_to_info.empty())
            continue;

        for (const auto & [_, requested_path] : chunk_structure.position_to_requested_path)
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
                    subcolumns_substream_data[pos].serialization->deserializeBinaryBulkWithMultipleStreams(subcolumn, 0, chunk_structure.num_rows, deserialization_settings, subcolumns_substream_data[pos].deserialize_state, &cache_for_subcolumns);
                    paths_data_chunk.paths_subcolumns_data[requested_path][subcolumns_infos[pos].name] = std::move(subcolumn);
                }

#if defined(DEBUG_OR_SANITIZER_BUILD)
                /// The local `cache_for_subcolumns` and `deserialize_states_cache` (and the per-subcolumn
                /// deserialize states) are dropped when this block ends, before the outer
                /// `SubstreamsCachePathsDataElement` that later covers these subcolumns is created. Verify
                /// here that the reference counts of the just-produced path subcolumns account for those
                /// holders too, so a broken copy-on-write reference count on a shared child (e.g. array
                /// offsets or a LowCardinality dictionary) is not freed at this earlier destruction point
                /// while it is still referenced from a produced subcolumn (issue #105626).
                ColumnsOwnershipValidator ownership_validator;
                ownership_validator.add(cache_for_subcolumns);
                ownership_validator.add(deserialize_states_cache);
                for (const auto & data : subcolumns_substream_data)
                    ownership_validator.add(data.deserialize_state);
                Columns produced_subcolumns;
                const auto & subcolumns_of_path = paths_data_chunk.paths_subcolumns_data[requested_path];
                produced_subcolumns.reserve(subcolumns_of_path.size());
                for (const auto & [_, column] : subcolumns_of_path)
                    produced_subcolumns.push_back(column);
                ownership_validator.validate(produced_subcolumns);
#endif
            }
            /// Otherwise read the whole path data.
            else
            {
                deserialization_settings.getter = [&](const SubstreamPath &) -> ReadBuffer * { return data_stream; };
                settings.seek_stream_to_mark_callback(settings.path, path_info.data_mark);
                DeserializeBinaryBulkStatePtr path_state;
                ColumnPtr dynamic_column = dynamic_type->createColumn();
                dynamic_serialization->deserializeBinaryBulkStatePrefix(deserialization_settings, path_state, nullptr);
                dynamic_serialization->deserializeBinaryBulkWithMultipleStreams(dynamic_column, 0, chunk_structure.num_rows, deserialization_settings, path_state, nullptr);
                paths_data_chunk.paths_data[requested_path] = std::move(dynamic_column);

#if defined(DEBUG_OR_SANITIZER_BUILD)
                /// The local `path_state` is dropped right here, before the outer
                /// `SubstreamsCachePathsDataElement` that later covers the produced column is created.
                /// The state can hold column references through nested states (e.g. nested `Object`
                /// or `LowCardinality` content of the path values), so verify that the reference
                /// count of the just-produced path column accounts for those holders too, and a
                /// broken copy-on-write reference count on a shared child is not freed at this
                /// earlier destruction point while it is still referenced from the produced column
                /// (issue #105626).
                ColumnsOwnershipValidator ownership_validator;
                ownership_validator.add(path_state);
                ownership_validator.validate({paths_data_chunk.paths_data[requested_path]});
#endif
            }
        }
    }

    addElementToSubstreamsCache(cache, settings.path, std::make_unique<SubstreamsCachePathsDataElement>(paths_data_chunks));
    settings.path.pop_back();

    return paths_data_chunks;
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
        Columns shared_data_buckets(buckets);
        for (size_t bucket = 0; bucket != buckets; ++bucket)
        {
            settings.path.push_back(Substream::Bucket);
            settings.path.back().bucket = bucket;
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
    else if (serialization_version.value == SerializationVersion::ADVANCED
             || serialization_version.value == SerializationVersion::ADVANCED_CHUNKED)
    {
        /// Check if we have shared data column in cache.
        if (insertDataFromSubstreamsCacheIfAny(cache, settings, column))
            return;

        size_t prev_size = column->size();

        /// In Compact part we always read whole chunk(s), so we don't need to worry about reading partial data.
        if (settings.data_part_type == MergeTreeDataPartType::Compact)
        {
            /// Per-chunk path lists. Each chunk has its own path-to-index mapping for PathsIndexes.
            std::vector<std::vector<String>> chunks_paths;
            /// Number of rows in each chunk (same across all buckets).
            std::vector<size_t> chunks_num_rows;

            /// Collect paths per chunk from all buckets.
            for (size_t bucket = 0; bucket != buckets; ++bucket)
            {
                settings.path.push_back(Substream::Bucket);
                settings.path.back().bucket = bucket;

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

                /// A Compact granule always contains at least one chunk, and an empty (0-row) granule
                /// still writes one empty chunk, so read at least one chunk to always consume its bytes.
                std::vector<ChunkStructure> chunk_structures;
                size_t total_chunk_rows = 0;
                do
                {
                    ChunkStructure chunk_structure;
                    deserializeChunkStructurePrefix(*structure_prefix_stream, chunk_structure, *structure_state);
                    total_chunk_rows += chunk_structure.num_rows;
                    chunk_structures.push_back(std::move(chunk_structure));
                } while (total_chunk_rows < rows_offset + limit);

                if (total_chunk_rows != rows_offset + limit)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Expected total chunk rows {} to equal requested rows {} in Compact part in bucket {}",
                        total_chunk_rows, rows_offset + limit, bucket);

                settings.path.pop_back();

                /// Initialize per-chunk structures on first bucket.
                if (bucket == 0)
                {
                    chunks_paths.resize(chunk_structures.size());
                    chunks_num_rows.reserve(chunk_structures.size());
                    for (const auto & chunk_structure : chunk_structures)
                        chunks_num_rows.push_back(chunk_structure.num_rows);
                }

                /// Collect paths per chunk.
                for (size_t chunk_idx = 0; chunk_idx < chunk_structures.size(); ++chunk_idx)
                    chunks_paths[chunk_idx].insert(chunks_paths[chunk_idx].end(), chunk_structures[chunk_idx].all_paths.begin(), chunk_structures[chunk_idx].all_paths.end());

                /// Skip deserialization of flattened paths data/marks/substreams/etc if we can.
                if (settings.seek_stream_to_current_mark_callback)
                {
                    settings.path.pop_back();
                    continue;
                }

                /// Ignore all other data in all other streams for all chunks.
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

                for (const auto & chunk_structure : chunk_structures)
                {
                    for (size_t i = 0; i != chunk_structure.num_paths; ++i)
                    {
                        ColumnPtr path_column = dynamic_type->createColumn();
                        DeserializeBinaryBulkStatePtr path_state;
                        dynamic_serialization->deserializeBinaryBulkStatePrefix(deserialization_settings, path_state, nullptr);
                        dynamic_serialization->deserializeBinaryBulkWithMultipleStreams(path_column, chunk_structure.num_rows, 0, deserialization_settings, path_state, nullptr);
                    }
                }

                settings.path.push_back(Substream::ObjectSharedDataPathsMarks);
                auto * paths_marks_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!paths_marks_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths marks");

                for (const auto & chunk_structure : chunk_structures)
                    paths_marks_stream->ignore(sizeof(UInt64) * chunk_structure.num_paths * 2);

                settings.path.push_back(Substream::ObjectSharedDataSubstreams);
                auto * paths_substreams_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!paths_substreams_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths substreams");

                /// Track total substreams per chunk for SubstreamsMarks skipping.
                std::vector<size_t> chunk_total_substreams(chunk_structures.size(), 0);
                for (size_t chunk_idx = 0; chunk_idx != chunk_structures.size(); ++chunk_idx)
                {
                    for (size_t i = 0; i != chunk_structures[chunk_idx].num_paths; ++i)
                    {
                        size_t num_substreams = 0;
                        readVarUInt(num_substreams, *paths_substreams_stream);
                        chunk_total_substreams[chunk_idx] += num_substreams;
                        for (size_t j = 0; j != num_substreams; ++j)
                            skipStringBinary(*paths_substreams_stream);
                    }
                }

                settings.path.push_back(Substream::ObjectSharedDataSubstreamsMarks);
                auto * substreams_marks_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!substreams_marks_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths substreams marks");

                for (size_t chunk_idx = 0; chunk_idx != chunk_structures.size(); ++chunk_idx)
                    substreams_marks_stream->ignore(sizeof(UInt64) * chunk_total_substreams[chunk_idx] * 2);

                settings.path.push_back(Substream::ObjectSharedDataPathsSubstreamsMetadata);
                auto * paths_substreams_metadata_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!paths_substreams_metadata_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data paths substreams metadata");

                for (const auto & chunk_structure : chunk_structures)
                    paths_substreams_metadata_stream->ignore(sizeof(UInt64) * chunk_structure.num_paths * 4);

                settings.path.push_back(Substream::ObjectSharedDataStructureSuffix);
                auto * structure_suffix_stream = settings.getter(settings.path);
                settings.path.pop_back();

                if (!structure_suffix_stream)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data structure suffix");

                for (auto & chunk_structure : chunk_structures)
                    deserializeChunkStructureSuffix(*structure_suffix_stream, chunk_structure);

                settings.path.pop_back();
            }

            /// Now we have per-chunk path lists and can deserialize shared data copy with paths indexes and values.
            auto & shared_data_array_column = assert_cast<ColumnArray &>(*column->assumeMutable());
            auto & shared_data_tuple_column = assert_cast<ColumnTuple &>(shared_data_array_column.getData());
            auto & offsets_column = shared_data_array_column.getOffsetsPtr();
            auto & paths_column = shared_data_tuple_column.getColumn(0);
            auto & values_column = shared_data_tuple_column.getColumn(1);

            size_t prev_last_offset = shared_data_array_column.getOffsets().back();
            size_t prev_offset_size = shared_data_array_column.getOffsets().size();

            settings.path.push_back(Substream::ObjectSharedDataCopy);

            /// Read array sizes for all rows (including rows we'll skip via rows_offset).
            settings.path.push_back(Substream::ObjectSharedDataCopySizes);
            if (settings.seek_stream_to_current_mark_callback)
                settings.seek_stream_to_current_mark_callback(settings.path);

            if (!SerializationArray::deserializeOffsetsBinaryBulk(offsets_column, rows_offset + limit, settings, cache))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for object shared data copy sizes");
            settings.path.pop_back();

            /// Read paths indexes per-chunk. Each chunk has its own path-to-index mapping,
            /// so we calculate per-chunk offset/limit from rows_offset and read indexes in one pass.
            settings.path.push_back(Substream::ObjectSharedDataCopyPathsIndexes);
            auto * indexes_stream = settings.getter(settings.path);
            if (!indexes_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data copy indexes");

            auto & offsets = shared_data_array_column.getOffsets();
            size_t offsets_current_chunk_start = prev_offset_size;
            size_t remaining_offset = rows_offset;
            size_t remaining_limit = limit;
            for (size_t chunk_idx = 0; chunk_idx < chunks_paths.size(); ++chunk_idx)
            {
                size_t chunk_offset = 0;
                size_t chunk_limit = 0;
                if (remaining_offset >= chunks_num_rows[chunk_idx])
                {
                    chunk_offset = chunks_num_rows[chunk_idx];
                    chunk_limit = 0;
                    remaining_offset -= chunks_num_rows[chunk_idx];
                }
                else
                {
                    chunk_offset = remaining_offset;
                    chunk_limit = std::min(chunks_num_rows[chunk_idx] - remaining_offset, remaining_limit);
                    remaining_limit -= chunk_limit;
                    remaining_offset = 0;
                }

                /// Calculate how many nested rows should be skipped in this chunk.
                size_t nested_offset = offsets[offsets_current_chunk_start + chunk_offset - ssize_t(1)] - offsets[offsets_current_chunk_start - ssize_t(1)];
                /// Calculate how many nested rows should be read in this chunk.
                size_t nested_limit
                    = offsets[offsets_current_chunk_start + chunk_offset + chunk_limit - ssize_t(1)]
                    - offsets[offsets_current_chunk_start + chunk_offset - ssize_t(1)];
                /// Read indexes and collect paths into paths_column.
                deserializeIndexesAndCollectPaths(paths_column, *indexes_stream, std::move(chunks_paths[chunk_idx]), nested_offset, nested_limit);
                offsets_current_chunk_start += chunk_offset + chunk_limit;
            }
            settings.path.pop_back();

            /// Fix up offsets to remove skipped rows.
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

            /// Read paths values.
            settings.path.push_back(Substream::ObjectSharedDataCopyValues);
            auto * values_stream = settings.getter(settings.path);
            if (!values_stream)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Got empty stream for shared data copy values");

            SerializationString::create()->deserializeBinaryBulk(values_column, *values_stream, nested_offset, nested_limit, 0);
            settings.path.pop_back();

            settings.path.pop_back();
        }
        /// In Wide part we can read multiple chunks together and read only part of the last chunk.
        else
        {
            /// Collect list of paths from all buckets for each chunk.
            std::vector<std::vector<String>> chunks_paths;
            /// Collect offsets and limits for each chunk.
            std::vector<size_t> chunks_offsets;
            std::vector<size_t> chunks_limits;

            if (!settings.continuous_reading)
                shared_data_state->last_incomplete_chunk_offset = 0;

            for (size_t bucket = 0; bucket != buckets; ++bucket)
            {
                settings.path.push_back(Substream::Bucket);
                settings.path.back().bucket = bucket;

                auto * structure_state = checkAndGetState<DeserializeBinaryBulkStateObjectSharedDataStructure>(shared_data_state->bucket_structure_states[bucket]);
                /// Read structure for all chunks in this bucket.
                auto chunk_structures = deserializeStructure(rows_offset, limit, settings, *structure_state, cache);
                if (!chunk_structures)
                    return;

                /// Initialize chunks_paths/chunks_offsets/chunks_limits on first bucket.
                if (bucket == 0)
                {
                    chunks_paths.resize(chunk_structures->size());
                    chunks_offsets.reserve(chunk_structures->size());
                    chunks_limits.reserve(chunk_structures->size());
                    for (size_t chunk_idx = 0; chunk_idx != chunk_structures->size(); ++chunk_idx)
                    {
                        chunks_offsets.push_back((*chunk_structures)[chunk_idx].offset);
                        /// Offset in the first chunk includes rows that we could already read before.
                        if (chunk_idx == 0)
                            chunks_offsets.back() -= shared_data_state->last_incomplete_chunk_offset;
                        chunks_limits.push_back((*chunk_structures)[chunk_idx].limit);
                    }

                    if (!chunk_structures->empty())
                    {
                        /// Update last_incomplete_chunk_offset if there are remaining rows in the last chunk.
                        const auto & last_chunk = chunk_structures->back();
                        if (last_chunk.offset + last_chunk.limit < last_chunk.num_rows)
                            shared_data_state->last_incomplete_chunk_offset = last_chunk.offset + last_chunk.limit;
                        else
                            shared_data_state->last_incomplete_chunk_offset = 0;
                    }
                }

                for (size_t chunk_idx = 0; chunk_idx != chunk_structures->size(); ++chunk_idx)
                    chunks_paths[chunk_idx].insert(chunks_paths[chunk_idx].end(), (*chunk_structures)[chunk_idx].all_paths.begin(), (*chunk_structures)[chunk_idx].all_paths.end());

                settings.path.pop_back();
            }

            /// Now we have a list of all paths stored in each chunk. Read shared data copy with paths indexes and values.
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

            /// Each chunk has its own set of indexes, we should deserialize them chunk by chunk.
            size_t offsets_current_chunk_start = prev_offset_size;
            auto & offsets = shared_data_array_column.getOffsets();
            for (size_t chunk_idx = 0; chunk_idx != chunks_paths.size(); ++chunk_idx)
            {
                /// Calculate how many nested rows should be skipped in this chunk.
                size_t nested_offset = offsets[offsets_current_chunk_start + chunks_offsets[chunk_idx] - ssize_t(1)] - offsets[offsets_current_chunk_start - ssize_t(1)];
                /// Calculate how many nested rows should be read in this chunk.
                size_t nested_limit
                    = offsets[offsets_current_chunk_start + chunks_offsets[chunk_idx] + chunks_limits[chunk_idx] - ssize_t(1)]
                    - offsets[offsets_current_chunk_start + chunks_offsets[chunk_idx] - ssize_t(1)];
                /// Read indexes and collect paths into paths_column.
                deserializeIndexesAndCollectPaths(paths_column, *indexes_stream, std::move(chunks_paths[chunk_idx]), nested_offset, nested_limit);
                offsets_current_chunk_start += chunks_offsets[chunk_idx] + chunks_limits[chunk_idx];
            }
            settings.path.pop_back();

            /// Values can be read as usual String column from multiple chunks.
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
            SerializationString::create()->deserializeBinaryBulk(values_column, *values_stream, nested_offset, nested_limit, 0);
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
