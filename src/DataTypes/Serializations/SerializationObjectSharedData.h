#pragma once
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation-html"

#include <Common/Exception.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <boost/algorithm/string/join.hpp>

#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class SerializationObjectSharedDataPath;
class SerializationSubObjectSharedData;
class SerializationObjectDistinctPaths;

/// Class for binary serialization/deserialization of a shared data structure inside Object type.
class SerializationObjectSharedData final : public SimpleTextSerialization
{
public:
    struct SerializationVersion
    {
        enum Value
        {
            /// Standard serialization of shared data as Map(String, String), the same as it's stored in memory.
            MAP = 0,
            /// The same as MAP but the data is separated into N buckets (same Map columns) that are stored separately.
            MAP_WITH_BUCKETS = 1,
            /// Advanced serialization that implements logic similar to the Compact part inside each
            /// chunk of shared data for faster reading of individual paths from shared data.
            /// There are several streams in this serialization:
            /// - ObjectSharedDataStructure stream (in Compact parts it's separated into Prefix and Suffix):
            ///     <number of rows stored in the chunk>                                                ─┐
            ///     <number of paths stored in the chunk>                                                │   <- Prefix
            ///     <list of paths>                                                                     ─┘
            ///     <mark of the ObjectSharedDataData stream in this chunk>                             ─┐
            ///     <mark of the ObjectSharedDataPathsMarks stream in this chunk>                        │   <- Suffix
            ///     <mark of the ObjectSharedDataPathsSubstreamsMetadata stream in this chunk>          ─┘
            /// - ObjectSharedDataData stream:
            ///     <data of all paths as a single chunk in Compact part format>
            /// - ObjectSharedDataPathsMarks stream:
            ///     <mark of the beginning of each path data in ObjectSharedDataData stream>
            /// - ObjectSharedDataSubstreams stream:
            ///     <list of substreams for each path>
            /// - ObjectSharedDataSubstreamsMarks stream:
            ///     <mark of the beginning of each path substream data in ObjectSharedDataData stream>
            /// - ObjectSharedDataPathsSubstreamsMetadata stream:
            ///     <marks of the beginning of the substreams list and substreams marks of each path in ObjectSharedDataSubstreams and ObjectSharedDataSubstreamsMarks streams>
            /// In this serialization shared data can be also separated into buckets and each bucket will have all streams described above.
            /// There are a few more streams that are not separated into buckets, they are used to store a copy of the paths data for
            /// faster reading of the whole shared data:
            /// - ObjectSharedDataCopySizes stream:
            ///     <array sizes of the original shared data Map column>
            /// - ObjectSharedDataCopyPathsIndexes stream:
            ///     <indexes of paths stored in the shared data Map key column in a combined list of paths serialized in all buckets in ObjectSharedDataStructure stream>
            /// - ObjectSharedDataCopyValues stream:
            ///     <data of the shared data Map values column>
            ADVANCED = 2,
            /// The same as ADVANCED but with support for splitting rows into multiple chunks
            /// to reduce peak memory during serialization.
            /// In Wide parts: chunk-then-streams layout (each chunk writes all substreams sequentially).
            /// In Compact parts: stream-then-chunks layout (each substream writes all chunks contiguously).
            ADVANCED_CHUNKED = 3,
        };

        Value value;

        static void checkVersion(UInt64 version);

        explicit SerializationVersion(UInt64 version);
        explicit SerializationVersion(MergeTreeObjectSharedDataSerializationVersion version);
        explicit SerializationVersion(Value value_) : value(value_) {}
    };

private:
    SerializationObjectSharedData(SerializationVersion serialization_version_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, size_t buckets_);

public:
    static UInt128 getHash(SerializationVersion serialization_version_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, size_t buckets_);
    static SerializationPtr create(SerializationVersion serialization_version_, const DataTypePtr & dynamic_type_, const SerializationPtr & dynamic_serialization_, size_t buckets_);

    bool supportsPooling() const override { return dynamic_serialization->supportsPooling(); }

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override { throwNoSerialization(); }
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }
    bool tryDeserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override { throwNoSerialization(); }

private:
    /// Structure that represents the state of the data in a single chunk deserialized from ObjectSharedDataStructure stream.
    /// This structure contains all information that is needed to deserialize the data of a specific paths/their subcolumns from
    /// the same chunk.
    struct ChunkStructure
    {
        /// Map from the position of the path in the paths list to the requested path.
        /// We use std::map because we need positions to be ordered.
        std::map<size_t, String> position_to_requested_path;
        /// List of all paths stored in the chunk. It is filled only if whole
        /// shared data is deserialized when we need the list of all paths.
        std::vector<String> all_paths;
        /// Number of rows in this chunk.
        size_t num_rows = 0;
        /// How much rows should be read from this chunk. Can be less than num_rows if we read only a part of the chunk.
        size_t limit = 0;
        /// How much rows should be skipped in this chunk before reading.
        size_t offset = 0;
        /// Total number of paths in this chunk, not only requested ones.
        size_t num_paths = 0;
        /// Mark of the ObjectSharedDataData stream for this chunk.
        MarkInCompressedFile data_stream_mark{};
        /// Mark of the ObjectSharedDataPathsMarks stream for this chunk.
        MarkInCompressedFile paths_marks_stream_mark{};
        /// Mark of the ObjectSharedDataPathsSubstreamsMetadata stream for this chunk.
        MarkInCompressedFile paths_substreams_metadata_stream_mark{};

        void clear()
        {
            position_to_requested_path.clear();
            all_paths.clear();
            num_rows = 0;
            limit = 0;
            offset = 0;
            num_paths = 0;
        }
    };

    using ChunkStructures = std::vector<ChunkStructure>;

    /// We deserialize data from ObjectSharedDataStructure stream only once and then use substreams cache to get it if needed.
    /// We can read more than 1 chunk at a time, so we need to be able to store a list of ChunkStructure structures in the cache.
    struct SubstreamsCacheStructureElement : public ISerialization::ISubstreamsCacheElement
    {
        explicit SubstreamsCacheStructureElement(std::shared_ptr<ChunkStructures> chunk_structures_) : chunk_structures(chunk_structures_) {}

        std::shared_ptr<ChunkStructures> chunk_structures;
    };

    /// State of the deserialization from ObjectSharedDataStructure stream
    struct DeserializeBinaryBulkStateObjectSharedDataStructure : public ISerialization::DeserializeBinaryBulkState
    {
        /// List of all requested paths that needs to be read from the shared data.
        std::unordered_set<String> requested_paths;
        /// List of all paths subcolumns that needs to be read from the shared data.
        /// If path exists in requested_paths it cannot exist in requested_paths_subcolumns,
        /// because if the whole path is requested we extract subcolumns from it in memory
        /// and don't deserialize it.
        struct SubcolumnInfo
        {
            String name;
            DataTypePtr type;
            SerializationPtr serialization;
        };
        std::unordered_map<String, std::vector<SubcolumnInfo>> requested_paths_subcolumns;
        /// List of paths prefixes that were requested by reading a sub-object, all paths
        /// that matches these prefixes needs to be read.
        std::vector<String> requested_paths_prefixes;
        /// If we read the whole shared data we need the list of all paths stored in each chunk.
        bool need_all_paths = false;

        /// We can read only a part of the chunk, so we need to remember the state of the last read chunk.
        ChunkStructure last_chunk_structure;

        ISerialization::DeserializeBinaryBulkStatePtr clone() const override
        {
            return std::make_shared<DeserializeBinaryBulkStateObjectSharedDataStructure>(*this);
        }

        bool checkIfPathMatchesAnyRequestedPrefix(const String & path) const
        {
            return std::ranges::any_of(requested_paths_prefixes, [&](const auto & prefix) { return path.starts_with(prefix); });
        }
    };

    static DeserializeBinaryBulkStatePtr deserializeStructureStatePrefix(DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache);

    /// Deserialize data from ObjectSharedDataStructure stream with specified limit.
    static std::shared_ptr<ChunkStructures> deserializeStructure(
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
        SubstreamsCache * cache);

    /// Deserialize prefix of the chunk in ObjectSharedDataStructure(Prefix) stream.
    static void deserializeChunkStructurePrefix(
        ReadBuffer & buf,
        ChunkStructure & chunk_structure,
        const DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state);

    /// Deserialize suffix of the chunk in ObjectSharedDataStructure(Suffix) stream.
    static void deserializeChunkStructureSuffix(ReadBuffer & buf, ChunkStructure & chunk_structure);

    /// Struct that contains all information about path that is required to read
    /// its data or its subcolumn data from ADVANCED shared data serialization.
    /// This data is deserialized from 4 different substreams:
    ///   - ObjectSharedDataPathsMarks for path mark
    ///   - ObjectSharedDataSubstreamsMetadata for path substreams metadata
    ///   - ObjectSharedDataSubstreams for list of substreams for this path
    ///   - ObjectSharedDataSubstreamsMarks for substreams marks for this path.
    struct PathInfo
    {
        /// Mark of the ObjectSharedDataData stream for this path.
        MarkInCompressedFile data_mark{};
        /// Mark of the substreams list in ObjectSharedDataSubstreams stream for this path.
        MarkInCompressedFile substreams_mark{};
        /// Mark of the substreams marks in ObjectSharedDataSubstreamsMarks stream for this path.
        MarkInCompressedFile substreams_marks_mark{};
        /// List of substreams for this path.
        std::vector<String> substreams;
        /// Map Substream -> its mark in ObjectSharedDataData stream.
        std::unordered_map<std::string_view, MarkInCompressedFile> substream_to_mark;
    };

    struct PathsInfos
    {
        std::unordered_map<String, PathInfo> path_to_info;
    };

    using PathsInfosChunks = std::vector<PathsInfos>;

    /// We deserialize paths infos only once and then put it in the cache.
    struct SubstreamsCachePathsInfosElement : public ISubstreamsCacheElement
    {
        explicit SubstreamsCachePathsInfosElement(std::shared_ptr<PathsInfosChunks> paths_infos_chunks_) : paths_infos_chunks(paths_infos_chunks_) {}

        std::shared_ptr<PathsInfosChunks> paths_infos_chunks;
    };

    static std::shared_ptr<PathsInfosChunks> deserializePathsInfos(
        const SerializationObjectSharedData::ChunkStructures & chunk_structures,
        const SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
        DeserializeBinaryBulkSettings & settings,
        SubstreamsCache * cache);

    /// Struct that contains actual data from ObjectSharedDataData stream
    /// for specific paths and their subcolumns.
    struct PathsData
    {
        /// Data of the all requested paths in this chunk.
        std::unordered_map<String, ColumnPtr> paths_data;
        /// Data of the all requested paths subcolumns in this chunk.
        /// If paths is inside paths_data, it cannot be inside paths_subcolumns_data,
        /// so if we need subcolumn in this case we will extract it in memory.
        std::unordered_map<String, std::unordered_map<String, ColumnPtr>> paths_subcolumns_data;
    };

    using PathsDataChunks = std::vector<PathsData>;

    /// We deserialize paths data only once and then put it in the cache.
    struct SubstreamsCachePathsDataElement : public ISubstreamsCacheElement
    {
        explicit SubstreamsCachePathsDataElement(std::shared_ptr<PathsDataChunks> paths_data_chunks_) : paths_data_chunks(paths_data_chunks_) {}

        std::shared_ptr<PathsDataChunks> paths_data_chunks;
    };

    static std::shared_ptr<PathsDataChunks> deserializePathsData(
        const SerializationObjectSharedData::ChunkStructures & chunk_structures,
        const PathsInfosChunks & paths_infos_chunks,
        const SerializationObjectSharedData::DeserializeBinaryBulkStateObjectSharedDataStructure & structure_state,
        DeserializeBinaryBulkSettings & settings,
        const DataTypePtr & dynamic_type,
        const SerializationPtr & dynamic_serialization,
        SubstreamsCache * cache);

    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Text/binary serialization is not implemented for object shared data");
    }

    friend SerializationObjectSharedDataPath;
    friend SerializationSubObjectSharedData;
    friend SerializationObjectDistinctPaths;

    SerializationVersion serialization_version;
    DataTypePtr dynamic_type;
    SerializationPtr dynamic_serialization;
    size_t buckets;
    SerializationPtr serialization_map;
};

}
#pragma clang diagnostic pop
