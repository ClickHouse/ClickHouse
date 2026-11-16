#pragma once

#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Common/Exception.h>

#include <functional>
#include <map>
#include <optional>
#include <span>
#include <vector>

namespace DB
{

class ColumnObject;

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

/// Serialization of the typed shared data of a JSON column: the typed paths declared SHARED, stored together in a
/// constant number of streams inside a MergeTree part instead of a set of streams per path. Paths are sorted by name
/// and everything on disk refers to a path by its position in this order. Streams:
/// - ObjectTypedSharedDataHeader (.header): <serialization version>, written by the prefix once per Wide part and per
///   granule in Compact parts. In Wide parts the suffix appends a record:
///   - v1, or v2 with <has regions> = 0 before it: per path, <compressed bytes> <uncompressed bytes> of the path's data
///     in the plain layout; paths share compressed blocks, so the compressed bytes are apportioned by the stream's ratio;
///   - v2 with <has regions> = 1: <marks per region>, then per path <number of substreams> and per substream <name>
///     <compressed bytes> <uncompressed bytes> <marks slice bytes> of its region and marks slice.
/// - ObjectTypedSharedDataPathsData (.paths_data): per granule, per path: <the path's data of the granule, prefix and
///   all substreams of the path type, serialized like a column of a Compact part with a fresh state>. A block is cut
///   before a substream once the open block holds min_compress_block_size bytes, and after the granule's last path.
/// - ObjectTypedSharedDataPathsMarks (.paths_marks): per granule: <number of rows>, then per path <start of the
///   path's data in .paths_data>, as varint deltas.
/// A Wide part of version 2 also has the regions, unless spilled data was lost while writing them:
/// - ObjectTypedSharedDataPathsRegionData (.paths_region_data): per path, per substream of the path serialized like a
///   column of a Wide part with one state for the part, its region: the substream's bytes cut into blocks as a Wide
///   part's file is. The header's record lists per path its substreams by name with the sizes of their regions. A mark
///   the path's serialization stores in its data is <granule index, uncompressed offset from the granule's start>.
/// - ObjectTypedSharedDataPathsRegionMarks (.paths_region_marks): per substream, its marks slice: a mark per granule and
///   the final mark, relative to the region's start, in blocks of its own.
/// Readers of some paths (SerializationObjectTypedSharedDataPath, SerializationSubObjectTypedSharedData) read a path
/// over its regions like a column of a Wide part whose files are views of the region streams. From the plain layout
/// they share the work of a call: the first of them decodes every requested path of the call's granules, seeking to
/// their marks, and caches the columns; the others take them from the substreams cache. The whole column decodes every
/// granule sequentially. Every reader's prefix registers its paths in the state shared by the readers of a column.
class SerializationObjectTypedSharedData final : public SimpleTextSerialization
{
public:
    struct SharedTypedPath
    {
        String name;
        DataTypePtr type;
        SerializationPtr serialization;
    };

    /// Sorted by name.
    using SharedTypedPaths = std::vector<SharedTypedPath>;

    static UInt128 getHash(const SharedTypedPaths & paths_);
    static SerializationPtr create(SharedTypedPaths paths_);

    size_t allocatedBytes() const override;
    bool supportsPooling() const override;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

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

    /// Where a substream of a path is in a v2 part with regions: its region in .paths_region_data and its marks slice in
    /// .paths_region_marks, each a block-aligned byte range read through a file view. Compressed offsets.
    struct SubstreamRegion
    {
        /// getSubstreamName of the substream.
        String name;
        Substream::SizeHint size = {};
        size_t region_begin = 0;
        size_t marks_begin = 0;
        size_t marks_end = 0;
    };

    /// The header, read once per part and shared by all readers of the column.
    /// Per path, written by the suffix of a Wide part (empty in Compact parts): its bytes always, and the regions of its
    /// substreams only in a v2 part with regions.
    struct PathRecord
    {
        Substream::SizeHint size = {};
        std::vector<SubstreamRegion> substreams;
    };

    struct Header
    {
        MergeTreeObjectTypedSharedDataSerializationVersion serialization_version = MergeTreeObjectTypedSharedDataSerializationVersion::V1;
        /// v2 Wide parts only: the regions are written; false in a part with the plain layout alone.
        bool has_regions = false;
        /// With regions: the marks in the slice of every substream, one per granule plus the final mark.
        size_t region_marks_count = 0;
        std::vector<PathRecord> path_records;
    };
    using HeaderPtr = std::shared_ptr<const Header>;

    /// The read state of one SHARED path: its type and serialization, and the remainder of a granule a previous call of
    /// the plain layout cut.
    struct PathState
    {
        String name;
        DataTypePtr type;
        SerializationPtr serialization;
        /// The granule the previous call cut, decoded whole; the rows after remainder_consumed are pending.
        ColumnPtr remainder = {};
        /// Rows of this path's remainder returned already. The plain layout's paths are cut at the same row, each
        /// advances its own.
        size_t remainder_consumed = 0;
        /// Set at the prefix by a reader of the plain layout (the whole column, or a sub-object with more paths than the
        /// threshold): every reader of the path then reads it from the plain layout, so that it is not read from both.
        bool read_from_plain_layout = false;
        /// With regions: the view of each region of the path, in the order of its record's substreams.
        std::vector<std::shared_ptr<const Substream::StreamView>> region_views = {};
    };

    /// The deserialize state of one JSON column's typed shared data, shared by the readers of its SHARED paths (the
    /// per-path readers, the sub-object reader, the whole-data reader), so the header is read once and the plain layout
    /// is decoded once. Created from the header and cached under the .header path.
    struct DeserializeBinaryBulkStateObjectTypedSharedData : public DeserializeBinaryBulkState
    {
        HeaderPtr header;
        size_t num_paths = 0;
        /// The read state of each path a reader asked for, by its position among the sorted SHARED paths (the decode
        /// order of a granule).
        std::map<size_t, PathState> requested_paths;

        /// Records the path as read; with from_plain_layout every reader of the path reads it from the plain layout.
        void requestPath(size_t path_index, const SharedTypedPath & path, bool from_plain_layout);
        /// A v2 Wide part writes the regions of the paths' substreams, so a narrow read goes through them instead of
        /// the plain layout.
        bool hasRegions() const { return header && header->has_regions; }
        /// The path is read from its regions: the part has regions and no reader of the path needs the plain layout. The
        /// prefixes of all readers come before any data, so this does not change while the data is read.
        bool readsFromRegions(size_t path_index) const
        {
            return hasRegions() && !requested_paths.at(path_index).read_from_plain_layout;
        }

        DeserializeBinaryBulkStatePtr cloneImpl(DeserializeBinaryBulkStateCloneContext &) const override
        {
            return std::make_shared<DeserializeBinaryBulkStateObjectTypedSharedData>(*this);
        }
    };

    /// The deserialize state of a reader of some SHARED paths (a path, a subcolumn of a path, a sub-object): the column's
    /// shared state and, per path of the reader read over its regions, the state of the serialization it is read with.
    struct DeserializeBinaryBulkStatePathsReader : public DeserializeBinaryBulkState
    {
        DeserializeBinaryBulkStatePtr shared_state;
        std::vector<DeserializeBinaryBulkStatePtr> path_states;

        DeserializeBinaryBulkStatePtr cloneImpl(DeserializeBinaryBulkStateCloneContext & context) const override
        {
            auto cloned = std::make_shared<DeserializeBinaryBulkStatePathsReader>();
            cloned->shared_state = shared_state ? shared_state->clone(context) : nullptr;
            cloned->path_states.reserve(path_states.size());
            for (const auto & path_state : path_states)
                cloned->path_states.push_back(path_state ? path_state->clone(context) : nullptr);
            return cloned;
        }
    };

    /// The typed shared data's deserialize state, created from the header on the first call and cached under the
    /// .header path so that every path reader's prefix gets the same object. Null when the part has no header stream.
    static DeserializeBinaryBulkStatePtr
    deserializeSharedState(DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache, size_t num_paths);

    /// The identity element of a path. Behind the ObjectTypedSharedData element it keys the path's column in the
    /// substreams cache; it is skipped in file names.
    static Substream getPathElement(const String & path);

    /// The streams behind settings.path: the three of v1 (the data stream with the size hint when given), and, with
    /// with_regions, the two region streams of a v2 Wide part.
    static void enumerateTypedSharedDataStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        std::optional<Substream::SizeHint> data_size_hint,
        bool with_regions = false);

    /// The bytes of the paths [first_path_index, first_path_index + num_paths) from the header's size record; nothing
    /// without a shared state or a record.
    static std::optional<Substream::SizeHint>
    pathsSizeHint(const DeserializeBinaryBulkStatePtr & shared_state, size_t first_path_index, size_t num_paths);

    /// The shortcut of every reader: copies the call's rows of the path from the substreams cache. False when the
    /// first reader of the call has not decoded them yet. settings.path ends with the ObjectTypedSharedData element.
    static bool insertPathFromCache(IColumn & column, const String & path, DeserializeBinaryBulkSettings & settings, SubstreamsCache * cache);

    /// The work of the first reader of the plain layout in a call: decodes the call's rows of every requested path not
    /// read from its regions and caches each path's column under its element. A path the caller owns is decoded straight
    /// into the column own_destination returns for it, the others into columns of the call. settings.path ends with the
    /// ObjectTypedSharedData element.
    static void deserializePlainLayoutPaths(
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
        SubstreamsCache * cache,
        const std::function<IColumn *(size_t path_index)> & own_destination);

    /// The name of a substream of a path in the region record: the stream name of a substream path of the path's
    /// serialization, which starts after the path's identity element, rendered without a column name.
    static String getSubstreamName(std::span<const Substream> path_substream_path);

    /// Over the regions of a v2 part, a path is read like a column of a Wide part by `path_serialization` (of the path,
    /// or of a subcolumn of the path type) with the state `path_state`, every substream through the view of its region:
    /// the prefix, the streams with the state, the data. settings.path ends with the path's identity element.
    static void deserializePathRegionsPrefix(
        DeserializeBinaryBulkSettings & settings,
        const DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
        size_t path_index,
        const ISerialization & path_serialization,
        DeserializeBinaryBulkStatePtr & path_state,
        SubstreamsDeserializeStatesCache * cache);

    static void enumeratePathRegionsStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
        size_t path_index,
        const SubstreamData & path_data);

    static void deserializePathRegions(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        const DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
        size_t path_index,
        const ISerialization & path_serialization,
        DeserializeBinaryBulkStatePtr & path_state,
        SubstreamsCache * cache);

private:
    explicit SerializationObjectTypedSharedData(SharedTypedPaths paths_);

    [[noreturn]] static void throwNoSerialization()
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Serialization of the typed shared data of Object type is used only as a part of Object serialization");
    }

    SharedTypedPaths paths;
};

}
