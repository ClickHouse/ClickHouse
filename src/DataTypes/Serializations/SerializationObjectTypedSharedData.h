#pragma once

#include <Core/MergeTreeSerializationEnums.h>
#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <Common/Exception.h>

#include <functional>
#include <map>
#include <optional>

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
///   granule in Compact parts. In Wide parts the suffix appends, per path, <compressed bytes> <uncompressed bytes> of
///   the path's data in the part; paths share compressed blocks, so the compressed bytes are apportioned by the
///   stream's ratio.
/// - ObjectTypedSharedDataPathsData (.paths_data): per granule, per path: <the path's data of the granule, prefix and
///   all substreams of the path type, serialized like a column of a Compact part with a fresh state>. A block is cut
///   before a substream once the open block holds min_compress_block_size bytes, and after the granule's last path.
/// - ObjectTypedSharedDataPathsMarks (.paths_marks): per granule: <number of rows>, then per path <start of the
///   path's data in .paths_data>, as varint deltas.
/// Readers of some paths (SerializationObjectTypedSharedDataPath, SerializationSubObjectTypedSharedData) share the work
/// of a call: the first of them decodes every requested path of the call's granules, seeking to their marks, and
/// caches the columns; the others take them from the substreams cache. The whole column decodes every granule
/// sequentially. Every reader's prefix registers its paths in the structure state shared by the readers of a column.
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

    /// The header, read once per part and shared by all readers of the column.
    struct Header
    {
        MergeTreeObjectTypedSharedDataSerializationVersion serialization_version = MergeTreeObjectTypedSharedDataSerializationVersion::V1;
        /// Per path, the bytes of its data in the part; written by the suffix of a Wide part, empty in Compact parts.
        std::vector<Substream::SizeHint> path_sizes;
    };
    using HeaderPtr = std::shared_ptr<const Header>;

    /// A path some reader of the call reads whole, with what the first reader needs to decode it.
    struct RequestedPath
    {
        String name;
        DataTypePtr type;
        SerializationPtr serialization;
        /// The granule the previous call cut, decoded whole; the rows after remainder_consumed of the state are pending.
        ColumnPtr remainder;
    };

    /// Shared by all readers of one column: the header, the paths they read and the remainders of a cut granule.
    struct DeserializeBinaryBulkStateObjectTypedSharedDataStructure : public DeserializeBinaryBulkState
    {
        HeaderPtr header;
        size_t num_paths = 0;
        /// By the path's position among the sorted SHARED paths, which is the decode order of a granule.
        std::map<size_t, RequestedPath> requested_paths;
        /// Rows of the remainders the previous calls returned already; the remainders of all paths advance together.
        size_t remainder_consumed = 0;

        void requestPath(size_t path_index, const SharedTypedPath & path);
        bool allPathsRequested() const { return requested_paths.size() == num_paths; }

        DeserializeBinaryBulkStatePtr cloneImpl(DeserializeBinaryBulkStateCloneContext &) const override
        {
            return std::make_shared<DeserializeBinaryBulkStateObjectTypedSharedDataStructure>(*this);
        }
    };

    /// The structure state of the column, created with the header on the first call and cached under the .header
    /// path, so that every reader's prefix gets the same object. Null when the part has no header stream.
    static DeserializeBinaryBulkStatePtr
    deserializeStructureStatePrefix(DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache, size_t num_paths);

    /// The identity element of a path. Behind the ObjectTypedSharedData element it keys the path's column in the
    /// substreams cache; it is skipped in file names.
    static Substream getPathElement(const String & path);

    /// The three streams behind settings.path, the data stream with the size hint when given.
    static void enumerateTypedSharedDataStreams(
        EnumerateStreamsSettings & settings, const StreamCallback & callback, std::optional<Substream::SizeHint> data_size_hint);

    /// The bytes of the paths [first_path_index, first_path_index + num_paths) from the header's size record; nothing
    /// without a structure state or a record.
    static std::optional<Substream::SizeHint>
    pathsSizeHint(const DeserializeBinaryBulkStatePtr & structure_state, size_t first_path_index, size_t num_paths);

    /// The shortcut of every reader: copies the call's rows of the path from the substreams cache. False when the
    /// first reader of the call has not decoded them yet. settings.path ends with the ObjectTypedSharedData element.
    static bool insertPathFromCache(IColumn & column, const String & path, DeserializeBinaryBulkSettings & settings, SubstreamsCache * cache);

    /// The work of the first reader of a call: decodes the call's rows of every requested path of the structure state
    /// and caches each path's column under its element. A path the caller owns is decoded straight into the column
    /// own_destination returns for it, the others into columns of the call. settings.path ends with the
    /// ObjectTypedSharedData element.
    static void deserializeRequestedPaths(
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStateObjectTypedSharedDataStructure & structure_state,
        SubstreamsCache * cache,
        const std::function<IColumn *(size_t path_index)> & own_destination);

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
