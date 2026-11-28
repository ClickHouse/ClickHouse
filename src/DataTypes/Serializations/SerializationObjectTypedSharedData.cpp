#include <Columns/ColumnObject.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/Serializations/SerializationObjectTypedSharedData.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <base/scope_guard.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

struct SerializeBinaryBulkStateObjectTypedSharedData : public ISerialization::SerializeBinaryBulkState
{
    SerializationObjectTypedSharedData::Header header;

    /// Rows of the granule being written, one slice per serialize call. A slice references the block's columns, which
    /// are immutable while referenced; a granule inside one block is one slice, a granule that spans several blocks has
    /// one slice per block.
    struct Slice
    {
        Columns path_columns;
        size_t offset = 0;
        size_t rows = 0;
    };
    std::vector<Slice> current_granule_slices;

    /// Serialized bytes of every path so far, for the size record of a Wide part.
    std::vector<size_t> paths_uncompressed_bytes;
};

/// One granule's entry of .paths_marks: its rows and the marks of its paths in .paths_data.
struct GranuleEntry
{
    size_t num_rows = 0;
    std::vector<MarkInCompressedFile> marks;
};

String getStreamName(ISerialization::Substream::Type type)
{
    ISerialization::SubstreamPath path;
    path.push_back(type);
    return ISerialization::getFileNameForStream("", path, {});
}

WriteBuffer * getStream(ISerialization::Substream::Type type, ISerialization::SerializeBinaryBulkSettings & settings)
{
    settings.path.push_back(type);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream {} of the typed shared data of Object", getStreamName(type));
    return stream;
}

MarkInCompressedFile getStreamMark(ISerialization::Substream::Type type, ISerialization::SerializeBinaryBulkSettings & settings)
{
    if (!settings.stream_mark_getter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark getter is not set for the typed shared data of Object");
    settings.path.push_back(type);
    auto mark = settings.stream_mark_getter(settings.path);
    settings.path.pop_back();
    return mark;
}

using PathColumnGetter = std::function<const IColumn &(size_t path_index)>;

/// Writes the granule's rows [offset, offset + num_rows) of every path into .paths_data, each path as one unit with a
/// fresh state, and returns where each path's data starts.
GranuleEntry writeGranulePathsData(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    const PathColumnGetter & get_column,
    size_t offset,
    size_t num_rows,
    ISerialization::SerializeBinaryBulkSettings & settings,
    std::vector<size_t> & paths_uncompressed_bytes)
{
    bool compact = settings.data_part_type == MergeTreeDataPartType::Compact;

    settings.path.push_back(ISerialization::Substream::ObjectTypedSharedDataPathsData);
    SCOPE_EXIT(settings.path.pop_back());
    auto * data_stream = settings.getter(settings.path);
    if (!data_stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream {} of the typed shared data of Object", getStreamName(settings.path.back().type));
    if (!settings.stream_mark_getter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark getter is not set for the typed shared data of Object");
    auto get_data_mark = [&] { return settings.stream_mark_getter(settings.path); };

    GranuleEntry entry;
    entry.num_rows = num_rows;
    entry.marks.resize(paths.size());

    /// A granule without rows has no path data (a limit of 0 would mean the whole column to the serializations); the
    /// caller still writes its marks entry, which the reader expects for every granule.
    if (num_rows == 0)
    {
        std::ranges::fill(entry.marks, get_data_mark());
        return entry;
    }

    auto path_settings = getObjectPathDataSerializationSettings(settings, settings.object_shared_data_serialization_version);
    path_settings.stream_mark_getter = [&](const ISerialization::SubstreamPath &) { return get_data_mark(); };
    /// A new block once the open one is worth it: bounds a selective read's over-read to min_compress_block_size while
    /// small paths still share a block.
    auto cut_block_if_worth_it = [&]
    {
        if (data_stream->offset() >= settings.min_compress_block_size)
            data_stream->next();
    };
    path_settings.getter = [&](const ISerialization::SubstreamPath &) -> WriteBuffer *
    {
        cut_block_if_worth_it();
        return data_stream;
    };

    for (size_t path_index = 0; path_index != paths.size(); ++path_index)
    {
        const auto & path = paths[path_index];
        cut_block_if_worth_it();
        entry.marks[path_index] = get_data_mark();
        size_t count_before_path = data_stream->count();

        const auto & column = get_column(path_index);
        ISerialization::SerializeBinaryBulkStatePtr path_state;
        path.serialization->serializeBinaryBulkStatePrefix(column, path_settings, path_state);
        path.serialization->serializeBinaryBulkWithMultipleStreams(column, offset, num_rows, path_settings, path_state);
        auto mark_after_data = get_data_mark();
        path.serialization->serializeBinaryBulkStateSuffix(path_settings, path_state);
        /// The reader decodes a path's data sequentially up to the next path's, so the suffix must write nothing.
        if (get_data_mark() != mark_after_data)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Suffix of path {} wrote data into the typed shared data of Object", path.name);

        paths_uncompressed_bytes[path_index] += data_stream->count() - count_before_path;
    }

    /// The cut after the granule's last path: the next granule's mark is a block start, so a granule's blocks are its own.
    if (!compact)
        data_stream->next();

    return entry;
}

void writeGranuleEntry(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    const GranuleEntry & entry,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    auto * marks_stream = getStream(ISerialization::Substream::ObjectTypedSharedDataPathsMarks, settings);
    writeVarUInt(entry.num_rows, *marks_stream);
    size_t previous_compressed_offset = 0;
    for (size_t path_index = 0; path_index != paths.size(); ++path_index)
    {
        writeMarkDelta(entry.marks[path_index], previous_compressed_offset, *marks_stream);
        previous_compressed_offset = entry.marks[path_index].offset_in_compressed_file;
    }
}

/// Writes the complete granule the slices hold: its paths' data and its marks entry. A granule inside one block is
/// serialized from the block's columns; a granule that spans blocks is combined into one column per path first.
void completeGranule(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    SerializeBinaryBulkStateObjectTypedSharedData & state,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    auto & slices = state.current_granule_slices;
    size_t num_rows = 0;
    for (const auto & slice : slices)
        num_rows += slice.rows;

    GranuleEntry entry;
    if (slices.size() <= 1)
    {
        const auto * slice = slices.empty() ? nullptr : &slices.front();
        PathColumnGetter get_column = [&](size_t path_index) -> const IColumn & { return *slice->path_columns[path_index]; };
        entry = writeGranulePathsData(paths, get_column, slice ? slice->offset : 0, num_rows, settings, state.paths_uncompressed_bytes);
    }
    else
    {
        /// The writer takes each path once, in order, so one combined column at a time is enough.
        MutableColumnPtr combined;
        PathColumnGetter get_column = [&](size_t path_index) -> const IColumn &
        {
            combined = slices.front().path_columns[path_index]->cloneEmpty();
            combined->reserve(num_rows);
            for (const auto & slice : slices)
                combined->insertRangeFrom(*slice.path_columns[path_index], slice.offset, slice.rows);
            return *combined;
        };
        entry = writeGranulePathsData(paths, get_column, 0, num_rows, settings, state.paths_uncompressed_bytes);
    }

    writeGranuleEntry(paths, entry, settings);
    slices.clear();
}

SerializationObjectTypedSharedData::HeaderPtr readHeader(ReadBuffer & stream, size_t num_paths, bool with_path_sizes)
{
    auto header = std::make_shared<SerializationObjectTypedSharedData::Header>();
    UInt64 version = 0;
    readVarUInt(version, stream);
    if (version != static_cast<UInt64>(MergeTreeObjectTypedSharedDataSerializationVersion::V1))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version for the typed shared data of Object: {}", version);
    header->serialization_version = static_cast<MergeTreeObjectTypedSharedDataSerializationVersion>(version);

    if (with_path_sizes)
    {
        header->path_sizes.resize(num_paths);
        for (auto & path_size : header->path_sizes)
        {
            readVarUInt(path_size.compressed, stream);
            readVarUInt(path_size.uncompressed, stream);
        }
    }
    return header;
}

void writeHeader(const SerializationObjectTypedSharedData::Header & header, ISerialization::SerializeBinaryBulkSettings & settings)
{
    auto * stream = getStream(ISerialization::Substream::ObjectTypedSharedDataHeader, settings);
    if (header.serialization_version != MergeTreeObjectTypedSharedDataSerializationVersion::V1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unsupported version {} for the typed shared data of Object",
            static_cast<UInt64>(header.serialization_version));
    writeVarUInt(static_cast<UInt64>(header.serialization_version), *stream);
}

/// The size record of a Wide part, appended to the header stream by the suffix. Paths share compressed blocks, so a
/// path's compressed bytes are its share of the stream by serialized bytes.
void writePathSizes(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    const SerializeBinaryBulkStateObjectTypedSharedData & state,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    size_t total_compressed = getStreamMark(ISerialization::Substream::ObjectTypedSharedDataPathsData, settings).offset_in_compressed_file;
    size_t total_uncompressed = 0;
    for (auto bytes : state.paths_uncompressed_bytes)
        total_uncompressed += bytes;

    auto * stream = getStream(ISerialization::Substream::ObjectTypedSharedDataHeader, settings);
    for (size_t path_index = 0; path_index != paths.size(); ++path_index)
    {
        size_t uncompressed = state.paths_uncompressed_bytes[path_index];
        size_t compressed = total_uncompressed == 0
            ? 0
            : static_cast<size_t>(static_cast<double>(uncompressed) * static_cast<double>(total_compressed) / static_cast<double>(total_uncompressed));
        writeVarUInt(compressed, *stream);
        writeVarUInt(uncompressed, *stream);
    }
}

}

SerializationObjectTypedSharedData::SerializationObjectTypedSharedData(SharedTypedPaths paths_)
    : paths(std::move(paths_))
{
}

UInt128 SerializationObjectTypedSharedData::getHash(const SharedTypedPaths & paths_)
{
    SipHash hash;
    hash.update("ObjectTypedSharedData");
    hash.update(paths_.size());
    for (const auto & path : paths_)
    {
        hash.update(path.name.size());
        hash.update(path.name);
        auto type_name = path.type->getName();
        hash.update(type_name.size());
        hash.update(type_name);
        hash.update(path.serialization->getHash());
    }
    return hash.get128();
}

SerializationPtr SerializationObjectTypedSharedData::create(SharedTypedPaths paths_)
{
    bool pooling = std::ranges::all_of(paths_, [](const auto & path) { return path.serialization->supportsPooling(); });
    if (!pooling)
        return std::shared_ptr<ISerialization>(new SerializationObjectTypedSharedData(std::move(paths_)));
    return ISerialization::pooled(getHash(paths_), [&] { return new SerializationObjectTypedSharedData(std::move(paths_)); });
}

size_t SerializationObjectTypedSharedData::allocatedBytes() const
{
    size_t bytes = sizeof(*this) + paths.capacity() * sizeof(SharedTypedPath);
    for (const auto & path : paths)
        bytes += path.name.capacity();
    return bytes;
}

bool SerializationObjectTypedSharedData::supportsPooling() const
{
    return std::ranges::all_of(paths, [](const auto & path) { return path.serialization->supportsPooling(); });
}

void SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedDataStructure::requestPath(
    size_t path_index, const SharedTypedPath & path)
{
    requested_paths.try_emplace(path_index, RequestedPath{path.name, path.type, path.serialization, nullptr});
}

ISerialization::Substream SerializationObjectTypedSharedData::getPathElement(const String & path)
{
    Substream element(Substream::ObjectTypedSharedDataPath);
    element.object_path_name = path;
    return element;
}

std::optional<ISerialization::Substream::SizeHint> SerializationObjectTypedSharedData::pathsSizeHint(
    const DeserializeBinaryBulkStatePtr & structure_state, size_t first_path_index, size_t num_paths)
{
    if (!structure_state)
        return std::nullopt;
    const auto * structure = typeid_cast<const DeserializeBinaryBulkStateObjectTypedSharedDataStructure *>(structure_state.get());
    if (!structure)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid structure state of the typed shared data of Object");
    const auto & path_sizes = structure->header->path_sizes;
    if (path_sizes.empty())
        return std::nullopt;

    Substream::SizeHint hint;
    for (size_t path_index = first_path_index; path_index != first_path_index + num_paths; ++path_index)
    {
        hint.compressed += path_sizes.at(path_index).compressed;
        hint.uncompressed += path_sizes.at(path_index).uncompressed;
    }
    return hint;
}

void SerializationObjectTypedSharedData::enumerateTypedSharedDataStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, std::optional<Substream::SizeHint> data_size_hint)
{
    auto add = [&](Substream substream)
    {
        settings.path.push_back(std::move(substream));
        callback(settings.path);
        settings.path.pop_back();
    };

    add(Substream::ObjectTypedSharedDataHeader);
    Substream data(Substream::ObjectTypedSharedDataPathsData);
    data.size_hint = data_size_hint;
    add(std::move(data));
    add(Substream::ObjectTypedSharedDataPathsMarks);
}

void SerializationObjectTypedSharedData::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData &) const
{
    /// The caller has pushed ObjectData and ObjectTypedSharedData.
    enumerateTypedSharedDataStreams(settings, callback, std::nullopt);
}

void SerializationObjectTypedSharedData::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto typed_shared_data_state = std::make_shared<SerializeBinaryBulkStateObjectTypedSharedData>();
    typed_shared_data_state->header.serialization_version = settings.object_typed_shared_data_serialization_version;
    typed_shared_data_state->paths_uncompressed_bytes.assign(paths.size(), 0);
    writeHeader(typed_shared_data_state->header, settings);
    state = std::move(typed_shared_data_state);
}

void SerializationObjectTypedSharedData::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto * typed_shared_data_state = checkAndGetState<SerializeBinaryBulkStateObjectTypedSharedData>(state);
    const auto & typed_paths = assert_cast<const ColumnObject &>(column).getTypedPaths();
    size_t end = limit == 0 ? column.size() : std::min(column.size(), offset + limit);

    SerializeBinaryBulkStateObjectTypedSharedData::Slice slice;
    slice.path_columns.reserve(paths.size());
    for (const auto & path : paths)
        slice.path_columns.push_back(typed_paths.at(path.name)->getPtr());
    slice.offset = offset;
    slice.rows = end > offset ? end - offset : 0;

    auto & slices = typed_shared_data_state->current_granule_slices;
    /// In Compact parts every call is a complete granule and no state survives it.
    if (settings.data_part_type == MergeTreeDataPartType::Compact)
    {
        if (!slices.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A granule of a Compact part spans serialize calls of the typed shared data of Object");
        slices.push_back(std::move(slice));
        completeGranule(paths, *typed_shared_data_state, settings);
        return;
    }

    if (slice.rows != 0)
        slices.push_back(std::move(slice));
    if (settings.granule_is_complete)
        completeGranule(paths, *typed_shared_data_state, settings);
}

void SerializationObjectTypedSharedData::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto * typed_shared_data_state = checkAndGetState<SerializeBinaryBulkStateObjectTypedSharedData>(state);
    /// Slices left over belong to the part's last granule, which the writer completes without a data call.
    if (!typed_shared_data_state->current_granule_slices.empty())
        completeGranule(paths, *typed_shared_data_state, settings);

    if (settings.data_part_type != MergeTreeDataPartType::Compact)
        writePathSizes(paths, *typed_shared_data_state, settings);
}

ISerialization::DeserializeBinaryBulkStatePtr SerializationObjectTypedSharedData::deserializeStructureStatePrefix(
    DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache, size_t num_paths)
{
    settings.path.push_back(Substream::ObjectTypedSharedDataHeader);
    SCOPE_EXIT(settings.path.pop_back());
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
        return cached_state;

    auto * stream = settings.getter(settings.path);
    if (!stream)
        return nullptr;

    auto structure_state = std::make_shared<DeserializeBinaryBulkStateObjectTypedSharedDataStructure>();
    structure_state->header = readHeader(*stream, num_paths, settings.data_part_type != MergeTreeDataPartType::Compact);
    structure_state->num_paths = num_paths;
    addToSubstreamsDeserializeStatesCache(cache, settings.path, structure_state);
    return structure_state;
}

void SerializationObjectTypedSharedData::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto structure_state = deserializeStructureStatePrefix(settings, cache, paths.size());
    if (!structure_state)
        return;

    /// The whole column reads every path; the structure state is the reader's state.
    auto * structure = checkAndGetState<DeserializeBinaryBulkStateObjectTypedSharedDataStructure>(structure_state);
    for (size_t path_index = 0; path_index != paths.size(); ++path_index)
        structure->requestPath(path_index, paths[path_index]);
    state = std::move(structure_state);
}

namespace
{

ReadBuffer * getReadStream(ISerialization::Substream::Type type, ISerialization::DeserializeBinaryBulkSettings & settings)
{
    settings.path.push_back(type);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream {} of the typed shared data of Object", getStreamName(type));
    return stream;
}

GranuleEntry readGranuleEntry(ReadBuffer & stream, size_t num_paths)
{
    GranuleEntry entry;
    readVarUInt(entry.num_rows, stream);
    entry.marks.resize(num_paths);
    size_t previous_compressed_offset = 0;
    for (size_t path_index = 0; path_index != num_paths; ++path_index)
    {
        entry.marks[path_index] = readMarkDelta(previous_compressed_offset, stream);
        previous_compressed_offset = entry.marks[path_index].offset_in_compressed_file;
    }
    return entry;
}

/// A path's data has the layout of a Compact part and is read sequentially from one stream.
ISerialization::DeserializeBinaryBulkSettings getPathDataDeserializeSettings(ReadBuffer * data_stream)
{
    ISerialization::DeserializeBinaryBulkSettings path_settings;
    path_settings.read_statistics = false;
    path_settings.position_independent_encoding = true;
    path_settings.use_specialized_prefixes_and_suffixes_substreams = true;
    path_settings.data_part_type = MergeTreeDataPartType::Compact;
    path_settings.getter = [data_stream](const ISerialization::SubstreamPath &) -> ReadBuffer * { return data_stream; };
    return path_settings;
}

/// One path's data of a granule, whole, with a fresh state; the suffix wrote nothing, so the stream then stands at the
/// next path's data. Bulk deserialization stops silently at the end of a stream, so the rows are counted here.
void decodeGranule(
    const SerializationObjectTypedSharedData::RequestedPath & path,
    size_t num_rows,
    IColumn & column,
    ISerialization::DeserializeBinaryBulkSettings & path_settings)
{
    size_t rows_before = column.size();
    ISerialization::DeserializeBinaryBulkStatePtr path_state;
    path.serialization->deserializeBinaryBulkStatePrefix(path_settings, path_state, nullptr);
    path.serialization->deserializeBinaryBulkWithMultipleStreams(column, num_rows, path_settings, path_state, nullptr);
    if (column.size() != rows_before + num_rows)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Read {} rows instead of {} of a granule of path {} from the typed shared data of Object",
            column.size() - rows_before,
            num_rows,
            path.name);
}

/// The first rows of an empty column bring the decoded column's dynamic structure along, so that later granules can be
/// decoded straight into it.
void insertRangeFromDecoded(IColumn & column, const IColumn & decoded, size_t offset, size_t limit)
{
    if (column.empty())
        column.takeExactDynamicStructureFrom(decoded);
    column.insertRangeFrom(decoded, offset, limit);
}

/// Where a requested path's rows of the call go, and how many it had before the call.
struct PathDestination
{
    size_t path_index = 0;
    SerializationObjectTypedSharedData::RequestedPath * path = nullptr;
    IColumn * column = nullptr;
    size_t rows_before = 0;
};

/// Decodes the requested paths of one granule, in path order. A granule the call consumes whole goes straight into the
/// destinations; a granule it cuts is decoded whole, its first rows copied and the rest kept as the remainders.
void decodeGranuleIntoPathDestinations(
    const GranuleEntry & entry,
    bool all_paths,
    const std::function<void(const MarkInCompressedFile &)> & seek_to,
    ISerialization::DeserializeBinaryBulkSettings & path_settings,
    std::vector<PathDestination> & destinations,
    size_t & rows_left,
    SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedDataStructure & state)
{
    /// A granule without rows has no path data.
    if (entry.num_rows == 0)
        return;

    size_t rows = std::min(entry.num_rows, rows_left);
    bool cut = rows < entry.num_rows;
    std::optional<size_t> previous_index;
    for (auto & destination : destinations)
    {
        /// Every path is read at the position the previous one left the stream, unless some path lies between them.
        bool adjacent = previous_index && *previous_index + 1 == destination.path_index;
        if (!all_paths && !adjacent)
            seek_to(entry.marks[destination.path_index]);

        if (!cut)
        {
            decodeGranule(*destination.path, entry.num_rows, *destination.column, path_settings);
        }
        else
        {
            auto whole_granule = destination.column->cloneEmpty();
            decodeGranule(*destination.path, entry.num_rows, *whole_granule, path_settings);
            insertRangeFromDecoded(*destination.column, *whole_granule, 0, rows);
            destination.path->remainder = std::move(whole_granule);
        }
        previous_index = destination.path_index;
    }

    if (cut)
        state.remainder_consumed = rows;
    rows_left -= rows;
}

}

bool SerializationObjectTypedSharedData::insertPathFromCache(
    IColumn & column, const String & path, DeserializeBinaryBulkSettings & settings, SubstreamsCache * cache)
{
    settings.path.push_back(getPathElement(path));
    auto cached = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path);
    settings.path.pop_back();
    if (!cached)
        return false;
    const auto & [cached_column, num_read_rows] = *cached;
    chassert(&column != cached_column.get());
    insertRangeFromDecoded(column, *cached_column, cached_column->size() - num_read_rows, num_read_rows);
    return true;
}

void SerializationObjectTypedSharedData::deserializeRequestedPaths(
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStateObjectTypedSharedDataStructure & state,
    SubstreamsCache * cache,
    const std::function<IColumn *(size_t path_index)> & own_destination)
{
    if (state.requested_paths.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No path of the typed shared data of Object is requested");

    bool compact = settings.data_part_type == MergeTreeDataPartType::Compact;
    bool all_paths = state.allPathsRequested();

    std::vector<PathDestination> destinations;
    std::vector<MutableColumnPtr> call_columns;
    destinations.reserve(state.requested_paths.size());
    for (auto & [path_index, path] : state.requested_paths)
    {
        IColumn * column = own_destination(path_index);
        if (!column)
        {
            call_columns.push_back(path.type->createColumn());
            column = call_columns.back().get();
        }
        destinations.push_back({path_index, &path, column, column->size()});
    }

    /// A call that does not continue the previous one starts at a granule the reader positioned the streams at, so the
    /// remainders are stale. One decision serves all readers of the column: a remainder outlives only the call that cut
    /// the granule, which the same reader continues, and a reader entering later starts at a granule boundary.
    if (!settings.continuous_reading)
    {
        for (auto & destination : destinations)
            destination.path->remainder.reset();
        state.remainder_consumed = 0;
    }

    size_t rows_left = limit;

    /// The granule the previous call cut goes first; the remainders of all paths advance together.
    if (destinations.front().path->remainder)
    {
        size_t remainder_rows = destinations.front().path->remainder->size() - state.remainder_consumed;
        size_t rows = std::min(rows_left, remainder_rows);
        for (auto & destination : destinations)
        {
            if (!destination.path->remainder)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object has no remainder", destination.path->name);
            insertRangeFromDecoded(*destination.column, *destination.path->remainder, state.remainder_consumed, rows);
        }
        state.remainder_consumed += rows;
        rows_left -= rows;
        if (rows == remainder_rows)
        {
            for (auto & destination : destinations)
                destination.path->remainder.reset();
            state.remainder_consumed = 0;
        }
    }

    auto marks_path = settings.path;
    marks_path.push_back(Substream::ObjectTypedSharedDataPathsMarks);
    auto data_path = settings.path;
    data_path.push_back(Substream::ObjectTypedSharedDataPathsData);
    auto seek_to = [&](const MarkInCompressedFile & mark)
    {
        if (!settings.seek_stream_to_mark_callback)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Seek to mark callback is not set for the typed shared data of Object");
        settings.seek_stream_to_mark_callback(data_path, mark);
    };

    if (!compact)
    {
        /// Both streams stand at the call's granule, positioned by the reader or left there by the previous call; a granule
        /// ends with a block, so the whole range reads on without seeks. Requested before the row check, so that a call
        /// without rows still positions them.
        auto * marks_stream = getReadStream(Substream::ObjectTypedSharedDataPathsMarks, settings);
        auto * data_stream = getReadStream(Substream::ObjectTypedSharedDataPathsData, settings);
        auto path_settings = getPathDataDeserializeSettings(data_stream);
        while (rows_left != 0 && !marks_stream->eof())
        {
            auto entry = readGranuleEntry(*marks_stream, state.num_paths);
            decodeGranuleIntoPathDestinations(entry, all_paths, seek_to, path_settings, destinations, rows_left, state);
        }
    }
    else if (!settings.seek_stream_to_current_mark_callback)
    {
        /// The container cannot position the substreams (a Compact part without substream marks, a value read whole from
        /// the dynamic shared data): one buffer, read in write order, the paths' data and then the entry, which only
        /// confirms the row count. Only the whole column is read this way. An absolute seek callback alone does not
        /// mean the substreams can be positioned.
        if (!all_paths)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Some paths of the typed shared data of Object are read from a stream without substream marks");

        auto * data_stream = getReadStream(Substream::ObjectTypedSharedDataPathsData, settings);
        auto path_settings = getPathDataDeserializeSettings(data_stream);
        GranuleEntry entry;
        entry.num_rows = limit;
        decodeGranuleIntoPathDestinations(entry, all_paths, seek_to, path_settings, destinations, rows_left, state);

        auto * marks_stream = getReadStream(Substream::ObjectTypedSharedDataPathsMarks, settings);
        auto written_entry = readGranuleEntry(*marks_stream, state.num_paths);
        if (written_entry.num_rows != limit)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "A granule of the typed shared data of Object in a Compact part has {} rows, {} were read",
                written_entry.num_rows,
                limit);
    }
    else
    {
        /// With substream marks the single buffer is positioned at a substream's mark when the substream is requested (by
        /// the getter for a subcolumn, by the callback for the whole column), so each is requested right before it is
        /// read. The entry is read again after the data, which leaves the buffer at the region's end for the next substream.
        auto seek_to_current_mark = [&](const SubstreamPath & path)
        {
            if (settings.seek_stream_to_current_mark_callback)
                settings.seek_stream_to_current_mark_callback(path);
        };

        seek_to_current_mark(marks_path);
        auto * marks_stream = getReadStream(Substream::ObjectTypedSharedDataPathsMarks, settings);
        auto entry = readGranuleEntry(*marks_stream, state.num_paths);

        seek_to_current_mark(data_path);
        auto * data_stream = getReadStream(Substream::ObjectTypedSharedDataPathsData, settings);
        auto path_settings = getPathDataDeserializeSettings(data_stream);
        if (entry.num_rows != 0)
            seek_to(entry.marks[destinations.front().path_index]);
        decodeGranuleIntoPathDestinations(entry, all_paths, seek_to, path_settings, destinations, rows_left, state);

        if (all_paths)
        {
            seek_to_current_mark(marks_path);
            getReadStream(Substream::ObjectTypedSharedDataPathsMarks, settings);
            readGranuleEntry(*marks_stream, state.num_paths);
        }
    }

    for (const auto & destination : destinations)
    {
        settings.path.push_back(getPathElement(destination.path->name));
        addColumnWithNumReadRowsToSubstreamsCache(
            cache, settings.path, destination.column->getPtr(), destination.column->size() - destination.rows_before);
        settings.path.pop_back();
    }
}

void SerializationObjectTypedSharedData::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column, size_t limit, DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsCache * cache) const
{
    /// The column is absent from the part: the prefix found no header, and the reader fills the defaults.
    if (!state)
        return;

    auto * structure = checkAndGetState<DeserializeBinaryBulkStateObjectTypedSharedDataStructure>(state);
    auto & typed_paths = assert_cast<ColumnObject &>(column).getTypedPaths();

    /// The caller has pushed ObjectData and ObjectTypedSharedData. The first reader of a call decodes every requested
    /// path, so either every path is in the cache or none is.
    if (insertPathFromCache(*typed_paths.at(paths.front().name), paths.front().name, settings, cache))
    {
        for (size_t path_index = 1; path_index != paths.size(); ++path_index)
        {
            if (!insertPathFromCache(*typed_paths.at(paths[path_index].name), paths[path_index].name, settings, cache))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object is not in the substreams cache", paths[path_index].name);
        }
        return;
    }

    deserializeRequestedPaths(
        limit, settings, *structure, cache, [&](size_t path_index) -> IColumn * { return typed_paths.at(paths[path_index].name).get(); });
}

}
