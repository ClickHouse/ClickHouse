#include <Columns/ColumnObject.h>
#include <DataTypes/Serializations/SerializationSpiller.h>
#include <DataTypes/Serializations/SerializationObjectHelpers.h>
#include <DataTypes/Serializations/SerializationObjectTypedSharedData.h>
#include <DataTypes/Serializations/SubstreamView.h>
#include <Formats/MarkInCompressedFile.h>
#include <IO/PiecewiseWriteBuffer.h>
#include <IO/copyData.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/scope_guard.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <boost/noncopyable.hpp>

#include <algorithm>
#include <set>
#include <span>

namespace ProfileEvents
{
    extern const Event ObjectTypedSharedDataRegionReads;
    extern const Event ObjectTypedSharedDataPlainLayoutReads;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// One granule's entry of .paths_marks: its rows and the marks of its paths in .paths_data.
struct GranuleEntry
{
    size_t num_rows = 0;
    std::vector<MarkInCompressedFile> marks;
};

/// The offsets in a substream's bytes at which its granules start, non-decreasing. A granule starting at the offset of
/// the granule before it, which is then empty, is counted at that offset instead of taking an entry, so that a substream
/// without bytes takes constant memory whatever the number of granules.
class GranuleOffsets
{
public:
    /// Adds a granule starting at `offset`.
    void add(size_t offset)
    {
        chassert(!(offset & MORE_GRANULES_FLAG));
        if (!entries.empty() && lastOffset() == offset)
        {
            if (entries.back() & MORE_GRANULES_FLAG)
                ++entries.back();
            else
                entries.push_back(MORE_GRANULES_FLAG | 1);
            return;
        }

        chassert(entries.empty() || lastOffset() < offset);
        entries.push_back(offset);
    }

    [[nodiscard]] bool empty() const { return entries.empty(); }
    /// Only when not empty.
    [[nodiscard]] size_t firstOffset() const { return entries.front(); }

    /// Calls `callback(begin, end)` for each granule in order; an empty granule has `begin == end`, and the last one
    /// ends at `data_size`.
    template <typename Callback>
    void forEachGranule(size_t data_size, Callback && callback) const
    {
        for (size_t i = 0; i != entries.size(); ++i)
        {
            size_t begin = entries[i];
            size_t num_empty_granules = 0;
            if (i + 1 != entries.size() && (entries[i + 1] & MORE_GRANULES_FLAG))
                num_empty_granules = entries[++i] & ~MORE_GRANULES_FLAG;
            for (size_t granule = 0; granule != num_empty_granules; ++granule)
                callback(begin, begin);
            callback(begin, i + 1 != entries.size() ? entries[i + 1] : data_size);
        }
    }

    void write(WriteBuffer & out) const
    {
        writeVarUInt(entries.size(), out);
        for (UInt64 entry : entries)
            writeVarUInt(entry, out);
    }

    /// `max_size` is the size of the encoded data: each entry takes at least one byte, which bounds the count before
    /// it drives the allocation.
    void read(ReadBuffer & in, size_t max_size)
    {
        size_t num_entries = 0;
        readVarUInt(num_entries, in);
        if (num_entries > max_size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Granule offsets of the typed shared data of Object declare {} entries in {} bytes",
                num_entries, max_size);
        entries.resize(num_entries);
        for (auto & entry : entries)
            readVarUInt(entry, in);
    }

    /// Frees the memory.
    void clear() { std::vector<UInt64>{}.swap(entries); }

    [[nodiscard]] size_t allocatedBytes() const { return entries.capacity() * sizeof(UInt64); }

private:
    /// Set on an entry after an offset: the number of granules starting at that offset besides the first. All the
    /// granules at an offset but the last are empty.
    static constexpr UInt64 MORE_GRANULES_FLAG = 1ULL << 63;

    [[nodiscard]] size_t lastOffset() const { return (entries.back() & MORE_GRANULES_FLAG) ? entries[entries.size() - 2] : entries.back(); }

    std::vector<UInt64> entries;
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

/// Serializes one path's rows [offset, offset + num_rows) of a granule through `path_settings`: a fresh state, prefix,
/// one bulk call, suffix. The reader decodes it sequentially up to the next granule, so the suffix must write nothing.
void writePlainLayoutPathGranule(
    const SerializationObjectTypedSharedData::SharedTypedPath & path,
    const IColumn & column,
    size_t offset,
    size_t num_rows,
    ISerialization::SerializeBinaryBulkSettings & path_settings)
{
    ISerialization::SerializeBinaryBulkStatePtr path_state;
    path.serialization->serializeBinaryBulkStatePrefix(column, path_settings, path_state);
    path.serialization->serializeBinaryBulkWithMultipleStreams(column, offset, num_rows, path_settings, path_state);
    auto mark_after_data = path_settings.stream_mark_getter(path_settings.path);
    path.serialization->serializeBinaryBulkStateSuffix(path_settings, path_state);
    if (path_settings.stream_mark_getter(path_settings.path) != mark_after_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Suffix of path {} wrote data into the typed shared data of Object", path.name);
}

/// Rows [offset, offset + rows) of a block's columns of the paths, which are immutable while referenced. Empty columns
/// for a zero-row granule.
struct GranuleSlice
{
    Columns path_columns;
    size_t offset = 0;
    size_t rows = 0;
};

/// The serialize settings of a path into its buffer: those of the column, a column of a Wide part, with the getter and
/// the mark getter of the path's substream buffers. Nested typed shared data keeps the plain layout.
ISerialization::SerializeBinaryBulkSettings getPathBufferSerializeSettings(const ISerialization::SerializeBinaryBulkSettings & settings)
{
    auto path_settings = settings;
    path_settings.path.clear();
    path_settings.getter = {};
    path_settings.stream_mark_getter = {};
    path_settings.stream_marks_are_file_positions = false;
    path_settings.get_temporary_data_scope = {};
    path_settings.object_typed_shared_data_serialization_version = MergeTreeObjectTypedSharedDataSerializationVersion::V1;
    return path_settings;
}

ISerialization::EnumerateStreamsSettings getPathBufferEnumerateSettings(const ISerialization::SerializeBinaryBulkSettings & settings)
{
    ISerialization::EnumerateStreamsSettings enumerate_settings;
    enumerate_settings.object_serialization_version = settings.object_serialization_version;
    enumerate_settings.object_shared_data_serialization_version = settings.object_shared_data_serialization_version;
    enumerate_settings.object_shared_data_buckets = settings.object_shared_data_buckets;
    enumerate_settings.object_shared_data_target_chunk_rows = settings.object_shared_data_target_chunk_rows;
    enumerate_settings.object_typed_shared_data_serialization_version = MergeTreeObjectTypedSharedDataSerializationVersion::V1;
    enumerate_settings.max_buckets_in_map = settings.max_buckets_in_map;
    enumerate_settings.map_buckets_strategy = settings.map_buckets_strategy;
    enumerate_settings.map_buckets_coefficient = settings.map_buckets_coefficient;
    enumerate_settings.map_buckets_min_avg_size = settings.map_buckets_min_avg_size;
    enumerate_settings.data_part_type = MergeTreeDataPartType::Wide;
    return enumerate_settings;
}

/// Writes the regions of a part, one after another from the start of .paths_region_data, each as a Wide part's writer
/// writes a file: a mark at each of the region's marks, the block cut at its end. The marks slice of a region, a mark per
/// granule and the final mark (the region's end) relative to the region's start, goes into .paths_region_marks in
/// blocks of its own. Every region is recorded in its path's record of the header, which stores only the sizes.
class RegionsWriter
{
public:
    RegionsWriter(
        const SerializationObjectTypedSharedData::SharedTypedPaths & paths_,
        ISerialization::SerializeBinaryBulkSettings & settings_,
        size_t num_granules_,
        std::vector<SerializationObjectTypedSharedData::PathRecord> & records_)
        : paths(paths_)
        , settings(settings_)
        , num_granules(num_granules_)
        , records(records_)
        , data_stream(getStream(ISerialization::Substream::ObjectTypedSharedDataPathsRegionData, settings))
        , marks_stream(getStream(ISerialization::Substream::ObjectTypedSharedDataPathsRegionMarks, settings))
    {
        if (!settings.stream_mark_getter)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark getter is not set for the typed shared data of Object");
    }

    void startRegion(size_t path_index_, const String & name)
    {
        path_index = path_index_;
        current_region = {};
        current_region.name = name;

        auto begin = getDataMark();
        current_region.region_begin = begin.offset_in_compressed_file;
        if (begin.offset_in_decompressed_block != 0 || current_region.region_begin != next_region_begin)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Region '{}' of path {} of the typed shared data of Object starts at ({}, {}), expected ({}, 0)",
                name, paths[path_index].name, current_region.region_begin, begin.offset_in_decompressed_block, next_region_begin);

        /// The previous slice's cut left the marks stream at a block boundary.
        current_region.marks_begin = getMarksMark().offset_in_compressed_file;
        if (current_region.marks_begin != next_marks_begin)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Marks of region '{}' of path {} of the typed shared data of Object start at {}, expected {}",
                name, paths[path_index].name, current_region.marks_begin, next_marks_begin);

        uncompressed_begin = data_stream->count();
        num_marks = 0;
    }

    /// The stream the region's bytes are written into.
    WriteBuffer & data() { return *data_stream; }

    void addMark()
    {
        /// A region has one mark per granule of the part. Caught here, before the mark is written, so that a decoded
        /// granule count larger than the part's (an empty-granule run from a corrupt spill segment) fails at once
        /// instead of writing marks until the disk fills; the final count is checked again in `endRegion`.
        if (num_marks >= num_granules)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Region '{}' of path {} of the typed shared data of Object has more granules than the {} of the part",
                current_region.name, paths[path_index].name, num_granules);

        /// As a Wide part's writer before a mark: a full block is flushed, and a new block starts once the open one is
        /// worth compressing.
        data_stream->nextIfAtEnd();
        if (data_stream->offset() >= settings.min_compress_block_size)
            data_stream->next();
        auto mark = getDataMark();
        writeMark(mark.offset_in_compressed_file - current_region.region_begin, mark.offset_in_decompressed_block);
        ++num_marks;
    }

    void endRegion()
    {
        if (num_marks != num_granules)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Region '{}' of path {} of the typed shared data of Object has {} marks for {} granules",
                current_region.name, paths[path_index].name, num_marks, num_granules);

        data_stream->next();
        current_region.size.compressed = getDataMark().offset_in_compressed_file - current_region.region_begin;
        current_region.size.uncompressed = data_stream->count() - uncompressed_begin;
        writeMark(current_region.size.compressed, 0);
        marks_stream->next();
        current_region.marks_end = getMarksMark().offset_in_compressed_file;

        next_region_begin = current_region.region_begin + current_region.size.compressed;
        next_marks_begin = current_region.marks_end;
        auto & record = records[path_index];
        record.size.compressed += current_region.size.compressed;
        record.size.uncompressed += current_region.size.uncompressed;
        record.substreams.push_back(std::move(current_region));
    }

private:
    MarkInCompressedFile getDataMark() { return getStreamMark(ISerialization::Substream::ObjectTypedSharedDataPathsRegionData, settings); }
    MarkInCompressedFile getMarksMark() { return getStreamMark(ISerialization::Substream::ObjectTypedSharedDataPathsRegionMarks, settings); }

    void writeMark(size_t offset_in_region, size_t offset_in_decompressed_block)
    {
        writeBinaryLittleEndian(static_cast<UInt64>(offset_in_region), *marks_stream);
        writeBinaryLittleEndian(static_cast<UInt64>(offset_in_decompressed_block), *marks_stream);
    }

    const SerializationObjectTypedSharedData::SharedTypedPaths & paths;
    ISerialization::SerializeBinaryBulkSettings & settings;
    const size_t num_granules;
    std::vector<SerializationObjectTypedSharedData::PathRecord> & records;
    WriteBuffer * data_stream;
    WriteBuffer * marks_stream;

    size_t next_region_begin = 0;
    size_t next_marks_begin = 0;

    /// The region being written.
    size_t path_index = 0;
    SerializationObjectTypedSharedData::SubstreamRegion current_region;
    size_t uncompressed_begin = 0;
    size_t num_marks = 0;
};

/// The buffered data of one SHARED path of a part with regions, serialized like a column of a Wide part with one state
/// for the whole part: per substream its bytes, in memory or spilled, and the positions of its marks.
class PathBuffer : private boost::noncopyable
{
public:
    /// Enumerates the path's substreams with `column`, the first block's, and serializes the prefix. `spiller_` is null
    /// when the part does not spill.
    PathBuffer(
        const SerializationObjectTypedSharedData::SharedTypedPath & path_,
        size_t path_index_,
        const IColumn & column,
        const ISerialization::SerializeBinaryBulkSettings & column_settings,
        SerializationSpiller * spiller_)
        : path(path_)
        , path_index(path_index_)
        , spiller(spiller_)
        , settings(getPathBufferSerializeSettings(column_settings))
    {
        auto enumerate_settings = getPathBufferEnumerateSettings(column_settings);
        auto data = ISerialization::SubstreamData(path.serialization).withType(path.type).withColumn(column.getPtr());
        path.serialization->enumerateStreams(
            enumerate_settings,
            [&](const ISerialization::SubstreamPath & substream_path)
            {
                if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                    return;
                auto name = SerializationObjectTypedSharedData::getSubstreamName(substream_path);
                if (substreams_by_name.contains(name))
                    return;
                substreams.push_back(std::make_unique<SubstreamBuffer>(name));
                substreams_by_name.emplace(std::move(name), substreams.back().get());
            },
            data);

        settings.getter = [this](const ISerialization::SubstreamPath & substream_path) -> WriteBuffer *
        {
            return &getSubstream(substream_path).out;
        };
        /// A mark is {the granule's index, the uncompressed offset from the granule's start}: the reader seeks the region
        /// to the granule's mark and skips the offset.
        settings.stream_mark_getter = [this](const ISerialization::SubstreamPath & substream_path)
        {
            if (num_granules == 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object asks for a mark before its first granule", path.name);
            const auto & substream = getSubstream(substream_path);
            return MarkInCompressedFile{num_granules - 1, substream.position() - substream.granule_start};
        };
        path.serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    }

    /// Adds a granule given by its slices of blocks: the marks of every substream, then the rows.
    void addGranule(std::span<const GranuleSlice> granule_slices)
    {
        ++num_granules;
        for (auto & substream : substreams)
            substream->markGranuleStart();

        for (size_t i = 0; i != granule_slices.size(); ++i)
        {
            const auto & slice = granule_slices[i];
            settings.granule_is_complete = i + 1 == granule_slices.size();
            path.serialization->serializeBinaryBulkWithMultipleStreams(
                *slice.path_columns[path_index], slice.offset, slice.rows, settings, state);
        }
    }

    /// Adds the suffix; nothing is added after it.
    void finish() { path.serialization->serializeBinaryBulkStateSuffix(settings, state); }

    /// The memory of the bytes in memory.
    size_t allocatedBytes() const
    {
        size_t bytes = 0;
        for (const auto & substream : substreams)
            bytes += substream->allocatedBytes();
        return bytes;
    }

    /// The memory spillLargest would free; 0 when nothing is in memory.
    size_t largestSpillableBytes() const
    {
        const auto * largest = findLargestSpillable();
        return largest ? largest->allocatedBytes() : 0;
    }

    /// Spills the in-memory bytes of the biggest substream as one segment. False when spilled data was lost.
    bool spillLargest()
    {
        auto * substream = findLargestSpillable();
        if (!substream)
            return true;
        if (!spiller)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object spills without a spiller", path.name);

        /// A segment is the offsets of its granules, the size of its data, then the data.
        const size_t data_size = substream->out.count();
        WriteBufferFromOwnString header_out;
        substream->mark_offsets.write(header_out);
        writeVarUInt(data_size, header_out);
        const String & header = header_out.str();

        auto write_segment = [&](WriteBuffer & out)
        {
            out.write(header.data(), header.size());
            copyData(*substream->out.readWritten(), out, data_size);
        };
        auto spilled_segment_id = spiller->spill(header.size() + data_size, write_segment);
        if (!spilled_segment_id)
            return false;

        substream->spilled_segments.push_back(*spilled_segment_id);
        substream->spilled_bytes += substream->out.count();
        substream->mark_offsets.clear();
        /// Frees the data and keeps the buffer, which the getter has handed out.
        substream->out.clear();
        return true;
    }

    /// Writes a region per substream, in the order of the enumeration, the spilled bytes of each first. False when
    /// spilled data was lost.
    bool writeRegions(RegionsWriter & regions_writer)
    {
        for (auto & substream : substreams)
        {
            regions_writer.startRegion(path_index, substream->name);
            auto & out = regions_writer.data();

            auto copy_granules = [&](ReadBuffer & in, size_t data_size, const GranuleOffsets & offsets)
            {
                /// The bytes the prefix wrote before the first granule belong to none.
                copyData(in, out, offsets.empty() ? data_size : offsets.firstOffset());
                offsets.forEachGranule(data_size, [&](size_t begin, size_t end)
                {
                    regions_writer.addMark();
                    copyData(in, out, end - begin);
                });
            };

            if (!substream->spilled_segments.empty() && !spiller)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object has spilled segments without a spiller", path.name);
            for (auto spilled_segment_id : substream->spilled_segments)
            {
                auto read_segment = [&](ReadBuffer & in, size_t size)
                {
                    const size_t begin = in.count();
                    GranuleOffsets offsets;
                    offsets.read(in, size);
                    size_t data_size = 0;
                    readVarUInt(data_size, in);
                    copy_granules(in, data_size, offsets);
                    /// The reader is not limited to the segment, so a corrupt offset or data size that read past its
                    /// bytes is caught here rather than reading into the next segment.
                    if (in.count() - begin != size)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Spilled segment of path {} of the typed shared data of Object has {} bytes, but decoding read {}",
                            path.name, size, in.count() - begin);
                };
                if (!spiller->readSpilledSegment(spilled_segment_id, read_segment))
                    return false;
            }

            /// Then the in-memory bytes. The buffer is read only here, for the last time.
            const size_t data_size = substream->out.count();
            auto in = substream->out.tryGetReadBuffer();
            copy_granules(*in, data_size, substream->mark_offsets);

            regions_writer.endRegion();
        }
        return true;
    }

private:
    /// The bytes of one substream and the positions of its marks. The earlier bytes may be spilled to temporary files
    /// as segments; the rest are in `out`.
    struct SubstreamBuffer
    {
        explicit SubstreamBuffer(String name_) : name(std::move(name_)) { }

        /// The bytes written to the substream so far, spilled included.
        size_t position() const { return spilled_bytes + out.count(); }

        /// The memory of the in-memory bytes and of the marks.
        size_t allocatedBytes() const { return out.allocatedBytes() + mark_offsets.allocatedBytes(); }

        void markGranuleStart()
        {
            mark_offsets.add(out.count());
            granule_start = position();
        }

        String name;
        PiecewiseWriteBuffer out;
        /// The positions in `out` of the marks still in memory. Those of granules without bytes in the substream take
        /// no memory each, so a substream that never has bytes and is never spilled does not grow.
        GranuleOffsets mark_offsets;
        std::vector<SerializationSpiller::SpilledSegmentId> spilled_segments;
        size_t spilled_bytes = 0;
        /// The position of the current granule's start: the internal marks of the substream are offsets from it.
        size_t granule_start = 0;
    };

    SubstreamBuffer & getSubstream(const ISerialization::SubstreamPath & substream_path) const
    {
        auto name = SerializationObjectTypedSharedData::getSubstreamName(substream_path);
        auto it = substreams_by_name.find(name);
        if (it == substreams_by_name.end())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object writes substream '{}' it does not have", path.name, name);
        return *it->second;
    }

    SubstreamBuffer * findLargestSpillable() const
    {
        SubstreamBuffer * largest = nullptr;
        for (const auto & substream : substreams)
        {
            if (substream->out.count() && (!largest || substream->allocatedBytes() > largest->allocatedBytes()))
                largest = substream.get();
        }
        return largest;
    }

    const SerializationObjectTypedSharedData::SharedTypedPath & path;
    const size_t path_index;
    SerializationSpiller * const spiller;
    std::vector<std::unique_ptr<SubstreamBuffer>> substreams;
    std::unordered_map<String, SubstreamBuffer *> substreams_by_name;
    ISerialization::SerializeBinaryBulkSettings settings;
    ISerialization::SerializeBinaryBulkStatePtr state;
    /// Every granule of the part is added, in order.
    size_t num_granules = 0;
};

/// The granule being written, as slices of the blocks it spans: one per serialize call, several only for a granule
/// that spans blocks.
class PendingGranule
{
public:
    explicit PendingGranule(const SerializationObjectTypedSharedData::SharedTypedPaths & paths_) : paths(paths_) { }

    /// Adds the call's rows [offset, offset + limit); a zero-row call adds nothing.
    void add(const ColumnObject & column, size_t offset, size_t limit)
    {
        size_t end = limit == 0 ? column.size() : std::min(column.size(), offset + limit);
        if (end <= offset)
            return;

        GranuleSlice slice{.path_columns = {}, .offset = offset, .rows = end - offset};
        const auto & typed_paths = column.getTypedPaths();
        slice.path_columns.reserve(paths.size());
        for (const auto & path : paths)
            slice.path_columns.push_back(typed_paths.at(path.name)->getPtr());
        num_rows += slice.rows;

        /// A call that continues the granule within the same block (same columns, adjacent rows) extends the last slice,
        /// so a granule has at most one slice per block, whichever way a caller splits it into calls.
        if (!slices.empty() && slices.back().path_columns.front().get() == slice.path_columns.front().get()
            && slices.back().offset + slices.back().rows == slice.offset)
            slices.back().rows += slice.rows;
        else
            slices.push_back(std::move(slice));
    }

    [[nodiscard]] bool empty() const { return slices.empty(); }
    [[nodiscard]] size_t rows() const { return num_rows; }
    [[nodiscard]] const std::vector<GranuleSlice> & getSlices() const { return slices; }

    /// A path's column of the granule, read at `offset`.
    struct PathColumn
    {
        ColumnPtr column;
        size_t offset = 0;
    };

    /// Materializes the column only for a granule that spans slices.
    [[nodiscard]] PathColumn getPathColumn(size_t path_index) const
    {
        chassert(num_rows != 0);
        if (slices.size() == 1)
            return {slices.front().path_columns[path_index], slices.front().offset};

        const auto & first = *slices.front().path_columns[path_index];
        auto combined = first.cloneEmpty();
        /// Fix the source's dynamic structure on the combined column so the inserts cannot extend or reorder it, keeping it
        /// equal to the part-frozen structure, as the read path does when it decodes into an empty column.
        combined->takeExactDynamicStructureFrom(first);
        combined->reserve(num_rows);
        for (const auto & slice : slices)
            combined->insertRangeFrom(*slice.path_columns[path_index], slice.offset, slice.rows);
        return {std::move(combined), 0};
    }

    void clear()
    {
        slices.clear();
        num_rows = 0;
    }

private:
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths;
    std::vector<GranuleSlice> slices;
    size_t num_rows = 0;
};

/// The buffered data of all SHARED paths of a part with regions, until the regions are written at the end of the part.
/// A PathBuffer per path is created at the first granule with rows. While the part has seen one block, its granules are
/// deferred: kept as slices referencing the block, which holds the data already, so nothing is spilled; when the regions
/// are written each path is serialized from them right before its regions, one path at a time. The second block adds
/// them to every path's buffer, and from then on the buffers are kept under the memory budget, spilling, until finish;
/// writing the regions only reads.
class PathsBuffer
{
public:
    using SharedTypedPaths = SerializationObjectTypedSharedData::SharedTypedPaths;

    PathsBuffer(const SharedTypedPaths & paths_, const ISerialization::SerializeBinaryBulkSettings & settings)
        : paths(paths_)
        , budget(settings.serialization_spill_max_bytes_to_buffer)
        , buffered_bytes(settings.serialization_spill_buffered_bytes ? settings.serialization_spill_buffered_bytes : std::make_shared<size_t>(0))
    {
        if (budget && settings.get_temporary_data_scope)
        {
            /// The columns of a write share the scopes, and with them the limit of the temporary storage.
            spiller.emplace(
                settings.get_temporary_data_scope,
                "serialization_spill",
                settings.serialization_spill_max_temporary_data_size,
                settings.serialization_spill_to_part_disk_only,
                settings.serialization_spill_error_mode == MergeTreeSerializationSpillErrorMode::THROW);
        }
    }

    ~PathsBuffer() { setMemoryBytes(0); }

    void addGranule(const std::vector<GranuleSlice> & granule_slices, const ISerialization::SerializeBinaryBulkSettings & settings)
    {
        ++num_granules;
        if (spilled_data_lost)
            return;

        /// The first granule with rows is from the part's first block.
        if (path_buffers.empty() && !granule_slices.empty())
            createPathBuffers(&granule_slices.front().path_columns, settings);

        if (defer_granules)
        {
            /// The deferred slices hold the first block's columns alive, so a different column pointer is another block.
            bool from_first_block = std::ranges::all_of(
                granule_slices, [&](const auto & slice) { return slice.path_columns.front().get() == first_block_column.get(); });
            if (from_first_block)
            {
                /// A granule within one block is one slice (PendingGranule squashes same-block calls).
                chassert(granule_slices.size() <= 1);
                deferred_granules.push_back(granule_slices.empty() ? GranuleSlice{} : granule_slices.front());
                return;
            }

            /// The budget applies from here, to the deferred granules too.
            defer_granules = false;
            for (size_t path_index = 0; path_index != paths.size() && !spilled_data_lost; ++path_index)
                addDeferredGranules(path_index);
            deferred_granules.clear();
            first_block_column.reset();
        }

        for (size_t path_index = 0; path_index != paths.size() && !spilled_data_lost; ++path_index)
            addWithinBudget(path_index, [&](PathBuffer & path_buffer) { path_buffer.addGranule(granule_slices); });
    }

    /// False once data of the paths was lost, which leaves the part without regions.
    bool canWriteRegions() const { return !spilled_data_lost; }

    size_t numGranules() const { return num_granules; }

    /// Adds the suffix of every path whose granules are in its buffer, under the budget: nothing is buffered or spilled
    /// after it. With deferred granules it leaves the paths to writeRegions.
    void finish(const ISerialization::SerializeBinaryBulkSettings & settings)
    {
        if (spilled_data_lost)
            return;
        if (path_buffers.empty())
            createPathBuffers(nullptr, settings);

        if (!defer_granules)
        {
            for (size_t path_index = 0; path_index != paths.size() && !spilled_data_lost; ++path_index)
                addWithinBudget(path_index, [](PathBuffer & path_buffer) { path_buffer.finish(); });
        }
        if (spiller && !spilled_data_lost)
            spiller->finishSpilling();
    }

    /// Writes the regions of every path in order, each path freed after; a path with deferred granules is serialized from
    /// them first. Called after finish. False when spilled data was lost.
    bool writeRegions(RegionsWriter & regions_writer)
    {
        /// Nothing is spilled after finish.
        spill_order.clear();
        spillable_bytes.clear();

        for (size_t path_index = 0; path_index != paths.size(); ++path_index)
        {
            if (defer_granules)
                serializeDeferredPath(path_index);

            /// Writing the regions frees the path's buffer as it goes.
            auto & path_buffer = path_buffers[path_index];
            if (!path_buffer->writeRegions(regions_writer))
            {
                dropAfterLostData();
                return false;
            }
            path_buffer.reset();
        }
        deferred_granules.clear();
        first_block_column.reset();
        /// The next column's suffix runs after this one, and sees only what is still buffered.
        setMemoryBytes(0);
        return true;
    }

    /// Drops the spill files once the regions are written, before the part is finalized.
    void releaseSpilledData()
    {
        if (spiller)
            spiller->release();
    }

private:
    /// The prefixes take the first block's columns, as a Wide part's writer takes the first block's; empty columns in a
    /// part without rows. Called while the granules are deferred, so outside the budget.
    void createPathBuffers(const Columns * first_block_columns, const ISerialization::SerializeBinaryBulkSettings & settings)
    {
        path_buffers.resize(paths.size());
        spillable_bytes.assign(paths.size(), 0);
        for (size_t path_index = 0; path_index != paths.size(); ++path_index)
        {
            ColumnPtr column = first_block_columns ? (*first_block_columns)[path_index] : paths[path_index].type->createColumn();
            auto & path_buffer = path_buffers[path_index];
            path_buffer = std::make_unique<PathBuffer>(paths[path_index], path_index, *column, settings, spiller ? &*spiller : nullptr);
            setMemoryBytes(memory_bytes + path_buffer->allocatedBytes());
            /// The prefix is in the buffer already.
            updateSpillOrder(path_index);
        }
        if (first_block_columns)
            first_block_column = first_block_columns->front();
    }

    /// Adds the path's deferred granules and its suffix, outside the budget.
    void serializeDeferredPath(size_t path_index)
    {
        auto & path_buffer = *path_buffers[path_index];
        for (const auto & granule : deferred_granules)
            path_buffer.addGranule(std::span<const GranuleSlice>(&granule, granule.rows != 0 ? 1 : 0));
        path_buffer.finish();
    }

    /// Moves the deferred granules into the path's buffer once the part has a second block, under the budget.
    void addDeferredGranules(size_t path_index)
    {
        for (const auto & granule : deferred_granules)
        {
            addWithinBudget(path_index, [&](PathBuffer & path_buffer)
            {
                path_buffer.addGranule(std::span<const GranuleSlice>(&granule, granule.rows != 0 ? 1 : 0));
            });
            if (spilled_data_lost)
                return;
        }
    }

    /// Runs an addition to the path's buffer and keeps the budget (enforceBudget), which may lose spilled data.
    void addWithinBudget(size_t path_index, const std::function<void(PathBuffer &)> & add)
    {
        auto & path_buffer = *path_buffers[path_index];
        size_t bytes_before = path_buffer.allocatedBytes();
        add(path_buffer);
        setMemoryBytes(memory_bytes - bytes_before + path_buffer.allocatedBytes());
        updateSpillOrder(path_index);
        enforceBudget();
    }

    /// Spills from the biggest path buffers of this column to temporary files while the memory of all JSON columns of the
    /// part write exceeds the budget. Another column's buffers grow only in its own call, which keeps the budget too.
    void enforceBudget()
    {
        if (!spiller)
            return;
        while (!spilled_data_lost && *buffered_bytes > budget && !spill_order.empty())
        {
            const size_t path_index = spill_order.begin()->second;
            auto & largest = *path_buffers[path_index];
            size_t bytes_before = largest.allocatedBytes();
            if (!largest.spillLargest())
            {
                dropAfterLostData();
                return;
            }
            setMemoryBytes(memory_bytes - bytes_before + largest.allocatedBytes());
            updateSpillOrder(path_index);
        }
    }

    /// Puts the path at its place in spill_order by the bytes it can spill now.
    void updateSpillOrder(size_t path_index)
    {
        if (!spiller)
            return;
        auto & bytes = spillable_bytes[path_index];
        if (bytes)
            spill_order.erase({bytes, path_index});
        bytes = path_buffers[path_index] ? path_buffers[path_index]->largestSpillableBytes() : 0;
        if (bytes)
            spill_order.emplace(bytes, path_index);
    }

    /// Sets this column's buffered memory and keeps the part write's total in step.
    void setMemoryBytes(size_t bytes)
    {
        *buffered_bytes -= memory_bytes;
        *buffered_bytes += bytes;
        memory_bytes = bytes;
    }

    /// Spilled data was lost: drops the rest, which cannot make the regions any more; the plain layout is written as always.
    void dropAfterLostData()
    {
        spilled_data_lost = true;
        path_buffers.clear();
        spill_order.clear();
        spillable_bytes.clear();
        deferred_granules.clear();
        first_block_column.reset();
        setMemoryBytes(0);
    }

    const SharedTypedPaths & paths;
    /// Over this much memory of the path buffers of all JSON columns of the part write, they are spilled to temporary
    /// files; 0 disables.
    const size_t budget;
    /// The memory of the path buffers of all JSON columns of the part write.
    const std::shared_ptr<size_t> buffered_bytes;
    /// Set with a budget and scopes of temporary data to spill to.
    std::optional<SerializationSpiller> spiller;

    size_t num_granules = 0;

    /// Per path, created at the first granule with rows; each is freed once its regions are written.
    std::vector<std::unique_ptr<PathBuffer>> path_buffers;

    /// Bytes descending, then path index ascending: the first entry is the path to spill from, the first one on a tie.
    struct SpillOrderLess
    {
        bool operator()(const std::pair<size_t, size_t> & lhs, const std::pair<size_t, size_t> & rhs) const
        {
            return lhs.first != rhs.first ? lhs.first > rhs.first : lhs.second < rhs.second;
        }
    };
    /// The paths with bytes to spill, as (largestSpillableBytes, path index); kept only with a spiller.
    std::set<std::pair<size_t, size_t>, SpillOrderLess> spill_order;
    /// Per path, its bytes in spill_order (0 when it is not there).
    std::vector<size_t> spillable_bytes;

    /// While the part has seen one block, its granules; the first path's column of that block tells another block apart.
    bool defer_granules = true;
    ColumnPtr first_block_column;
    std::vector<GranuleSlice> deferred_granules;

    /// The memory of this column's path buffers (`PathBuffer::allocatedBytes`), its part of `buffered_bytes`.
    size_t memory_bytes = 0;
    bool spilled_data_lost = false;
};

struct SerializeBinaryBulkStateObjectTypedSharedData : public ISerialization::SerializeBinaryBulkState
{
    SerializeBinaryBulkStateObjectTypedSharedData(
        const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
        bool with_regions,
        const ISerialization::SerializeBinaryBulkSettings & settings)
        : paths_uncompressed_bytes(paths.size(), 0)
        , granule(paths)
    {
        if (with_regions)
            paths_buffer.emplace(paths, settings);
    }

    SerializationObjectTypedSharedData::Header header;
    /// Serialized bytes of every path so far in the plain layout, for its size record of a Wide part.
    std::vector<size_t> paths_uncompressed_bytes;
    PendingGranule granule;
    /// Set in a part with regions.
    std::optional<PathsBuffer> paths_buffer;
};

/// Writes the granule into .paths_data, each path's granule with a fresh state, and returns where each path's data
/// starts.
GranuleEntry writePlainLayoutPathsData(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    SerializeBinaryBulkStateObjectTypedSharedData & state,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    bool compact = settings.data_part_type == MergeTreeDataPartType::Compact;
    size_t num_rows = state.granule.rows();

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

        auto [column, offset] = state.granule.getPathColumn(path_index);
        writePlainLayoutPathGranule(path, *column, offset, num_rows, path_settings);

        state.paths_uncompressed_bytes[path_index] += data_stream->count() - count_before_path;
    }

    /// The cut after the granule's last path: the next granule's mark is a block start, so a granule's blocks are its own.
    if (!compact)
        data_stream->next();

    return entry;
}

void writePlainLayoutEntry(
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

/// Writes the granule in the plain layout and adds it to the paths buffer, then starts the next one.
void completeGranule(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    SerializeBinaryBulkStateObjectTypedSharedData & state,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    auto entry = writePlainLayoutPathsData(paths, state, settings);
    writePlainLayoutEntry(paths, entry, settings);
    if (state.paths_buffer)
        state.paths_buffer->addGranule(state.granule.getSlices(), settings);
    state.granule.clear();
}

/// A sanity bound on the substreams of one path declared in a v2 header, to reject a corrupt count before allocating.
constexpr size_t MAX_SUBSTREAMS_PER_PATH = 1000000;

SerializationObjectTypedSharedData::HeaderPtr readHeader(ReadBuffer & stream, size_t num_paths, bool wide)
{
    using Version = MergeTreeObjectTypedSharedDataSerializationVersion;
    auto header = std::make_shared<SerializationObjectTypedSharedData::Header>();
    UInt64 version = 0;
    readVarUInt(version, stream);
    if (version != static_cast<UInt64>(Version::V1) && version != static_cast<UInt64>(Version::V2))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid version for the typed shared data of Object: {}", version);
    header->serialization_version = static_cast<Version>(version);

    /// Compact parts write no record; Wide parts append it in the suffix.
    if (!wide)
        return header;

    /// v2 prefixes the record with a flag, since the regions may be absent (a part with the plain layout alone); v1 has
    /// sizes only.
    if (header->serialization_version == Version::V2)
    {
        UInt64 has_regions = 0;
        readVarUInt(has_regions, stream);
        header->has_regions = has_regions != 0;
        if (header->has_regions)
        {
            readVarUInt(header->region_marks_count, stream);
            /// Every region has a mark per granule and the final mark.
            if (header->region_marks_count == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "The header of the typed shared data of Object has regions without marks");
        }
    }

    /// The record is the last thing in the header stream, so bytes after it mean a record of other paths than the type's.
    auto check_end = [&stream]
    {
        if (!stream.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "The header of the typed shared data of Object has more records than the paths of its type");
    };

    header->path_records.resize(num_paths);
    if (!header->has_regions)
    {
        for (auto & record : header->path_records)
        {
            readVarUInt(record.size.compressed, stream);
            readVarUInt(record.size.uncompressed, stream);
        }
        check_end();
        return header;
    }

    /// The regions and the marks slices follow each other from the start of their streams, path by path, substream by
    /// substream, so only their sizes are stored.
    size_t region_begin = 0;
    size_t marks_begin = 0;
    for (auto & record : header->path_records)
    {
        size_t num_substreams = 0;
        readVarUInt(num_substreams, stream);
        /// `num_substreams` comes from the stored header; bound it so a corrupt value cannot drive the allocation
        /// before the records are read. The exact count is not known in advance (a Dynamic path's substreams depend
        /// on its data), so this is a loose sanity limit.
        if (num_substreams > MAX_SUBSTREAMS_PER_PATH)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "The header of the typed shared data of Object declares {} substreams for a path, over the limit of {}",
                num_substreams, MAX_SUBSTREAMS_PER_PATH);
        record.substreams.resize(num_substreams);
        for (auto & substream : record.substreams)
        {
            readStringBinary(substream.name, stream);
            readVarUInt(substream.size.compressed, stream);
            readVarUInt(substream.size.uncompressed, stream);
            size_t marks_size = 0;
            readVarUInt(marks_size, stream);
            substream.region_begin = region_begin;
            substream.marks_begin = marks_begin;
            substream.marks_end = marks_begin + marks_size;
            region_begin += substream.size.compressed;
            marks_begin = substream.marks_end;
            record.size.compressed += substream.size.compressed;
            record.size.uncompressed += substream.size.uncompressed;
        }
    }
    check_end();
    return header;
}

void writeHeaderPrefix(const SerializationObjectTypedSharedData::Header & header, ISerialization::SerializeBinaryBulkSettings & settings)
{
    using Version = MergeTreeObjectTypedSharedDataSerializationVersion;
    auto * stream = getStream(ISerialization::Substream::ObjectTypedSharedDataHeader, settings);
    if (header.serialization_version != Version::V1 && header.serialization_version != Version::V2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unsupported version {} for the typed shared data of Object",
            static_cast<UInt64>(header.serialization_version));
    writeVarUInt(static_cast<UInt64>(header.serialization_version), *stream);
}

/// The record a Wide part's suffix appends to the header stream. With regions it is exact per substream; otherwise (v1,
/// or a v2 part with the plain layout alone) the sizes are apportioned per path, since paths share the plain layout's
/// compressed blocks.
void writeHeaderSuffix(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    const SerializeBinaryBulkStateObjectTypedSharedData & state,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    using Version = MergeTreeObjectTypedSharedDataSerializationVersion;
    auto * stream = getStream(ISerialization::Substream::ObjectTypedSharedDataHeader, settings);

    if (state.header.serialization_version == Version::V2)
    {
        writeVarUInt(state.header.has_regions ? 1 : 0, *stream);
        if (state.header.has_regions)
            writeVarUInt(state.header.region_marks_count, *stream);
    }

    if (state.header.has_regions)
    {
        for (const auto & record : state.header.path_records)
        {
            writeVarUInt(record.substreams.size(), *stream);
            for (const auto & substream : record.substreams)
            {
                writeStringBinary(substream.name, *stream);
                writeVarUInt(substream.size.compressed, *stream);
                writeVarUInt(substream.size.uncompressed, *stream);
                writeVarUInt(substream.marks_end - substream.marks_begin, *stream);
            }
        }
        return;
    }

    /// Without file positions (inside a region, compressed only after the part's data is written) the compressed bytes
    /// are unknown: each path's uncompressed bytes stand for them, and the reader of the region scales them by its ratio.
    std::optional<size_t> total_compressed;
    if (settings.stream_marks_are_file_positions)
        total_compressed = getStreamMark(ISerialization::Substream::ObjectTypedSharedDataPathsData, settings).offset_in_compressed_file;
    size_t total_uncompressed = 0;
    for (auto bytes : state.paths_uncompressed_bytes)
        total_uncompressed += bytes;

    for (size_t path_index = 0; path_index != paths.size(); ++path_index)
    {
        size_t uncompressed = state.paths_uncompressed_bytes[path_index];
        size_t compressed = uncompressed;
        if (total_compressed)
            compressed = total_uncompressed == 0
                ? 0
                : static_cast<size_t>(static_cast<double>(uncompressed) * static_cast<double>(*total_compressed) / static_cast<double>(total_uncompressed));
        writeVarUInt(compressed, *stream);
        writeVarUInt(uncompressed, *stream);
    }
}

/// Writes the v2 regions at the suffix and fills the header's records.
void writeRegions(
    const SerializationObjectTypedSharedData::SharedTypedPaths & paths,
    SerializeBinaryBulkStateObjectTypedSharedData & state,
    ISerialization::SerializeBinaryBulkSettings & settings)
{
    size_t num_granules = state.paths_buffer->numGranules();
    state.header.region_marks_count = num_granules + 1;
    state.header.path_records.assign(paths.size(), {});

    RegionsWriter regions_writer(paths, settings, num_granules, state.header.path_records);
    if (!state.paths_buffer->writeRegions(regions_writer))
    {
        /// Spilled data was lost: the header written after the regions says the part has none, and the bytes already in
        /// the region streams stay unreferenced.
        state.header.has_regions = false;
        state.header.path_records.clear();
        return;
    }

    state.paths_buffer->releaseSpilledData();
    state.header.has_regions = true;
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

namespace
{

std::vector<std::shared_ptr<const ISerialization::Substream::StreamView>>
makeRegionViews(const SerializationObjectTypedSharedData::Header & header, size_t path_index)
{
    using Substream = ISerialization::Substream;
    std::vector<std::shared_ptr<const Substream::StreamView>> views;
    for (const auto & region : header.path_records.at(path_index).substreams)
    {
        auto view = std::make_shared<Substream::StreamView>();
        /// Substream names start with a dot or are empty, so the index ends where the name begins.
        view->key = toString(path_index) + region.name;
        view->data_stream = Substream::ObjectTypedSharedDataPathsRegionData;
        view->marks_stream = Substream::ObjectTypedSharedDataPathsRegionMarks;
        view->begin = region.region_begin;
        view->end = region.region_begin + region.size.compressed;
        view->marks_begin = region.marks_begin;
        view->marks_end = region.marks_end;
        view->marks_count = header.region_marks_count;
        views.push_back(std::move(view));
    }
    return views;
}

}

void SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedData::requestPath(
    size_t path_index, const SharedTypedPath & path, bool from_plain_layout)
{
    auto [it, inserted] = requested_paths.try_emplace(path_index, PathState{path.name, path.type, path.serialization});
    it->second.read_from_plain_layout |= from_plain_layout;
    /// Built once, so that the reads of the path's regions do not build a view per call.
    if (inserted && hasRegions())
        it->second.region_views = makeRegionViews(*header, path_index);
}

ISerialization::Substream SerializationObjectTypedSharedData::getPathElement(const String & path)
{
    Substream element(Substream::ObjectTypedSharedDataPath);
    element.object_path_name = path;
    return element;
}

String SerializationObjectTypedSharedData::getSubstreamName(std::span<const Substream> path_substream_path)
{
    SubstreamPath substream_path;
    substream_path.insert(substream_path.end(), path_substream_path.begin(), path_substream_path.end());
    return ISerialization::getFileNameForStream(String{}, substream_path, StreamFileNameSettings{});
}

std::optional<ISerialization::Substream::SizeHint> SerializationObjectTypedSharedData::pathsSizeHint(
    const DeserializeBinaryBulkStatePtr & shared_state, size_t first_path_index, size_t num_paths)
{
    if (!shared_state)
        return std::nullopt;
    const auto * state = typeid_cast<const DeserializeBinaryBulkStateObjectTypedSharedData *>(shared_state.get());
    if (!state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid shared state of the typed shared data of Object");
    const auto & path_records = state->header->path_records;
    if (path_records.empty())
        return std::nullopt;

    Substream::SizeHint hint;
    for (size_t path_index = first_path_index; path_index != first_path_index + num_paths; ++path_index)
    {
        hint.compressed += path_records.at(path_index).size.compressed;
        hint.uncompressed += path_records.at(path_index).size.uncompressed;
    }
    return hint;
}

void SerializationObjectTypedSharedData::enumerateTypedSharedDataStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, std::optional<Substream::SizeHint> data_size_hint, bool with_regions)
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

    if (with_regions)
    {
        add(Substream::ObjectTypedSharedDataPathsRegionData);
        add(Substream::ObjectTypedSharedDataPathsRegionMarks);
    }
}

void SerializationObjectTypedSharedData::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData &) const
{
    /// The caller has pushed ObjectData and ObjectTypedSharedData. A v2 Wide part has the two region streams, listed
    /// only for an enumeration that sets the part type and the version, which is the writer's: it opens them from here.
    /// A reader's enumeration sets neither, so it lists the plain layout's streams; the regions are read through views.
    bool with_regions = settings.data_part_type == MergeTreeDataPartType::Wide
        && settings.object_typed_shared_data_serialization_version == MergeTreeObjectTypedSharedDataSerializationVersion::V2;
    enumerateTypedSharedDataStreams(settings, callback, std::nullopt, with_regions);
}

void SerializationObjectTypedSharedData::serializeBinaryBulkStatePrefix(
    const IColumn &, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    /// Compact parts and nested typed shared data keep the v1 layout, so the header names the layout actually written.
    auto version = settings.data_part_type == MergeTreeDataPartType::Compact ? MergeTreeObjectTypedSharedDataSerializationVersion::V1
                                                                             : settings.object_typed_shared_data_serialization_version;
    bool with_regions = version == MergeTreeObjectTypedSharedDataSerializationVersion::V2;
    auto typed_shared_data_state = std::make_shared<SerializeBinaryBulkStateObjectTypedSharedData>(paths, with_regions, settings);
    typed_shared_data_state->header.serialization_version = version;
    writeHeaderPrefix(typed_shared_data_state->header, settings);
    state = std::move(typed_shared_data_state);
}

void SerializationObjectTypedSharedData::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto * typed_shared_data_state = checkAndGetState<SerializeBinaryBulkStateObjectTypedSharedData>(state);
    auto & granule = typed_shared_data_state->granule;

    /// In Compact parts every call is a complete granule.
    bool compact = settings.data_part_type == MergeTreeDataPartType::Compact;
    if (compact && !granule.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A granule of a Compact part spans serialize calls of the typed shared data of Object");

    granule.add(assert_cast<const ColumnObject &>(column), offset, limit);
    if (compact || settings.granule_is_complete)
        completeGranule(paths, *typed_shared_data_state, settings);
}

void SerializationObjectTypedSharedData::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto * typed_shared_data_state = checkAndGetState<SerializeBinaryBulkStateObjectTypedSharedData>(state);
    /// Slices left over belong to the part's last granule, which the writer completes without a data call.
    if (!typed_shared_data_state->granule.empty())
        completeGranule(paths, *typed_shared_data_state, settings);

    if (settings.data_part_type == MergeTreeDataPartType::Compact)
        return;

    if (auto & paths_buffer = typed_shared_data_state->paths_buffer)
    {
        paths_buffer->finish(settings);
        if (paths_buffer->canWriteRegions())
            writeRegions(paths, *typed_shared_data_state, settings);
    }
    writeHeaderSuffix(paths, *typed_shared_data_state, settings);
}

ISerialization::DeserializeBinaryBulkStatePtr SerializationObjectTypedSharedData::deserializeSharedState(
    DeserializeBinaryBulkSettings & settings, SubstreamsDeserializeStatesCache * cache, size_t num_paths)
{
    settings.path.push_back(Substream::ObjectTypedSharedDataHeader);
    SCOPE_EXIT(settings.path.pop_back());
    if (auto cached_state = getFromSubstreamsDeserializeStatesCache(cache, settings.path))
        return cached_state;

    auto * stream = settings.getter(settings.path);
    if (!stream)
        return nullptr;

    auto shared_state = std::make_shared<DeserializeBinaryBulkStateObjectTypedSharedData>();
    shared_state->header = readHeader(*stream, num_paths, settings.data_part_type != MergeTreeDataPartType::Compact);
    shared_state->num_paths = num_paths;
    addToSubstreamsDeserializeStatesCache(cache, settings.path, shared_state);
    return shared_state;
}

void SerializationObjectTypedSharedData::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache * cache) const
{
    auto shared_state = deserializeSharedState(settings, cache, paths.size());
    if (!shared_state)
        return;

    /// The whole column reads every path, from the plain layout; its state is the column's shared state.
    auto * typed_shared_data_state = checkAndGetState<DeserializeBinaryBulkStateObjectTypedSharedData>(shared_state);
    for (size_t path_index = 0; path_index != paths.size(); ++path_index)
        typed_shared_data_state->requestPath(path_index, paths[path_index], /*from_plain_layout=*/ true);
    state = std::move(shared_state);
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
    const SerializationObjectTypedSharedData::PathState & path,
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
    SerializationObjectTypedSharedData::PathState * path = nullptr;
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
    size_t & rows_left)
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
            destination.path->remainder_consumed = rows;
        }
        previous_index = destination.path_index;
    }

    rows_left -= rows;
}

/// For a substream path of the serialization of the path `path_index`, whose first `prefix_size` elements end with the
/// path's identity element: the same path with the view of the substream's region on the element after them, where
/// the path's own substream path starts. Nothing when the part has no region for it.
std::optional<ISerialization::SubstreamPath> getRegionPath(
    const ISerialization::SubstreamPath & substream_path,
    size_t prefix_size,
    const SerializationObjectTypedSharedData::Header & header,
    size_t path_index,
    const SerializationObjectTypedSharedData::PathState & path_state)
{
    if (substream_path.size() <= prefix_size)
        return std::nullopt;
    auto name = SerializationObjectTypedSharedData::getSubstreamName(std::span(substream_path).subspan(prefix_size));
    const auto & regions = header.path_records.at(path_index).substreams;
    auto region = std::ranges::find(regions, name, &SerializationObjectTypedSharedData::SubstreamRegion::name);
    if (region == regions.end())
        return std::nullopt;

    auto region_path = substream_path;
    region_path[prefix_size].view = path_state.region_views.at(static_cast<size_t>(region - regions.begin()));
    /// A hint of the path's serialization is its bytes of a part of the substream, with only the uncompressed ones
    /// known inside a region: the compressed ones are scaled by the region's ratio.
    auto & size_hint = region_path.back().size_hint;
    if (!size_hint)
        size_hint = region->size;
    else if (region->size.uncompressed == 0)
        size_hint->compressed = 0;
    else
        size_hint->compressed = static_cast<size_t>(
            static_cast<double>(size_hint->uncompressed) * static_cast<double>(region->size.compressed)
            / static_cast<double>(region->size.uncompressed));
    return region_path;
}

/// The settings of the path `path_index`'s serialization read over the regions: every callback that takes a substream
/// path gets the path of its region. settings.path ends with the path's identity element.
ISerialization::DeserializeBinaryBulkSettings getPathRegionsDeserializeSettings(
    const ISerialization::DeserializeBinaryBulkSettings & settings,
    const SerializationObjectTypedSharedData::DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
    size_t path_index)
{
    using SubstreamPath = ISerialization::SubstreamPath;
    auto region_settings = settings;
    const size_t prefix_size = settings.path.size();
    const auto * header = shared_state.header.get();
    const auto * path_state = &shared_state.requested_paths.at(path_index);
    const auto * path_name = &path_state->name;
    auto region_path = [header, prefix_size, path_index, path_state](const SubstreamPath & substream_path)
    {
        return getRegionPath(substream_path, prefix_size, *header, path_index, *path_state);
    };

    region_settings.getter = [getter = settings.getter, region_path, path_name, prefix_size](const SubstreamPath & substream_path) -> ReadBuffer *
    {
        auto path = region_path(substream_path);
        if (!path)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Path {} of the typed shared data of Object has no region of substream '{}'",
                *path_name, SerializationObjectTypedSharedData::getSubstreamName(std::span(substream_path).subspan(prefix_size)));
        return getter(*path);
    };
    region_settings.check_stream_exists_callback = [region_path](const SubstreamPath & substream_path)
    {
        return region_path(substream_path).has_value();
    };
    /// A mark the path's serialization stored in its data is {granule index, uncompressed offset from the granule's start}.
    region_settings.seek_stream_to_mark_callback
        = [seek_to_granule = settings.seek_stream_to_granule_callback, region_path, path_name](
              const SubstreamPath & substream_path, const MarkInCompressedFile & mark)
    {
        if (!seek_to_granule)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object seeks by a mark without a granule seek callback", *path_name);
        auto path = region_path(substream_path);
        if (!path)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object seeks a substream without a region", *path_name);
        auto * stream = seek_to_granule(*path, mark.offset_in_compressed_file);
        if (!stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object seeks a region it cannot read", *path_name);
        stream->ignore(mark.offset_in_decompressed_block);
    };

    auto with_region_path = [&region_path](const auto & callback) -> std::function<void(const SubstreamPath &)>
    {
        if (!callback)
            return {};
        return [callback, region_path](const SubstreamPath & substream_path)
        {
            if (auto path = region_path(substream_path))
                callback(*path);
        };
    };
    region_settings.seek_stream_to_current_mark_callback = with_region_path(settings.seek_stream_to_current_mark_callback);
    region_settings.seek_to_start_callback = with_region_path(settings.seek_to_start_callback);
    region_settings.release_stream_callback = with_region_path(settings.release_stream_callback);
    region_settings.prefixes_prefetch_callback = with_region_path(settings.prefixes_prefetch_callback);

    if (settings.has_uniform_marks_callback)
    {
        region_settings.has_uniform_marks_callback
            = [callback = settings.has_uniform_marks_callback, region_path](const SubstreamPath & substream_path, size_t max_transitions)
        {
            auto path = region_path(substream_path);
            return path && callback(*path, max_transitions);
        };
    }
    if (settings.get_avg_value_size_hint_callback)
    {
        region_settings.get_avg_value_size_hint_callback
            = [callback = settings.get_avg_value_size_hint_callback, region_path](const SubstreamPath & substream_path)
        {
            auto path = region_path(substream_path);
            return path ? callback(*path) : 0.0;
        };
    }
    if (settings.update_avg_value_size_hint_callback)
    {
        region_settings.update_avg_value_size_hint_callback
            = [callback = settings.update_avg_value_size_hint_callback, region_path](const SubstreamPath & substream_path, const IColumn & column)
        {
            if (auto path = region_path(substream_path))
                callback(*path, column);
        };
    }
    return region_settings;
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

void SerializationObjectTypedSharedData::deserializePlainLayoutPaths(
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStateObjectTypedSharedData & state,
    SubstreamsCache * cache,
    const std::function<IColumn *(size_t path_index)> & own_destination)
{
    bool compact = settings.data_part_type == MergeTreeDataPartType::Compact;

    std::vector<PathDestination> destinations;
    std::vector<MutableColumnPtr> call_columns;
    destinations.reserve(state.requested_paths.size());
    for (auto & [path_index, path] : state.requested_paths)
    {
        if (state.readsFromRegions(path_index))
            continue;

        IColumn * column = own_destination(path_index);
        if (!column)
        {
            call_columns.push_back(path.type->createColumn());
            column = call_columns.back().get();
        }
        destinations.push_back({path_index, &path, column, column->size()});
    }

    if (destinations.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No path of the typed shared data of Object is read from the plain layout");

    ProfileEvents::increment(ProfileEvents::ObjectTypedSharedDataPlainLayoutReads);

    /// With every path decoded here, a granule's data is read sequentially, without seeks to the paths' marks.
    bool all_paths = destinations.size() == state.num_paths;

    /// A call that does not continue the previous one starts at a granule the reader positioned the streams at, so the
    /// remainders are stale. One decision serves all readers of the plain layout: a remainder outlives only the call that cut
    /// the granule, which the same reader continues, and a reader entering later starts at a granule boundary.
    if (!settings.continuous_reading)
    {
        for (auto & destination : destinations)
        {
            destination.path->remainder.reset();
            destination.path->remainder_consumed = 0;
        }
    }

    size_t rows_left = limit;

    /// The granule the previous call cut goes first. All paths were cut at the same row, so their remainders have the
    /// same length and consumed count; each advances its own.
    if (destinations.front().path->remainder)
    {
        size_t remainder_rows = destinations.front().path->remainder->size() - destinations.front().path->remainder_consumed;
        size_t rows = std::min(rows_left, remainder_rows);
        for (auto & destination : destinations)
        {
            if (!destination.path->remainder)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object has no remainder", destination.path->name);
            insertRangeFromDecoded(*destination.column, *destination.path->remainder, destination.path->remainder_consumed, rows);
            destination.path->remainder_consumed += rows;
        }
        rows_left -= rows;
        if (rows == remainder_rows)
        {
            for (auto & destination : destinations)
            {
                destination.path->remainder.reset();
                destination.path->remainder_consumed = 0;
            }
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
            decodeGranuleIntoPathDestinations(entry, all_paths, seek_to, path_settings, destinations, rows_left);
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
        decodeGranuleIntoPathDestinations(entry, all_paths, seek_to, path_settings, destinations, rows_left);

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
        const auto & seek_to_current_mark = settings.seek_stream_to_current_mark_callback;

        seek_to_current_mark(marks_path);
        auto * marks_stream = getReadStream(Substream::ObjectTypedSharedDataPathsMarks, settings);
        auto entry = readGranuleEntry(*marks_stream, state.num_paths);

        seek_to_current_mark(data_path);
        auto * data_stream = getReadStream(Substream::ObjectTypedSharedDataPathsData, settings);
        auto path_settings = getPathDataDeserializeSettings(data_stream);
        if (entry.num_rows != 0)
            seek_to(entry.marks[destinations.front().path_index]);
        decodeGranuleIntoPathDestinations(entry, all_paths, seek_to, path_settings, destinations, rows_left);

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

void SerializationObjectTypedSharedData::deserializePathRegionsPrefix(
    DeserializeBinaryBulkSettings & settings,
    const DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
    size_t path_index,
    const ISerialization & path_serialization,
    DeserializeBinaryBulkStatePtr & path_state,
    SubstreamsDeserializeStatesCache * cache)
{
    auto region_settings = getPathRegionsDeserializeSettings(settings, shared_state, path_index);
    path_serialization.deserializeBinaryBulkStatePrefix(region_settings, path_state, cache);
}

void SerializationObjectTypedSharedData::enumeratePathRegionsStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
    size_t path_index,
    const SubstreamData & path_data)
{
    const size_t prefix_size = settings.path.size();
    const auto & path_state = shared_state.requested_paths.at(path_index);
    path_data.serialization->enumerateStreams(
        settings,
        [&](const SubstreamPath & substream_path)
        {
            if (auto region_path = getRegionPath(substream_path, prefix_size, *shared_state.header, path_index, path_state))
                callback(*region_path);
        },
        path_data);
}

void SerializationObjectTypedSharedData::deserializePathRegions(
    IColumn & column,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    const DeserializeBinaryBulkStateObjectTypedSharedData & shared_state,
    size_t path_index,
    const ISerialization & path_serialization,
    DeserializeBinaryBulkStatePtr & path_state,
    SubstreamsCache * cache)
{
    ProfileEvents::increment(ProfileEvents::ObjectTypedSharedDataRegionReads);
    auto region_settings = getPathRegionsDeserializeSettings(settings, shared_state, path_index);
    path_serialization.deserializeBinaryBulkWithMultipleStreams(column, limit, region_settings, path_state, cache);
}

void SerializationObjectTypedSharedData::deserializeBinaryBulkWithMultipleStreams(
    IColumn & column, size_t limit, DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsCache * cache) const
{
    /// The column is absent from the part: the prefix found no header, and the reader fills the defaults.
    if (!state)
        return;

    auto * shared_state = checkAndGetState<DeserializeBinaryBulkStateObjectTypedSharedData>(state);
    auto & typed_paths = assert_cast<ColumnObject &>(column).getTypedPaths();

    /// The caller has pushed ObjectData and ObjectTypedSharedData. Every path is read from the plain layout, whose first
    /// reader in a call decodes all of them, so either every path is in the cache or none is.
    if (insertPathFromCache(*typed_paths.at(paths.front().name), paths.front().name, settings, cache))
    {
        for (size_t path_index = 1; path_index != paths.size(); ++path_index)
        {
            if (!insertPathFromCache(*typed_paths.at(paths[path_index].name), paths[path_index].name, settings, cache))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Path {} of the typed shared data of Object is not in the substreams cache", paths[path_index].name);
        }
        return;
    }

    deserializePlainLayoutPaths(
        limit, settings, *shared_state, cache, [&](size_t path_index) -> IColumn * { return typed_paths.at(paths[path_index].name).get(); });
}

}
