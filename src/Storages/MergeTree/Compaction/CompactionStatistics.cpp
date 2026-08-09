#include <Interpreters/Context.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeProjectionPartsTask.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TTLDescription.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/BufferAllocationPolicy.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <IO/S3Defines.h>
#include <Core/Defines.h>
#include <Core/Settings.h>

#include <base/interpolate.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <unordered_set>

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsNonZeroUInt64 adaptive_write_buffer_initial_size;
    extern const MergeTreeSettingsBool allow_vertical_merges_from_compact_to_wide_parts;
    extern const MergeTreeSettingsUInt64 enable_vertical_merge_algorithm;
    extern const MergeTreeSettingsUInt64 max_merge_delayed_streams_for_parallel_write;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_bytes_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_columns_to_activate;
    extern const MergeTreeSettingsUInt64 vertical_merge_algorithm_min_rows_to_activate;
    extern const MergeTreeSettingsDeduplicateMergeProjectionMode deduplicate_merge_projection_mode;
    extern const MergeTreeSettingsBool enable_block_number_column;
    extern const MergeTreeSettingsBool enable_block_offset_column;
    extern const MergeTreeSettingsBool materialize_projections_on_merge;
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_max_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_min_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_compress_block_size;
    extern const MergeTreeSettingsUInt64 max_number_of_mutations_for_replica;
    extern const MergeTreeSettingsUInt64 min_columns_to_activate_adaptive_write_buffer;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_buckets_for_wide_part;
    extern const MergeTreeSettingsBool use_adaptive_write_buffer_for_dynamic_subcolumns;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_execute_mutation;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_lower_max_size_of_merge;
    extern const MergeTreeSettingsBool vertical_merge_optimize_ttl_delete;
}

namespace Setting
{
    extern const SettingsUInt64 max_compress_block_size;
    extern const SettingsUInt64 s3_max_single_part_upload_size;
    extern const SettingsUInt64 s3_min_upload_part_size;
    extern const SettingsUInt64 s3_max_upload_part_size;
    extern const SettingsUInt64 s3_strict_upload_part_size;
    extern const SettingsUInt64 s3_max_inflight_parts_for_one_file;
    extern const SettingsUInt64 azure_max_single_part_upload_size;
    extern const SettingsUInt64 azure_min_upload_part_size;
    extern const SettingsUInt64 azure_max_upload_part_size;
    extern const SettingsUInt64 azure_strict_upload_part_size;
    extern const SettingsUInt64 azure_max_inflight_parts_for_one_file;
}

/// Do not start to merge parts, if free space is less than sum size of parts times specified coefficient.
/// This value is chosen to not allow big merges to eat all free space. Thus allowing small merges to proceed.
constexpr static double DISK_USAGE_COEFFICIENT_TO_SELECT = 2;

/// To do merge, reserve amount of space equals to sum size of parts times specified coefficient.
/// Must be strictly less than DISK_USAGE_COEFFICIENT_TO_SELECT,
/// because between selecting parts to merge and doing merge, amount of free space could have decreased.
constexpr static double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

namespace CompactionStatistics
{

UInt64 estimateNeededDiskSpace(const MergeTreeDataPartsVector & source_parts, const bool & account_for_deleted)
{
    size_t bytes_size = 0;
    time_t current_time = std::time(nullptr);

    for (const MergeTreeData::DataPartPtr & part : source_parts)
    {
        /// Exclude expired parts
        time_t part_max_ttl = part->ttl_infos.part_max_ttl;
        if (part_max_ttl && part_max_ttl <= current_time)
            continue;

        if (account_for_deleted)
            bytes_size += part->getExistingBytesOnDisk();
        else
            bytes_size += part->getBytesOnDisk();
    }

    return static_cast<UInt64>(static_cast<double>(bytes_size) * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

namespace
{

/// The per-stream write buffer ceiling can be unbounded (MultipartUploadMemory::UNLIMITED, for a disk that
/// allows unlimited in-flight upload parts), so pricing every stream at it must saturate instead of wrapping
/// around to a tiny number. Such a worst case is only ever the upper half of a std::min with a
/// data-volume bound, which then governs the estimate.
UInt64 saturatingStreamsTimesBuffer(UInt64 streams, UInt64 buffer_size)
{
    UInt64 result = 0;
    if (__builtin_mul_overflow(streams, buffer_size, &result))
        return std::numeric_limits<UInt64>::max();
    return result;
}

/// Number of on-disk column streams for a set of columns: one per non-ephemeral serialization
/// substream. This mirrors how MergeTreeReaderWide / MergeTreeDataPartWriterWide open exactly one
/// IO buffer per substream (a plain column is one stream; Array/Nullable/Map/... add more).
/// Note: the default serialization does not know the dynamic substreams of JSON / Dynamic columns, so
/// this undercounts such columns; prefer countPartStreams below when an actual part is available.
size_t countColumnStreams(const NamesAndTypesList & columns)
{
    size_t streams = 0;
    for (const auto & column : columns)
    {
        auto serialization = column.type->getDefaultSerialization();
        serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
        {
            if (!ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                ++streams;
        }, column.type);
    }
    return streams;
}

/// A count of writer streams together with the share of them the writer opens WITHOUT an adaptive write
/// buffer. MergeTreeDataPartWriterWide::addStreams decides adaptivity PER STREAM: a stream is adaptive when
/// the writer's own columns list reaches min_columns_to_activate_adaptive_write_buffer (a per-writer, not
/// per-table, condition - the caller applies it, see estimateNeededMemoryForMerge) or when
/// use_adaptive_write_buffer_for_dynamic_subcolumns is on and the substream is dynamic
/// (ISerialization::isDynamicSubcolumn). An adaptive stream's compressor block and file buffer start at
/// adaptive_write_buffer_initial_size and only grow with the data written through them, while a non-adaptive
/// stream allocates both at the full max_compress_block_size up front, so pricing the two classes together
/// at the full size over-reserves a JSON / Dynamic merge - whose streams are almost all dynamic - by orders
/// of magnitude, the same starvation pattern the data-volume bounds exist to avoid.
struct WriterStreamCounts
{
    size_t total = 0;
    size_t non_adaptive = 0;
};

/// Streams of a column's default-serialization skeleton the writer does NOT treat as dynamic subcolumns:
/// ISerialization::isDynamicSubcolumn holds for every substream inside a Dynamic / Object subtree (any of
/// DynamicStructure / DynamicData / ObjectStructure / ObjectData on the path), so for a top-level JSON or
/// Dynamic column this is zero - even its structure and shared-data streams live inside that subtree - while
/// a composite keeps its non-dynamic skeleton (Array(JSON) offsets, the scalar elements of
/// Tuple(UInt64, JSON)) at the full write buffer.
size_t countNonAdaptiveColumnStreams(const NameAndTypePair & column)
{
    size_t streams = 0;
    auto serialization = column.type->getDefaultSerialization();
    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        if (!ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size())
            && !ISerialization::isDynamicSubcolumn(substream_path, substream_path.size()))
            ++streams;
    }, column.type);
    return streams;
}

/// The non-adaptive share of a column's counted streams, for a count produced by any of the per-column
/// arms below (a recorded-substream union, a recovered legacy layout, a capacity bound). A column without
/// dynamic structure cannot have dynamic substreams, so all of its streams are non-adaptive. For a column
/// WITH dynamic structure, everything counted beyond its static non-adaptive skeleton is a dynamic
/// substream (a dynamic path, a variant, shared data) that the writer opens adaptively - but a source part
/// may record a non-dynamic skeleton stream in its SPARSE form, which adds one extra recorded stream name
/// (the sparse offsets) next to the full one, and such a stream is NOT a dynamic subcolumn. Allow up to
/// twice the skeleton's non-adaptive count before classifying the remainder as adaptive: classifying a
/// sparse offsets stream as adaptive would under-price its eagerly allocated full-size buffers (the
/// admission-gate bug this estimate exists to close), while the doubled allowance can only over-price up to
/// one extra full-size buffer pair per non-dynamic substream - the safe direction, and exactly zero for the
/// common pure JSON / Dynamic column.
size_t nonAdaptiveStreamsShare(const NameAndTypePair & column, size_t column_streams)
{
    if (!column.type->hasDynamicStructure())
        return column_streams;
    return std::min(column_streams, 2 * countNonAdaptiveColumnStreams(column));
}

/// On-disk column data file (.bin) names a wide part physically stores: one per non-ephemeral substream.
/// This is the ground truth for the stream layout of an old part that predates columns_substreams.txt
/// and, unlike the default serialization, it accounts for the dynamic substreams of JSON / Dynamic
/// columns (which are written as separate files on disk but cannot be enumerated from the default
/// serialization).
std::unordered_set<std::string> collectWidePartDataFileNames(const IMergeTreeDataPart & part)
{
    std::unordered_set<std::string> data_files;
    for (const auto & [file_name, _] : part.checksums.files)
        if (file_name.ends_with(IMergeTreeDataPart::DATA_FILE_EXTENSION))
            data_files.insert(file_name);
    return data_files;
}

/// Whether a wide part's on-disk stream file (a .bin name) belongs to the given column. Every stream file is
/// named escapeForFileName(column) followed by '.', '%2E', or the file extension (the ColumnsSubstreams
/// substream-name invariant, see ColumnsSubstreams::findInvalidSubstreamName), so no column's escaped name is a
/// prefix of another column's files. Used to keep only the files of columns the merged part still writes and to
/// drop an old part's on-disk files for a column removed by a metadata-only ALTER DROP COLUMN.
bool streamFileBelongsToColumn(const std::string & file_name, const std::string & escaped_column_name)
{
    if (!file_name.starts_with(escaped_column_name))
        return false;
    const std::string_view rest(file_name.data() + escaped_column_name.size(), file_name.size() - escaped_column_name.size());
    return rest.starts_with('.') || rest.starts_with("%2E");
}

/// Number of on-disk column streams that a wide part actually reads/writes through. Prefer the exact
/// per-column substream layout recorded in columns_substreams.txt - the reliable source of truth for
/// types with a dynamic structure such as JSON and Dynamic, whose real substreams cannot be recovered
/// from the default serialization (this is exactly why MergeTreeDataPartWide::doCheckConsistency trusts
/// columns_substreams.txt over enumerateStreams). For an old wide part written before that file existed,
/// count the actual .bin files on disk, which is exact for dynamic columns too; only fall back to the
/// default serialization when neither source is available.
size_t countPartStreams(const IMergeTreeDataPart & part)
{
    const auto & columns_substreams = part.getColumnsSubstreams();
    if (!columns_substreams.empty())
        return columns_substreams.getTotalSubstreams();
    if (part.getType() == MergeTreeDataPartType::Wide)
        if (const size_t data_files = collectWidePartDataFileNames(part).size(); data_files != 0)
            return data_files;
    return countColumnStreams(part.getColumns());
}

/// Like countPartStreams, but restricted to the columns the merged part will actually write (those present in
/// output_columns). IMergeTreeDataPart::loadColumns keeps a source part's original columns.txt, so an old part
/// can still carry - in columns_substreams.txt, or as .bin files on disk - a JSON / Dynamic column that a
/// metadata-only ALTER DROP COLUMN removed from the current metadata. The merge writes only the current
/// metadata's columns, so such a dead column must not raise the output-stream floor and reserve memory for a
/// column that is never written.
size_t countPartStreamsForColumns(const IMergeTreeDataPart & part, const NamesAndTypesList & output_columns)
{
    const auto & columns_substreams = part.getColumnsSubstreams();
    if (!columns_substreams.empty())
    {
        size_t streams = 0;
        for (const auto & column : part.getColumns())
            if (output_columns.contains(column.name))
                if (const auto * substreams = columns_substreams.tryGetColumnSubstreams(column.name))
                    streams += substreams->size();
        return streams;
    }

    NamesAndTypesList written_columns;
    for (const auto & column : part.getColumns())
        if (output_columns.contains(column.name))
            written_columns.push_back(column);

    if (part.getType() == MergeTreeDataPartType::Wide)
    {
        std::vector<std::string> escaped_written_columns;
        escaped_written_columns.reserve(written_columns.size());
        for (const auto & column : written_columns)
            escaped_written_columns.push_back(escapeForFileName(column.name));

        size_t data_files = 0;
        for (const auto & file_name : collectWidePartDataFileNames(part))
            if (std::any_of(
                    escaped_written_columns.begin(), escaped_written_columns.end(),
                    [&](const auto & escaped) { return streamFileBelongsToColumn(file_name, escaped); }))
                ++data_files;
        if (data_files != 0)
            return data_files;
    }

    return countColumnStreams(written_columns);
}

/// On-disk bytes (compressed and uncompressed) of the data a merge actually reads from a source part:
/// the sizes of the part's columns that appear in read_columns. This is deliberately not the whole part:
/// a metadata-only ALTER DROP COLUMN leaves a dead column's files on disk (and in columns.txt) that the
/// merge never opens, and a parent part's bytes_on_disk / bytes_uncompressed_on_disk include its
/// projection parts, whose IO is priced separately (see projection_memory in
/// estimateNeededMemoryForMerge) - charging either would over-reserve on a table with dropped wide
/// JSON / Dynamic columns or large projections and needlessly serialize background merges. A compact
/// part does not track per-column sizes, and its single shared stream reads the whole data file anyway
/// (dead columns are interleaved with live ones), so its total columns size - the data file itself,
/// which likewise excludes projections - is the cap there.
ColumnSize partReadBytes(const IMergeTreeDataPart & part, const NamesAndTypesList & read_columns)
{
    if (part.getType() != MergeTreeDataPartType::Wide)
        return part.getTotalColumnsSize();

    ColumnSize read_size;
    for (const auto & column : part.getColumns())
        if (read_columns.contains(column.name))
            read_size.add(part.getColumnSize(column.name));
    return read_size;
}

/// The .bin file names the default serialization can enumerate for a set of columns - the static skeleton
/// already fully accounted for by countColumnStreams / the per-column union in countOutputStreams below.
/// Named exactly the way MergeTreeDataPartWriterWide names them (ISerialization::getFileNameForStream), so
/// they can be subtracted, name for name, from a legacy part's actual on-disk file set to recover exactly
/// the invisible dynamic substreams (see countOutputStreams).
std::unordered_set<std::string> collectStaticStreamFileNames(const NamesAndTypesList & columns, const ISerialization::StreamFileNameSettings & settings)
{
    std::unordered_set<std::string> names;
    for (const auto & column : columns)
    {
        auto serialization = column.type->getDefaultSerialization();
        serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
        {
            if (!ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                names.insert(ISerialization::getFileNameForStream(column, substream_path, settings));
        }, column.type);
    }
    return names;
}

/// Union, by name, of a single column's recorded substream names across the source parts that physically
/// store it (for JSON / Dynamic the merged dynamic structure is chosen from all source columns -
/// ColumnObject::chooseDynamicStructureForMerge / ColumnDynamic::chooseDynamicStructureForMerge - so paths
/// that appear in only some parts all end up in the result; a plain max over the parts would undercount that
/// case). Returns nullopt when no part records the column's substreams (parts written before
/// columns_substreams.txt existed, or a column absent from every given part) so the caller can decide the
/// fallback. The union is an upper bound on the real count - the merge may collapse some dynamic substreams
/// via max_dynamic_paths / max_dynamic_types - which is the safe direction for a reservation.
std::optional<size_t> tryCountColumnSubstreamsFromParts(const String & column_name, const MergeTreeDataPartsVector & source_parts)
{
    std::unordered_set<std::string_view> union_substreams;
    bool recorded = false;
    for (const auto & part : source_parts)
    {
        if (const auto * substreams = part->getColumnsSubstreams().tryGetColumnSubstreams(column_name))
        {
            recorded = true;
            for (const auto & substream : *substreams)
                union_substreams.insert(substream);
        }
    }

    if (!recorded)
        return std::nullopt;
    return union_substreams.size();
}

/// Upper bound on the on-disk streams one shared-data bucket of a JSON column serializes. The ADVANCED
/// shared-data serialization writes, per bucket, a structure (prefix) stream, the data / paths-marks /
/// substreams / substreams-marks / paths-substreams-metadata streams, and a structure-suffix stream (see
/// SerializationObjectSharedData::enumerateStreams); MAP / MAP_WITH_BUCKETS emit fewer. Kept as a small
/// worst-case constant so a rebuilt JSON projection is never priced below its real shared-data footprint.
constexpr size_t MAX_OBJECT_SHARED_DATA_STREAMS_PER_BUCKET = 7;

/// The ADVANCED shared-data serialization also writes three global copy streams once per JSON column, after
/// the per-bucket loop: ObjectSharedDataCopySizes, ObjectSharedDataCopyPathsIndexes, ObjectSharedDataCopyValues
/// (SerializationObjectSharedData::enumerateStreams). A rebuilt projection temp part inherits the parent
/// part's non-zero level (MergeTreeDataWriter::writeTempProjectionPart), so its shared data always takes this
/// ADVANCED path and these streams are present whatever max_dynamic_paths is.
constexpr size_t OBJECT_SHARED_DATA_GLOBAL_COPY_STREAMS = 3;

/// Data-independent streams a Dynamic column writes around its variants, whatever the data: the
/// DynamicStructure stream, and the variant discriminators prefix + discriminators streams
/// (SerializationDynamic::enumerateStreams -> SerializationVariant::enumerateStreams). The prefix stream only
/// exists with specialized prefix/suffix substreams, so this over-counts by one when it does not - a safe
/// direction for an upper bound.
constexpr size_t DYNAMIC_BASE_STREAMS = 3;

/// The streams a Dynamic node's default serialization already enumerates without a column present: exactly the
/// DynamicStructure stream (SerializationDynamic::enumerateStreams emits it unconditionally, then stops - it
/// has no column and so no variants to walk). countColumnStreams therefore already charges this one stream for
/// every Dynamic node reachable through the static type skeleton - a top-level Dynamic column, or one nested in
/// a Tuple / Array / Map / a JSON typed path - and every caller of countDynamicCapacityStreams adds
/// countColumnStreams({column}) on top of it. Subtracting it from a real Dynamic node's capacity keeps the
/// DynamicStructure stream from being reserved twice (on object storage a doubly counted stream is another full
/// multipart ceiling per column). This is not subtracted from a JSON node's hypothetical dynamic paths below:
/// the default serialization cannot enumerate those without a real column, so it never counts them.
constexpr size_t DYNAMIC_STREAMS_VISIBLE_TO_DEFAULT_SERIALIZATION = 1;

/// Worst-case number of on-disk streams a single materialized variant of a Dynamic value writes. A variant's
/// concrete type is runtime data, invisible in the declared Dynamic / JSON type, so it cannot be enumerated
/// statically: a scalar variant is one data stream, and the nested wrappers a value commonly carries -
/// Nullable, Array, Map, a small Tuple - add a null-map / offsets / element stream each. An arbitrarily wide
/// composite variant (for example CAST(tuple(<many columns>) AS Dynamic), or a wide tuple stored in a
/// Dynamic column) can write more than this and has no bound derivable from the declared type. This constant
/// backs the type-capacity fallback used wherever a column's real streams are not visible at selection time:
/// a rebuilt-projection column that is not a bare identifier of same-type base columns (priced at
/// max(capacity, the streams visible under its name in the source parts), see countRebuiltProjectionStreams),
/// a type-widened output column (priced at max(capacity, the streams visible in the source parts), see
/// countOutputStreams), and a dynamic-structure column of a compact source part that records no substreams
/// (a single data.bin, nothing per-column to recover from disk). On those paths a wide composite variant can therefore still be
/// under-estimated; that residual is irreducible without reading the parts' data, and it is covered by the
/// reservation being a soft throttle: MergeMemoryReservation::tryReserve always admits a single merge, so an
/// under-estimate only weakens the throttling of CONCURRENT merges - the oversubscription master allows for
/// every merge today - and never skips or fails a merge.
constexpr size_t STREAMS_PER_DYNAMIC_VARIANT = 4;

/// Worst-case number of on-disk streams the dynamic structure of a type materializes when a column of it is
/// written, bounded by the type's own write-time capacity limits - the caps ColumnDynamic / ColumnObject
/// enforce whatever the data looks like (concrete types beyond max_dynamic_types go to the shared variant,
/// paths beyond max_dynamic_paths go to the shared data). A Dynamic of max_dynamic_types can materialize that
/// many typed variants plus one shared variant; each of them is a value serialized by its own type's
/// serialization (SerializationVariant enumerates one stream group per variant), which can itself span more
/// than one stream, so a variant is priced at STREAMS_PER_DYNAMIC_VARIANT rather than a single stream. A JSON
/// column stores each of its up to max_dynamic_paths dynamic paths as a full Dynamic value of the JSON's own
/// max_dynamic_types (SerializationObject routes every dynamic path through SerializationDynamic), so a path
/// costs a whole Dynamic capacity, not a couple of streams. A JSON column also ALWAYS writes its shared-data
/// streams, whatever its data or max_dynamic_paths is (with max_dynamic_paths = 0 every path goes to the
/// shared data), and the default serialization cannot enumerate them without a real column, so they are added
/// here: up to object_shared_data_buckets_for_wide_part buckets (the wide-part count is never below the
/// compact-part one), each up to MAX_OBJECT_SHARED_DATA_STREAMS_PER_BUCKET streams, plus the
/// OBJECT_SHARED_DATA_GLOBAL_COPY_STREAMS the ADVANCED serialization writes once per column. Composite types
/// are walked recursively (forEachChild is a full descent), so Tuple(UInt64, JSON) or Array(Dynamic) are
/// priced by their nested semi-structured components. Zero for types without dynamic structure. Every caller
/// adds countColumnStreams({column}) on top of this, so a real Dynamic node's capacity here excludes the
/// DynamicStructure stream that the default serialization already enumerates (see
/// DYNAMIC_STREAMS_VISIBLE_TO_DEFAULT_SERIALIZATION), to avoid reserving that stream twice.
size_t countDynamicCapacityStreams(const IDataType & type, const MergeTreeSettings & settings)
{
    const UInt64 shared_data_buckets = settings[MergeTreeSetting::object_shared_data_buckets_for_wide_part];

    /// Worst-case streams a Dynamic value of at most max_dynamic_types variants writes.
    const auto dynamic_capacity = [](size_t max_dynamic_types) -> size_t
    {
        /// The base streams plus one worst-case variant footprint for each typed variant and the shared variant.
        return DYNAMIC_BASE_STREAMS + (max_dynamic_types + 1) * STREAMS_PER_DYNAMIC_VARIANT;
    };

    const auto node_capacity = [&](const IDataType & node) -> size_t
    {
        if (const auto * dynamic = typeid_cast<const DataTypeDynamic *>(&node))
            /// A real Dynamic node's DynamicStructure stream is already counted by the countColumnStreams the
            /// callers add on top, so exclude it here to avoid reserving it twice.
            return dynamic_capacity(dynamic->getMaxDynamicTypes()) - DYNAMIC_STREAMS_VISIBLE_TO_DEFAULT_SERIALIZATION;
        if (const auto * object = typeid_cast<const DataTypeObject *>(&node))
        {
            const size_t shared_data_streams
                = shared_data_buckets * MAX_OBJECT_SHARED_DATA_STREAMS_PER_BUCKET + OBJECT_SHARED_DATA_GLOBAL_COPY_STREAMS;
            return shared_data_streams + object->getMaxDynamicPaths() * dynamic_capacity(object->getMaxDynamicTypes());
        }
        return 0;
    };

    size_t streams = node_capacity(type);
    type.forEachChild([&](const IDataType & child) { streams += node_capacity(child); });
    return streams;
}

/// Union, by name, of a projection output column's recorded substreams across the source parts - but only
/// over the parts that store a column of the SAME type under this name, i.e. where the projection output is
/// a bare identifier of that base column (SELECT json ORDER BY ...), so its written substreams are exactly
/// that base column's. A projection that reuses a base column's NAME for a different type through an
/// expression - SELECT CAST(s, 'JSON') AS s over a String base s, SELECT CAST(v, 'Dynamic') AS v over a
/// UInt64 base v - is a synthesized column, not a bare identifier: its write footprint is that of the
/// target type, so it must NOT be priced from the base column's (String / UInt64) stream count. Returns
/// nullopt when no source part records this name with a matching type, so the caller falls back to
/// max(the type's own write-time capacity, the streams visible under this name in the source parts). Also
/// returns nullopt as soon as any same-name source part stores a
/// DIFFERENT type than the projection output (a capacity-changing ALTER: an old JSON part merged with a
/// newer JSON(val UInt32) one, or Dynamic parts of different max_types): the old part is reserialized under
/// the current metadata during the rebuild, but its dynamic paths are named for its own type and so are
/// invisible to the union over the same-type parts - the caller's fallback covers them safely.
///
/// A matching source part that records no substreams for the column (a pre-columns_substreams.txt upgrade
/// path) does not bail out to the capacity fallback when it is a WIDE part: its real per-column layout is
/// recoverable from disk - the same recovery countOutputStreams performs for base parts - and the static
/// type-capacity bound can be LOWER than that real layout (a wide composite variant the legacy part already
/// materialized exceeds the fixed per-variant worst case, see STREAMS_PER_DYNAMIC_VARIANT), so dropping to
/// capacity would under-reserve both the rebuilt temp-part writer and the read-back merge. The column's
/// dynamic .bin files (its on-disk files minus its static skeleton) are unioned by name across such parts
/// and added on top of the recorded union; when NO part records the column, the static skeleton the
/// recovery subtracted is charged once via countColumnStreams instead, keeping the result a full footprint.
/// Only a COMPACT part with no recorded substreams still returns nullopt: everything sits in one data.bin,
/// so there is physically nothing to recover, and the type-capacity fallback is the only sound bound.
///
/// The same bailout applies to a base column the merge materializes from its DEFAULT expression
/// (default_filled_dynamic_columns, see estimateNeededMemoryForMerge): after ALTER ... ADD COLUMN d JSON(...)
/// DEFAULT ..., parts that predate the ALTER do not store d at all, yet the projection rebuild runs on the
/// merged base rows AFTER IMergeTreeReader has filled and evaluated the missing defaults, so a rebuilt
/// SELECT d ... projection can write dynamic substreams that come only from the old, default-filled rows.
/// The recorded union over the parts that do store d cannot see those, so it must not be treated as exact.
std::optional<size_t> tryCountBareIdentifierProjectionSubstreams(
    const NameAndTypePair & column,
    const MergeTreeDataPartsVector & source_parts,
    const NameSet & default_filled_dynamic_columns,
    const MergeTreeSettings & settings)
{
    if (default_filled_dynamic_columns.contains(column.name))
        return std::nullopt;

    std::unordered_set<std::string_view> union_substreams;
    std::unordered_set<std::string> legacy_dynamic_files;
    bool recorded = false;
    for (const auto & part : source_parts)
    {
        const auto part_column = part->getColumns().tryGetByName(column.name);
        if (!part_column)
            continue;
        /// A same-name source part with a DIFFERENT type is still reserialized into this projection column
        /// under the current metadata during the rebuild - a supported path: a JSON part written before
        /// ALTER TABLE ... MODIFY COLUMN j JSON(val UInt32) merged with a newer hinted part, or Dynamic parts
        /// of different declared max_types. Its dynamic paths are invisible to the by-name union over the
        /// same-type parts (its recorded substreams are named for its own, different type), so trusting only
        /// the same-type parts would drop the old-part-only paths that writeTempProjectionPart still writes.
        /// Bail out to the type's write-time capacity, which no rebuilt column can exceed.
        if (!part_column->type->equals(*column.type))
            return std::nullopt;
        const auto * substreams = part->getColumnsSubstreams().tryGetColumnSubstreams(column.name);
        if (!substreams)
        {
            /// A legacy wide part predating columns_substreams.txt: its real per-column stream layout is on
            /// disk, and it can be wider than the static type capacity (a wide composite variant it already
            /// materialized), so recover the column's dynamic files instead of bailing out to capacity. The
            /// static skeleton is subtracted the same way countOutputStreams does, so the recorded union
            /// (which contains the skeleton) never double-counts it; the no-records case adds it back below.
            if (part->getType() != MergeTreeDataPartType::Wide)
                return std::nullopt;
            const ISerialization::StreamFileNameSettings stream_file_name_settings(settings);
            const auto static_files = collectStaticStreamFileNames({*part_column}, stream_file_name_settings);
            const auto escaped_column_name = escapeForFileName(column.name);
            for (const auto & file_name : collectWidePartDataFileNames(*part))
                if (!static_files.contains(file_name) && streamFileBelongsToColumn(file_name, escaped_column_name))
                    legacy_dynamic_files.insert(file_name);
            continue;
        }
        recorded = true;
        for (const auto & substream : *substreams)
            union_substreams.insert(substream);
    }

    if (!recorded && legacy_dynamic_files.empty())
        return std::nullopt;

    /// The recorded union carries the column's static skeleton; the recovered legacy files deliberately do
    /// not. When only legacy parts store the column, charge the skeleton once so the result stays a full
    /// per-column footprint (the rebuilt column writes its static streams too, not just the dynamic ones).
    const size_t recorded_or_skeleton_streams = recorded ? union_substreams.size() : countColumnStreams({column});
    return recorded_or_skeleton_streams + legacy_dynamic_files.size();
}

/// The streams demonstrably visible in the source parts for a rebuilt projection output column, whatever
/// the parts' declared types: the recorded-substreams union by name (type-agnostic - a part written before
/// a capacity-changing ALTER records its substreams under its own, different type, yet they are real
/// on-disk streams the rebuild reads back and reserializes) plus the real dynamic .bin files of same-name
/// wide parts that record no substreams (the pre-columns_substreams.txt upgrade path). This is the visible
/// arm of the max(capacity, visible) pricing countRebuiltProjectionStreams applies when
/// tryCountBareIdentifierProjectionSubstreams bails out: the static type capacity prices a variant at a
/// fixed worst case (STREAMS_PER_DYNAMIC_VARIANT), which a wide composite variant a source part already
/// materialized - under ANY declared type for this name - can exceed, so the visible streams are the
/// ground truth for exactly that case, the same way countOutputStreams prices a type-widened base column.
/// Like there, each arm is a FULL per-column footprint: the recorded union carries the source skeleton,
/// and when no part records the column the output column's own skeleton is charged instead
/// (tryCountColumnSubstreamsFromParts returns nullopt), so legacy dynamic files - whose recovery
/// subtracted the static skeleton - stack on a skeleton either way.
size_t countVisibleProjectionColumnStreams(
    const NameAndTypePair & column,
    const MergeTreeDataPartsVector & source_parts,
    const MergeTreeSettings & settings)
{
    size_t visible_streams = tryCountColumnSubstreamsFromParts(column.name, source_parts).value_or(countColumnStreams({column}));

    const ISerialization::StreamFileNameSettings stream_file_name_settings(settings);
    const auto escaped_column_name = escapeForFileName(column.name);
    std::unordered_set<std::string> legacy_dynamic_files;
    for (const auto & part : source_parts)
    {
        const auto part_column = part->getColumns().tryGetByName(column.name);
        if (!part_column)
            continue;
        if (part->getColumnsSubstreams().tryGetColumnSubstreams(column.name))
            continue;
        if (part->getType() != MergeTreeDataPartType::Wide)
            continue;
        /// Subtract the static skeleton of the part's OWN column definition - its on-disk files are named
        /// for the type the part was written with, not for the current projection output type.
        const auto static_files = collectStaticStreamFileNames({*part_column}, stream_file_name_settings);
        for (const auto & file_name : collectWidePartDataFileNames(*part))
            if (!static_files.contains(file_name) && streamFileBelongsToColumn(file_name, escaped_column_name))
                legacy_dynamic_files.insert(file_name);
    }
    return visible_streams + legacy_dynamic_files.size();
}

/// Number of on-disk streams the temporary part of a REBUILT projection writes. A rebuild recalculates the
/// projection from the merged base rows, so a semi-structured (JSON / Dynamic) projection column carries
/// real dynamic substreams, and writeTempProjectionPart writes one stream per substream. When the
/// projection output column is a bare identifier of a base column - the same name AND the same type - it is
/// priced precisely from that column's recorded substreams. But a projection may materialize a
/// semi-structured value through an expression, either under a name no base part records
/// (SELECT identity(json) ...) or under a base column's name but with a different type
/// (SELECT CAST(s, 'JSON') AS s, SELECT number::Dynamic AS d); its real substream count cannot be
/// enumerated from the default serialization (SerializationDynamic / SerializationObject without data stop
/// at the structure streams) nor traced to a base column by name - and the value does not have to be
/// derived from semi-structured input at all (number::Dynamic synthesizes variants from a plain column),
/// so no bound taken from the source parts' dynamic substreams is sound. Bound such a column by the LARGER
/// of its type's own write-time capacity (countDynamicCapacityStreams) and the streams visible under its
/// name in the source parts (countVisibleProjectionColumnStreams) - the same max(capacity, visible)
/// pricing countOutputStreams applies to a type-widened base column, and for the same reason: the fixed
/// per-variant capacity (STREAMS_PER_DYNAMIC_VARIANT) is not an upper bound for a wide composite variant a
/// source part already materialized, e.g. a Dynamic(max_types = 1) wide part holding a fat tuple variant
/// later widened by ALTER MODIFY COLUMN to Dynamic(max_types = 2) and rebuilt by a row-reducing merge -
/// the old part's streams are recorded (or recoverable from its .bin files) and must not be dropped just
/// because its declared type differs. For simple projection columns and bare identifiers of same-type base
/// columns all of this is a no-op.
WriterStreamCounts countRebuiltProjectionStreams(
    const NamesAndTypesList & projection_columns,
    const MergeTreeDataPartsVector & source_parts,
    const MergeTreeSettings & settings,
    const NameSet & default_filled_dynamic_columns)
{
    WriterStreamCounts counts;
    for (const auto & column : projection_columns)
    {
        size_t column_streams = 0;
        if (auto recorded = tryCountBareIdentifierProjectionSubstreams(column, source_parts, default_filled_dynamic_columns, settings))
            column_streams = *recorded;
        else
            column_streams = std::max(
                countColumnStreams({column}) + countDynamicCapacityStreams(*column.type, settings),
                countVisibleProjectionColumnStreams(column, source_parts, settings));
        counts.total += column_streams;
        counts.non_adaptive += nonAdaptiveStreamsShare(column, column_streams);
    }
    return counts;
}

/// Number of on-disk column streams the merged wide part will write. Its substream set is estimated per output
/// column as the union, by name, of the source parts' recorded substreams (tryCountColumnSubstreamsFromParts),
/// falling back to the default serialization for a column no part records. A column that some source part
/// stores under a DIFFERENT type than the merged metadata cannot be priced from that union: a metadata-only
/// ALTER MODIFY COLUMN widening (plain JSON / Dynamic(max_types=0) later widened, then merged with newer parts
/// of the wider type - 03918_json_lazy_type_hints_merge) reserializes the old rows under the current, wider
/// type on merge, but the old part's recorded substreams are named for its own, narrower type, so the union
/// undercounts what the merged part writes. Such a widened column is bounded by the LARGER of the output
/// type's own write-time capacity (countDynamicCapacityStreams, the same conservative bound the
/// compact-source recovery below and a rebuilt projection use) and the streams actually visible in the source
/// parts for that name - the recorded substream union plus the real dynamic .bin files of unrecorded legacy
/// wide parts. The capacity constant prices a variant at a fixed worst case, which a wide composite variant
/// (a fat tuple inside Dynamic) already materialized in a source part can exceed; the visible streams are the
/// ground truth for exactly that case, so the max never prices a widened column below what its sources
/// demonstrably wrote. What stays invisible - a wide variant hidden in a shared-variant stream or inside a
/// compact part's data.bin - is the irreducible residual documented on STREAMS_PER_DYNAMIC_VARIANT.
///
/// The per-column union can still miss dynamic substreams that live only in a source part without
/// columns_substreams.txt (there the default serialization collapses JSON / Dynamic to a single stream, and
/// tryGetColumnSubstreams returns nothing): an old wide part written before that file existed, or a compact part
/// written with write_marks_for_substreams_in_compact_parts = 0. Those parts' dynamic streams are added back
/// explicitly below - by name for a wide part (its .bin files are the ground truth), by the output column type's
/// write-time capacity for a compact part (which stores every column in a single data.bin, so its per-column
/// stream layout is not recoverable from disk) - so a mixed old/new merge with disjoint dynamic paths is not
/// undercounted. The result is also floored at the widest source part's actual stream count
/// (countPartStreamsForColumns reads an old wide part's real .bin count), since the merged part is never
/// narrower than any single source part.
///
/// A column with dynamic structure that some source part will materialize from its DEFAULT expression
/// (default_filled_dynamic_columns: absent from a source part after ALTER ... ADD COLUMN ... DEFAULT ...,
/// see estimateNeededMemoryForMerge) is priced the same way as a widened column: MergeTask keeps such a
/// column live (it is not expired while a default exists) and IMergeTreeReader fills and evaluates the
/// default for the rows of the parts that predate the ALTER, so the merged part can write dynamic
/// (JSON / Dynamic) substreams that come only from the default-filled rows - substreams no source part
/// records, invisible to the per-column union and to the legacy .bin recovery alike.
///
/// Throughout, a source column absent from output_columns (removed by a metadata-only ALTER DROP COLUMN but
/// still carried on an old part's disk / columns.txt) is ignored - the merge never writes it, so it must not
/// inflate the estimate. For simple columns and modern parts of the current type all adjustments are no-ops.
WriterStreamCounts countOutputStreams(
    const NamesAndTypesList & output_columns,
    const MergeTreeDataPartsVector & source_parts,
    const MergeTreeSettings & settings,
    const NameSet & default_filled_dynamic_columns)
{
    /// Per-column union of the source parts' recorded substreams, matched by name, except for the columns
    /// whose real output substreams the union cannot see: a column any source part stores under a different
    /// type than the merged metadata - a metadata-only widening whose old rows are reserialized under the
    /// current, wider type - and a dynamic-structure column some source part default-fills. Those columns are
    /// remembered and priced after the legacy-part recovery below, once the streams visible for them in the
    /// source parts (recorded substreams and real .bin files alike) are known, so neither the recovery nor
    /// the floor prices them again. Alongside the count, track the share of the streams the writer opens
    /// without an adaptive write buffer (see nonAdaptiveStreamsShare) - each per-column arm contributes its
    /// own share, and the purely dynamic aggregates (recovered dynamic files, compact capacity) contribute
    /// none.
    size_t streams = 0;
    size_t non_adaptive_streams = 0;
    std::unordered_set<std::string_view> capacity_priced_columns;
    for (const auto & column : output_columns)
    {
        const bool type_widened = std::any_of(
            source_parts.begin(), source_parts.end(),
            [&](const auto & part)
            {
                const auto part_column = part->getColumns().tryGetByName(column.name);
                return part_column && !part_column->type->equals(*column.type);
            });

        if (type_widened || default_filled_dynamic_columns.contains(column.name))
        {
            capacity_priced_columns.insert(column.name);
        }
        else
        {
            const size_t column_streams
                = tryCountColumnSubstreamsFromParts(column.name, source_parts).value_or(countColumnStreams({column}));
            streams += column_streams;
            non_adaptive_streams += nonAdaptiveStreamsShare(column, column_streams);
        }
    }

    /// The per-column union above can only see substreams recorded in columns_substreams.txt. A source part
    /// written without that file records nothing, so the dynamic substreams of its JSON / Dynamic columns are
    /// invisible to the union. When such a part is merged with newer parts, its dynamic paths can be disjoint
    /// from theirs (old part has path 'a', new part has 'b', and the merged part writes both), so the union -
    /// which only saw the newer part's 'b' - undercounts the result. The whole-part max floor below does not
    /// close this: neither part is on its own as wide as their union.
    ///
    /// A WIDE part's real layout is recoverable from disk: its actual .bin file names minus the names
    /// accountable to its columns' static skeleton (collectStaticStreamFileNames; already covered by the union
    /// above) are exactly its unrecorded dynamic files. UNION them across parts, by name, rather than summing
    /// their counts - two old parts that both physically store the same dynamic file (e.g. the same JSON path
    /// resolved to the same type) name it identically (ISerialization::getFileNameForStream depends only on
    /// column, path and resolved type, not on which part wrote it), and the merged part writes that stream only
    /// once; summing per-part counts would charge it once per part instead. Treating genuinely distinct dynamic
    /// files as disjoint from every other part is still the safe direction for a reservation.
    ///
    /// A COMPACT part stores every column in a single data.bin, so it has no per-column .bin files to recover
    /// from and (when it also records no substreams, i.e. write_marks_for_substreams_in_compact_parts = 0 or an
    /// older .mrk3 part) collapses its JSON / Dynamic columns to the default one-stream count. Bound each of its
    /// dynamic-structure columns by the write-time capacity of the OUTPUT (merged, current-metadata) column
    /// instead (countDynamicCapacityStreams, the same conservative bound a rebuilt projection uses for a
    /// synthesized semi-structured column), which no merged column can exceed. Using the output type, not the
    /// source part's own type, matters after a capacity-changing ALTER (an old Dynamic(max_types=0) part merged
    /// into a Dynamic(max_types=5) column writes the wider output type's streams). Add each output column once -
    /// the capacity is the merged column's hard limit, not a per-part quantity - so many similar compact parts
    /// do not multiply it.
    ///
    /// For parts written after columns_substreams.txt exists, and for merges of only simple columns, all of
    /// this adds nothing.
    const ISerialization::StreamFileNameSettings stream_file_name_settings(settings);
    std::unordered_set<std::string> unrecorded_dynamic_files;
    std::unordered_set<std::string> unrecorded_non_adaptive_files;
    std::unordered_map<std::string, std::unordered_set<std::string>> capacity_priced_dynamic_files;
    std::unordered_set<std::string> compact_recovered_columns;
    size_t compact_dynamic_streams = 0;
    for (const auto & part : source_parts)
    {
        /// A part that records its substreams is already fully accounted for by the per-column union above.
        if (!part->getColumnsSubstreams().empty())
            continue;

        /// Only types with a dynamic structure (JSON, Dynamic, and composites containing them) have
        /// substreams that the default serialization cannot enumerate, so only such parts need the recovery
        /// at all. hasDynamicSubcolumns() would be too broad as the gate: a plain Map or Variant reports true
        /// there, yet its physical streams are fully enumerable from the default serialization.
        const auto & part_columns = part->getColumns();
        const bool has_dynamic_structure_column = std::any_of(
            part_columns.begin(), part_columns.end(),
            [](const auto & column) { return column.type->hasDynamicStructure(); });
        if (!has_dynamic_structure_column)
            continue;

        if (part->getType() == MergeTreeDataPartType::Wide)
        {
            /// Subtract every file name the default serialization can enumerate from the part's actual on-disk
            /// file names. For a column without dynamic structure that is all of its files; for a column with
            /// dynamic structure it is the static skeleton of its layout - and composites keep a real one:
            /// Tuple(UInt64, JSON) still has the UInt64 element stream, Array(JSON) its offsets, JSON its
            /// shared-data streams. Both kinds are already counted once by the per-column union above, so
            /// subtracting only whole non-dynamic columns (as if a dynamic-structure column had no enumerable
            /// streams at all) would count that static skeleton twice and over-reserve upgrade-path merges.
            /// What remains after the subtraction is exactly the part's dynamic files - but only recover those
            /// of columns the merged part still writes: a column dropped by a metadata-only ALTER (absent from
            /// output_columns) is not written and must be excluded. A capacity-priced (widened / default-filled)
            /// column's dynamic files are collected separately, per column, and folded into the
            /// max(capacity, visible streams) pricing of such columns below rather than into the shared union,
            /// so they are not priced twice.
            const auto static_files = collectStaticStreamFileNames(part_columns, stream_file_name_settings);
            /// The recovered file's adaptivity class follows its owning column: a file of a column with
            /// dynamic structure that survived the skeleton subtraction is a dynamic substream the writer
            /// opens adaptively, while a file of a plain column that survived it is a non-dynamic extra (a
            /// sparse offsets stream the full skeleton does not name) that keeps the full write buffer.
            std::vector<std::pair<std::string, bool>> recoverable_escaped_columns;
            std::vector<std::pair<std::string, std::string>> capacity_priced_escaped_columns;
            for (const auto & column : part_columns)
            {
                if (!output_columns.contains(column.name))
                    continue;
                if (capacity_priced_columns.contains(column.name))
                    capacity_priced_escaped_columns.emplace_back(escapeForFileName(column.name), column.name);
                else
                    recoverable_escaped_columns.emplace_back(escapeForFileName(column.name), column.type->hasDynamicStructure());
            }

            for (const auto & file_name : collectWidePartDataFileNames(*part))
            {
                if (static_files.contains(file_name))
                    continue;
                const auto recoverable = std::find_if(
                    recoverable_escaped_columns.begin(), recoverable_escaped_columns.end(),
                    [&](const auto & escaped) { return streamFileBelongsToColumn(file_name, escaped.first); });
                if (recoverable != recoverable_escaped_columns.end())
                {
                    if (unrecorded_dynamic_files.insert(file_name).second && !recoverable->second)
                        unrecorded_non_adaptive_files.insert(file_name);
                    continue;
                }
                for (const auto & [escaped, name] : capacity_priced_escaped_columns)
                {
                    if (streamFileBelongsToColumn(file_name, escaped))
                    {
                        capacity_priced_dynamic_files[name].insert(file_name);
                        break;
                    }
                }
            }
        }
        else
        {
            /// A compact part cannot expose its per-column dynamic files; bound each of its dynamic-structure
            /// columns by the write-time capacity of the OUTPUT (merged, current-metadata) column, once per
            /// output column. The merged wide part reserializes the column under the current metadata, which a
            /// capacity-changing ALTER can make wider than the source part's own type - an old
            /// Dynamic(max_types=0) or unhinted JSON part merged into a Dynamic(max_types=5) / JSON(...) column
            /// writes the output type's streams, not the smaller source type's - so pricing the source type
            /// would under-reserve. A column absent from the merged part (dropped by ALTER) is not written and
            /// is skipped, and a capacity-priced (widened / default-filled) column is priced at no less than
            /// its output-type capacity below.
            for (const auto & column : part_columns)
            {
                if (!column.type->hasDynamicStructure())
                    continue;
                if (capacity_priced_columns.contains(column.name))
                    continue;
                const auto output_column = output_columns.tryGetByName(column.name);
                if (output_column && compact_recovered_columns.insert(column.name).second)
                    compact_dynamic_streams += countDynamicCapacityStreams(*output_column->type, settings);
            }
        }
    }
    /// The recovered dynamic files and the compact write-time capacity model streams inside Dynamic / Object
    /// subtrees, which the writer opens adaptively; only the sparse extras of plain columns keep the full
    /// write buffer.
    streams += unrecorded_dynamic_files.size() + compact_dynamic_streams;
    non_adaptive_streams += unrecorded_non_adaptive_files.size();

    /// Price the capacity-priced (widened / default-filled) columns now that every stream visible for them in
    /// the source parts is known: the recorded substream union by name (tryCountColumnSubstreamsFromParts does
    /// not require type equality, so it sees the old, narrower type's recorded substreams too) plus the real
    /// dynamic .bin files recovered above from unrecorded legacy wide parts. Each arm of the max is a FULL
    /// per-column footprint: the output type's static skeleton plus its write-time dynamic capacity, or the
    /// streams the source parts demonstrably wrote (the recorded union already contains the source skeleton, so
    /// the skeleton must not be added on top of it - that would double-count every statically enumerable stream
    /// and price a metadata-only scalar widen such as Enum value addition at two streams instead of one). When
    /// NO source part records the column - only pre-columns_substreams.txt legacy parts store it - the visible
    /// arm has no recorded union to carry the skeleton, and the file recovery above deliberately subtracted the
    /// static skeleton names (they must not be double-counted where a recorded union exists); the column's
    /// static skeleton is charged once here instead, so the visible arm stays a full footprint on the pure
    /// legacy-wide upgrade path rather than just the dynamic remainder. The
    /// capacity arm covers what the sources cannot show (paths and variants the wider output type materializes
    /// beyond what the narrower source type could record, and the substreams a DEFAULT expression materializes
    /// for the rows of parts that predate an ADD COLUMN, which no source part records at all); the visible arm
    /// is the ground truth for a wide composite variant a source part already materialized, which the fixed
    /// per-variant capacity (STREAMS_PER_DYNAMIC_VARIANT) can exceed. Taking the max of the two footprints
    /// never prices such a column below either bound and stays proportional to real data, so it cannot
    /// re-introduce the saturating over-reservation.
    for (const auto & column : output_columns)
    {
        if (!capacity_priced_columns.contains(column.name))
            continue;
        size_t visible_streams
            = tryCountColumnSubstreamsFromParts(column.name, source_parts).value_or(countColumnStreams({column}));
        if (const auto it = capacity_priced_dynamic_files.find(column.name); it != capacity_priced_dynamic_files.end())
            visible_streams += it->second.size();
        const size_t column_streams
            = std::max(countColumnStreams({column}) + countDynamicCapacityStreams(*column.type, settings), visible_streams);
        streams += column_streams;
        non_adaptive_streams += nonAdaptiveStreamsShare(column, column_streams);
    }

    /// The merged wide part is never narrower than any single source part, so floor the estimate at the
    /// widest source part's actual stream count - but counting only the columns the merged part still writes,
    /// so a column an old part carries yet the current metadata dropped does not inflate the floor. For simple
    /// columns and modern parts this floor equals the per-column union, so it never raises the estimate above
    /// what the union already accounts for.
    size_t max_source_streams = 0;
    for (const auto & part : source_parts)
        max_source_streams = std::max(max_source_streams, countPartStreamsForColumns(*part, output_columns));

    /// When the floor wins, the excess streams cannot be attributed to a column, so classify them as
    /// non-adaptive - the direction that can only over-price, and only on the legacy upgrade path where the
    /// widest source part exceeds the per-column accounting above.
    if (max_source_streams > streams)
    {
        non_adaptive_streams += max_source_streams - streams;
        streams = max_source_streams;
    }

    return {.total = streams, .non_adaptive = non_adaptive_streams};
}

/// The rows a column has to be SYNTHESIZED for: only the rows of the source parts that do not physically
/// store it. The rows of the parts that do store the column are read by the merge, so the bytes written for
/// them are already covered by the input-volume bound; charging the whole merge's row count for such a
/// column would count those rows twice and almost double the writer-side bound on the upgrade path (one tiny
/// pre-ALTER part merged with many large post-ALTER ones), rejecting the merge for the very starvation
/// reason this estimate exists.
UInt64 countRowsMissingColumn(const MergeTreeDataPartsVector & parts, const String & column_name)
{
    UInt64 missing_rows = 0;
    for (const auto & part : parts)
        if (!part->tryGetColumn(column_name))
            missing_rows += part->rows_count;
    return missing_rows;
}

}

UInt64 estimateNeededMemoryForMerge(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const MergeTreeSettings & settings,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    time_t time_of_merge,
    bool output_on_remote_disk,
    std::optional<DiskWriteBufferMemory> remote_write_buffer_memory,
    bool deduplicate,
    bool cleanup)
{
    /// Per-stream read buffer size, from the effective server settings (merges read through the global
    /// context). A read buffer is later shrunk to the granule size, and it is smaller for the local
    /// filesystem than for remote (object storage).
    const auto read_settings = context->getReadSettings();
    const UInt64 local_read_buffer_size = read_settings.local_fs_settings.buffer_size;

    /// On a cache-backed object storage disk the reader does NOT use remote_fs_settings.buffer_size as it
    /// stands: to avoid cache fragmentation DiskObjectStorage::prepareReadPipeline promotes it to
    /// max(buffer_size, large_buffer_size = prefetch_buffer_size) whenever the filesystem cache is active
    /// for that disk, and the merge readers get that promoted size. Mirror the same rule, so a raised
    /// prefetch_buffer_size is not under-reserved. Whether a disk really has a cache is known per part
    /// (getCacheName), so the promotion is applied per part below; with default settings
    /// prefetch_buffer_size == max_read_buffer_size, so the promoted size equals the plain one and nothing
    /// changes.
    const UInt64 remote_read_buffer_size = read_settings.remote_fs_settings.buffer_size;
    const bool cache_promotes_read_buffer = read_settings.enable_filesystem_cache
        && read_settings.filesystem_cache_settings.prefer_bigger_buffer_size
        && !read_settings.filesystem_cache_settings.read_if_exists_otherwise_bypass;
    const UInt64 cached_remote_read_buffer_size = cache_promotes_read_buffer
        ? std::max<UInt64>(remote_read_buffer_size, read_settings.remote_fs_settings.large_buffer_size)
        : remote_read_buffer_size;

    /// The size a single reader of this part costs: local parts read through the local buffer, remote parts
    /// through the remote one, promoted when the part's disk is cache-backed (see above).
    auto part_read_buffer_size = [&](const auto & part)
    {
        if (!part->isStoredOnRemoteDisk())
            return local_read_buffer_size;
        return part->getDataPartStorage().getCacheName() ? cached_remote_read_buffer_size : remote_read_buffer_size;
    };

    /// Per-stream write buffer size on multipart object storage (S3 / Azure). A stream's upload buffers
    /// follow the multipart buffer allocation policy of its writer (see BufferAllocationPolicy /
    /// WriteBufferFromS3 / WriteBufferFromAzureBlobStorage), and getMultipartUploadMemory derives both the
    /// first buffer and the ceiling from exactly the settings the writer uses: with the default exponential
    /// policy the first buffer is max(*_max_single_part_upload_size, *_min_upload_part_size) and later
    /// buffers grow up to *_max_upload_part_size, with *_strict_upload_part_size set every buffer is that
    /// size, and up to *_max_inflight_parts_for_one_file of them are held in memory at once while their
    /// uploads are in flight - unbounded when that setting is zero, in which case the ceiling is
    /// MultipartUploadMemory::UNLIMITED and only the data-volume bound below constrains the estimate.
    ///
    /// Prefer the actual destination disk's sizing (remote_write_buffer_memory from
    /// getDiskWriteBufferMemory) when the caller knows the disk: a background merge's
    /// object-storage writer takes its multipart sizes from the disk's own request settings and ignores the
    /// query/session settings (see S3ObjectStorage::writeObject), so a disk config that raises the multipart
    /// sizes is reflected here and cannot allocate more per stream than is reserved. A known disk with a
    /// zero ceiling has no multipart upload buffers at all - a remote disk such as HDFS writes through a
    /// normal buffer (see HDFSObjectStorage::writeObject) - so the local per-stream estimate applies there;
    /// treating every remote disk as an S3 / Azure multipart writer would reserve, per output stream,
    /// gigabytes a HDFS merge can never allocate, and a many-column table would saturate
    /// merges_mutations_memory_usage_soft_limit and starve merges for no real memory pressure. Only when
    /// the disk is not yet known (the admission guess before CurrentlyMergingPartsTagger picks it) fall
    /// back to the worst-case per-stream ceiling from the context settings over both back ends (a given
    /// disk is only one of them, so the max is a safe upper bound); pinning the first buffer to
    /// *_max_single_part_upload_size alone would underestimate it when *_min_upload_part_size is the larger
    /// of the two. This is only a ceiling: the output side is separately capped by the merge's data volume
    /// below, because an upload buffer never holds more than the data written into it. Not even the FIRST
    /// upload buffer is allocated up front. WriteBufferFromS3 / WriteBufferFromAzureBlobStorage are
    /// constructed with the buffer size their caller passes - S3ObjectStorage::writeObject hands them the
    /// stream's own buf_size (max_compress_block_size, or adaptive_write_buffer_initial_size for an adaptive
    /// stream) - keep that BufferWithOwnMemory allocation, and only grow it toward the allocation policy's
    /// first-part size once it actually fills, doubling as it goes
    /// (WriteBufferFromS3::reallocateFirstBuffer, reached from nextImpl on the available() == 0 path; the
    /// same shape on Azure). So the memory a stream pins regardless of volume is its own initial buffer,
    /// and everything above that is data that has already flowed through it - which is why a stream whose
    /// data volume this estimate cannot derive from the source parts is priced at the writer's initial
    /// buffers (see the unknowable-volume pricing below), not at any multipart size.
    UInt64 remote_write_buffer_size = 0;
    if (remote_write_buffer_memory.has_value())
        remote_write_buffer_size = remote_write_buffer_memory->ceiling;
    else if (output_on_remote_disk)
    {
        /// Model both back ends exactly as their writers do (getMultipartUploadMemory over the same
        /// allocation settings, so a strict_upload_part_size configuration and an unlimited
        /// *_max_inflight_parts_for_one_file are accounted for), and take the larger of the two: the disk
        /// this merge will write to is one of them, so the maximum is a safe upper bound. The ceiling is
        /// additionally never assumed smaller than the S3 default first part, because a disk config that
        /// this pre-selection guess cannot see may raise the sizes back to the defaults.
        const auto & query_settings = context->getSettingsRef();

        BufferAllocationPolicy::Settings s3_allocation;
        s3_allocation.strict_size = query_settings[Setting::s3_strict_upload_part_size];
        s3_allocation.min_size = query_settings[Setting::s3_min_upload_part_size];
        s3_allocation.max_size = query_settings[Setting::s3_max_upload_part_size];
        s3_allocation.max_single_size = query_settings[Setting::s3_max_single_part_upload_size];
        const auto s3_memory
            = getMultipartUploadMemory(s3_allocation, query_settings[Setting::s3_max_inflight_parts_for_one_file]);

        BufferAllocationPolicy::Settings azure_allocation;
        azure_allocation.strict_size = query_settings[Setting::azure_strict_upload_part_size];
        azure_allocation.min_size = query_settings[Setting::azure_min_upload_part_size];
        azure_allocation.max_size = query_settings[Setting::azure_max_upload_part_size];
        azure_allocation.max_single_size = query_settings[Setting::azure_max_single_part_upload_size];
        const auto azure_memory
            = getMultipartUploadMemory(azure_allocation, query_settings[Setting::azure_max_inflight_parts_for_one_file]);

        remote_write_buffer_size = std::max<UInt64>(
            {S3::DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE, s3_memory.ceiling, azure_memory.ceiling});
    }

    /// A merge that applies patch parts (lightweight updates, apply_patches_on_merge) opens a separate
    /// reader for every patch part alongside the base parts' readers (MergeTreeReadTask::createReaders
    /// builds new_readers.patches, one MergeTreeReader per entry of future_part.patch_parts), and the
    /// patched columns it writes are read from patches just as much as from the base parts. A patch part
    /// is a regular IMergeTreeDataPart - it only physically stores the columns it patches, so counting its
    /// own on-disk streams and bytes like a base part is exact, not an overestimate. Fold patch parts into
    /// the same source-parts accounting used below for input memory and output substream estimation, so
    /// patch-only JSON / Dynamic substreams and patch bytes are not silently dropped from the reservation.
    MergeTreeData::DataPartsVector source_and_patch_parts = future_part.parts;
    source_and_patch_parts.insert(source_and_patch_parts.end(), future_part.patch_parts.begin(), future_part.patch_parts.end());

    /// The merge reads and writes the current metadata's physical columns, not everything a source part
    /// stores: a column removed by a metadata-only ALTER DROP COLUMN still has its files on disk (and in
    /// columns.txt), but MergeTask never opens a reader for it. The lightweight-delete mask (_row_exists)
    /// is not a metadata column, yet it is read from every part that stores it, so it joins the read set.
    const auto & columns_description = metadata_snapshot->getColumns();
    NamesAndTypesList output_columns = columns_description.getAllPhysical();

    /// A storage column absent from every source part and without a default expression is expired:
    /// MergeTask marks it so (new_data_part->expired_columns) and erases it from the storage / merging
    /// columns, so the merge neither reads nor writes it - after a metadata-only ALTER ... ADD COLUMN with
    /// no default the next merge does not open a writer for that column at all. Price the output side
    /// without such columns, or a table that accumulated late-added semi-structured columns would reserve
    /// writer streams (and raise the adaptive-write-buffer threshold) for columns the merge never writes
    /// and saturate merges_mutations_memory_usage_soft_limit for no real memory pressure. Mirror
    /// MergeTask's expiry decision exactly, all three presence sources:
    ///  - the base parts' own columns;
    ///  - the patch parts' columns: a column added by ADD COLUMN and then filled by a lightweight UPDATE
    ///    is physically absent from every base part, yet its only live values sit in the patch parts -
    ///    MergeTask keeps it and writes it, so it must stay priced;
    ///  - the targets of a pending, not yet materialized RENAME COLUMN old TO new: the rename is applied
    ///    on-fly at read time, so the merged metadata carries `new` while the source parts still
    ///    physically store `old` - the merge keeps `new` alive (reading `old` through AlterConversions)
    ///    whenever a base or patch part holds `old` and `old` is not itself a live storage column (then
    ///    the physical data belongs to `new` only after the rename materializes and MergeTask falls back
    ///    to expiring `new`). The mapping is remembered (pending_rename_sources) and every later helper
    ///    that probes the source parts for a column probes them under the source part's own name for it
    ///    (see part_source_name below), so the read/write buffers of a rename target are not silently
    ///    dropped from the reservation while the real merge reads and rewrites the column.
    /// The full expired set (before the required-columns exemption below) also decides which projections
    /// the merge rebuilds, so it is computed once here and reused by the projection pricing below.
    NameSet expired_columns;
    std::unordered_map<String, String> pending_rename_sources;
    {
        NameSet columns_present_in_parts;
        for (const auto & part : future_part.parts)
            for (const auto & part_column : part->getColumns())
                columns_present_in_parts.insert(part_column.name);

        NameSet columns_present_in_patch_parts;
        for (const auto & part : future_part.patch_parts)
            for (const auto & part_column : part->getColumns())
                columns_present_in_patch_parts.insert(part_column.name);

        if (mutations_snapshot)
        {
            const auto storage_column_names = output_columns.getNameSet();
            for (const auto & part : future_part.parts)
            {
                const auto conversions = MergeTreeData::getAlterConversionsForPart(part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
                    , nullptr
#endif
                    );
                for (const auto & rename : conversions->getRenameMap())
                {
                    if ((columns_present_in_parts.contains(rename.rename_from)
                         || columns_present_in_patch_parts.contains(rename.rename_from))
                        && !storage_column_names.contains(rename.rename_from))
                        pending_rename_sources.emplace(rename.rename_to, rename.rename_from);
                }
            }
        }

        for (const auto & storage_column : output_columns)
            if (!columns_present_in_parts.contains(storage_column.name)
                && !columns_present_in_patch_parts.contains(storage_column.name)
                && !pending_rename_sources.contains(storage_column.name)
                && !columns_description.getDefault(storage_column.name))
                expired_columns.insert(storage_column.name);
    }

    /// The source-part view of a column name: a pending rename target is stored in the source parts under
    /// its old name, so every probe of a source part for it - stream counts, recorded substreams, column
    /// sizes, missing-rows accounting - goes through this mapping. The merge predicates only select parts
    /// with equal pending-mutation versions into one merge, so one mapping fits every source part (a part
    /// that already materialized the rename has an empty rename map and would never be merged with one
    /// that has not). Renames do not change the column's type, so a translated probe hits the source
    /// column with exactly the output column's type, and every type-sensitive path behaves as if the
    /// column had never been renamed. With no pending renames both are the identity.
    const auto part_source_name = [&](const String & name) -> const String &
    {
        const auto it = pending_rename_sources.find(name);
        return it == pending_rename_sources.end() ? name : it->second;
    };
    const auto part_source_view = [&](const NamesAndTypesList & columns)
    {
        if (pending_rename_sources.empty())
            return columns;
        NamesAndTypesList result;
        for (const auto & column : columns)
            result.emplace_back(part_source_name(column.name), column.type);
        return result;
    };

    /// MergeTask keeps an expired column its merge semantics still require (merge_required_columns, see
    /// extractMergingAndGatheringColumns) - such a column IS written, filled with the type's default
    /// values - so keep a conservative over-approximation of that required set priced. The exact set
    /// depends on state known only at merge time (the DEDUPLICATE BY column list, whether the minmax index
    /// is recalculated), and over-keeping a column can only over-reserve a rare corner (a required column
    /// absent from every source part), never under-reserve.
    if (!expired_columns.empty() && !future_part.parts.empty())
    {
        const auto & merging_params = future_part.parts.front()->storage.merging_params;

        /// Summing / Aggregating / Coalescing merges require every column, and a deduplicating merge may
        /// compare any subset chosen by DEDUPLICATE BY (unknown here) - keep every column priced there.
        const bool every_column_required = deduplicate
            || merging_params.mode == MergeTreeData::MergingParams::Summing
            || merging_params.mode == MergeTreeData::MergingParams::Aggregating
            || merging_params.mode == MergeTreeData::MergingParams::Coalescing;

        if (!every_column_required)
        {
            NameSet required_columns;
            const auto physical_names = output_columns.getNameSet();
            for (const auto & name : metadata_snapshot->getColumnsRequiredForSortingKey())
                required_columns.insert(
                    physical_names.contains(name) ? name : String(Nested::getColumnFromSubcolumn(name, physical_names)));
            if (!merging_params.sign_column.empty())
                required_columns.insert(merging_params.sign_column);
            if (!merging_params.is_deleted_column.empty())
                required_columns.insert(merging_params.is_deleted_column);
            if (!merging_params.version_column.empty())
                required_columns.insert(merging_params.version_column);
            if (merging_params.mode == MergeTreeData::MergingParams::Graphite)
            {
                required_columns.insert(merging_params.graphite_params.path_column_name);
                required_columns.insert(merging_params.graphite_params.time_column_name);
                required_columns.insert(merging_params.graphite_params.value_column_name);
                required_columns.insert(merging_params.graphite_params.version_column_name);
            }
            /// MergeTask forces at least one merged column when the key is empty.
            if (required_columns.empty() && !output_columns.empty())
                required_columns.insert(output_columns.front().name);
            /// A row-reducing merge recalculates the minmax index from the partition columns.
            for (const auto & name : metadata_snapshot->getColumnsRequiredForPartitionKey())
                required_columns.insert(name);

            NameSet unwritten_expired_columns;
            for (const auto & name : expired_columns)
                if (!required_columns.contains(name))
                    unwritten_expired_columns.insert(name);
            if (!unwritten_expired_columns.empty())
                output_columns = output_columns.eraseNames(unwritten_expired_columns);
        }
    }

    /// On top of the metadata's physical columns, a base merge writes the persisted `_block_number` /
    /// `_block_offset` virtual columns when the corresponding table settings are enabled (MergeTask adds
    /// them via addMergingColumn / addGatheringColumn after the expired-columns filter, so they are never
    /// expired); a projection merge never writes them (enabledBlockNumberColumn is false under a parent
    /// part). A source part that predates the setting does not store them - the reader synthesizes their
    /// values from the part's own block number - so, like any column missing from some source part, they
    /// are picked up by the default-filled detection below and their writer streams are priced at the
    /// per-stream worst case.
    const bool is_projection_merge = !future_part.parts.empty() && future_part.parts.front()->isProjectionPart();
    if (!is_projection_merge)
    {
        const auto add_persisted_virtual_column = [&](const String & name, const DataTypePtr & type)
        {
            if (!output_columns.contains(name))
                output_columns.emplace_back(name, type);
        };
        if (settings[MergeTreeSetting::enable_block_number_column])
            add_persisted_virtual_column(BlockNumberColumn::name, BlockNumberColumn::type);
        if (settings[MergeTreeSetting::enable_block_offset_column])
            add_persisted_virtual_column(BlockOffsetColumn::name, BlockOffsetColumn::type);
    }

    NamesAndTypesList input_columns = output_columns;
    input_columns.emplace_back(RowExistsColumn::name, RowExistsColumn::type);

    /// The source-part views of the read and written column sets: identical to the sets themselves unless
    /// a pending rename maps an output column to a differently-named source column (see part_source_view).
    const NamesAndTypesList part_view_input_columns = part_source_view(input_columns);
    const NamesAndTypesList part_view_output_columns = part_source_view(output_columns);

    /// Input side: one reader stream per column substream a base merge reader actually opens on every
    /// source part. The reader buffers hold a window of the compressed file plus the decompressed block,
    /// so they can never hold more than the data they read through: cap the per-part estimate by the size
    /// of the columns the merge reads (see partReadBytes) - not the whole part, which also counts dead
    /// dropped columns and the projection parts priced separately below. A patch part is accounted whole:
    /// it physically stores only the columns it patches plus the system columns needed to apply them, all
    /// of which its reader reads, and it has no projection parts.
    UInt64 input_memory = 0;
    UInt64 sum_input_bytes_uncompressed = 0;
    for (const auto & part : future_part.parts)
    {
        /// Compact and in-memory parts read all columns through a single shared stream.
        const size_t streams = part->getType() == MergeTreeDataPartType::Wide
            ? countPartStreamsForColumns(*part, part_view_input_columns)
            : 1;
        const UInt64 read_buffer_size = part_read_buffer_size(part);
        const ColumnSize read_bytes = partReadBytes(*part, part_view_input_columns);
        input_memory += std::min<UInt64>(streams * read_buffer_size, read_bytes.data_compressed + read_bytes.data_uncompressed);
        sum_input_bytes_uncompressed += read_bytes.data_uncompressed;
    }
    for (const auto & part : future_part.patch_parts)
    {
        const size_t streams = part->getType() == MergeTreeDataPartType::Wide
            ? countPartStreams(*part)
            : 1;
        const UInt64 read_buffer_size = part_read_buffer_size(part);
        const UInt64 part_bytes = part->getBytesOnDisk() + part->getBytesUncompressedOnDisk();
        input_memory += std::min<UInt64>(streams * read_buffer_size, part_bytes);
        sum_input_bytes_uncompressed += part->getBytesUncompressedOnDisk();
    }

    /// Output side: one writer stream per column substream of the result part. The result part is not
    /// written yet, so its dynamic substreams (JSON, Dynamic) are not known up front and the default
    /// serialization count would collapse such columns to a single stream. Estimate the result substreams
    /// as the union of the source parts' substreams (see countOutputStreams) - this both counts the actual
    /// dynamic substreams and covers the case where different source parts contribute disjoint dynamic
    /// paths that all appear in the merged part. Patch parts are included: a patched JSON / Dynamic column
    /// can carry a path that exists only in a patch, not in any base part.
    ///
    /// An output column absent from some base part is materialized for that part's rows during the merge:
    /// IMergeTreeReader::fillMissingColumns first fills the column with the type's default values, then
    /// evaluateMissingDefaults evaluates an explicit DEFAULT expression when the metadata has one. An
    /// explicit DEFAULT is NOT required for this to happen - any column still in output_columns here is
    /// written by the merge (a column absent from every base and patch part with no default was erased as
    /// expired above), so a column kept alive only because ANOTHER base part or a patch part stores it
    /// (ALTER ... ADD COLUMN with no DEFAULT followed by new inserts or a lightweight UPDATE) is
    /// default-filled for the older rows all the same. The synthesized values were never read from any
    /// source part, so nothing about them - neither their substreams nor their volume - can be derived
    /// from the sources:
    ///  - a dynamic-structure (JSON / Dynamic) such column can write real dynamic substreams that no source
    ///    part records, invisible to the recorded per-part substream union and to the legacy .bin recovery
    ///    alike - both countOutputStreams and the rebuilt-projection pricing must fall back to the output
    ///    type's write-time capacity for it instead of trusting the union as exact
    ///    (default_filled_dynamic_columns);
    ///  - a such column of ANY type breaks the input-volume cap on the writer's data-dependent buffers
    ///    below (default_filled_columns): its written bytes are not bounded by the bytes the merge reads.
    /// Only base parts decide missing-ness: a patch part physically stores just the columns it patches and
    /// its rows are applied as patches, never default-filled. A column missing from no base part keeps the
    /// exact pricing, so all of this is a no-op outside the schema-upgrade window.
    /// need_remove_expired_values / merge_may_reduce_rows exactly as the merge itself will compute them
    /// (MergeTask::ExecuteAndFinalizeHorizontalPart::prepare): a deduplicating or cleanup merge, a
    /// merge that removes expired TTL values, one that applies lightweight-delete masks or patch
    /// parts, or a non-Ordinary merging mode. All of these are knowable at selection time: deduplicate
    /// and cleanup come from the caller (an OPTIMIZE query or a replication log entry - background
    /// selection never sets them), the TTL state and delete masks from the source parts themselves.
    /// The TTL trigger compares the parts' part_min_ttl against the SAME time_of_merge the merge itself
    /// runs with (the selection time for a non-replicated merge, entry.create_time for a replicated one
    /// - see MergeMutateSelectedEntry::time_of_merge), not against the wall clock of this estimate: a
    /// merge that sits in the background queue while a TTL boundary passes must not execute as a
    /// row-reducing TTL merge that its reservation priced as an ordinary one. The one remaining
    /// deviation - the merge skips TTL removal while a ttl_merges_blocker is held - can only make the
    /// merge rebuild fewer projections than priced here, which is the safe direction for a reservation.
    bool need_remove_expired_values = false;
    if (metadata_snapshot->hasAnyTTL())
    {
        IMergeTreeDataPart::TTLInfos merged_ttl_infos;
        for (const auto & part : future_part.parts)
        {
            merged_ttl_infos.update(part->ttl_infos);
            if (!part->checkAllTTLCalculated(metadata_snapshot))
                need_remove_expired_values = true;
        }
        if (merged_ttl_infos.part_min_ttl && merged_ttl_infos.part_min_ttl <= time_of_merge)
            need_remove_expired_values = true;
    }

    const bool has_lightweight_delete = std::any_of(
        source_and_patch_parts.begin(), source_and_patch_parts.end(),
        [](const auto & part) { return part->hasLightweightDelete(); });

    const bool merge_may_reduce_rows = !future_part.parts.empty()
        && (deduplicate
            || cleanup
            || need_remove_expired_values
            || has_lightweight_delete
            || !future_part.patch_parts.empty()
            || future_part.parts.front()->storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary);

    /// Missing-ness is probed under the source part's own name for the column (a pending rename target is
    /// stored - and read - under its old name, and is default-filled only for the rows of parts that lack
    /// that old name, exactly as the reader behaves). default_filled_columns keeps the OUTPUT names (it is
    /// matched against output_columns below), while default_filled_dynamic_columns keeps the source-view
    /// names: its only consumers are countOutputStreams / countRebuiltProjectionStreams, which probe the
    /// source parts through the translated column lists. With no pending renames the two spaces coincide.
    NameSet default_filled_columns;
    NameSet default_filled_dynamic_columns;
    for (const auto & column : output_columns)
    {
        const String & source_name = part_source_name(column.name);
        const bool missing_from_some_part = std::any_of(
            future_part.parts.begin(), future_part.parts.end(),
            [&](const auto & part) { return !part->getColumns().contains(source_name); });
        if (missing_from_some_part)
        {
            default_filled_columns.insert(column.name);
            if (column.type->hasDynamicStructure())
                default_filled_dynamic_columns.insert(source_name);
        }
    }

    /// A compact output part writes every column through one shared writer buffer, and that writer does not
    /// take the wide writer's per-stream adaptive decision, so its single stream is priced non-adaptive.
    const auto output_stream_counts = future_part.part_format.part_type == MergeTreeDataPartType::Wide
        ? countOutputStreams(part_view_output_columns, source_and_patch_parts, settings, default_filled_dynamic_columns)
        : WriterStreamCounts{.total = 1, .non_adaptive = 1};
    const size_t output_streams = output_stream_counts.total;

    /// Per-stream write buffer size on a local disk: a writer stream keeps the compressor block and the
    /// file buffer, both sized by the stream's max_compress_block_size. That size is not one table-wide
    /// constant: MergeTreeDataPartWriterWide::addStreams resolves it per stream from the column-level
    /// max_compress_block_size setting when the column overrides it, falling back to the table setting and
    /// then to the global one, and clamps the result (see MergeTreeWriterSettings). Collapse the per-stream
    /// sizes to the LARGEST size any written column resolves to: one size can only over-reserve the streams
    /// of the other columns (the safe direction, and exact for the common table without overrides), while
    /// sizing everything from the table setting would under-reserve every stream of a column whose override
    /// is LARGER - the writer would allocate bigger eager buffers than were reserved, and the admission gate
    /// could admit more concurrent merges than the reservation is supposed to bound.
    /// The writer settings a projection is written with are its own: a projection definition may override
    /// MergeTree writer settings with WITH SETTINGS (max_compress_block_size among them, see
    /// ProjectionsDescription), and writeProjectionPartImpl builds the projection writer from
    /// getSettings(&projection.settings_changes). So this resolution is parameterized by the effective
    /// settings and the written columns, and the projection pricing below instantiates it again with the
    /// projection's own settings and columns instead of reusing the parent table's size.
    const auto resolve_local_write_buffer_size = [&](const MergeTreeSettings & writer_settings,
                                                     const NamesAndTypesList & written_columns,
                                                     const ColumnsDescription & written_columns_description)
    {
        UInt64 max_compress_block_size = writer_settings[MergeTreeSetting::max_compress_block_size];
        if (max_compress_block_size == 0)
            max_compress_block_size = context->getSettingsRef()[Setting::max_compress_block_size];
        max_compress_block_size = std::min<UInt64>(max_compress_block_size, MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE);
        for (const auto & column : written_columns)
        {
            const auto column_desc = written_columns_description.tryGetColumnDescription(
                GetColumnsOptions(GetColumnsOptions::AllPhysical), column.getNameInStorage());
            if (!column_desc)
                continue;
            const auto * override_value = column_desc->settings.tryGet("max_compress_block_size");
            if (!override_value)
                continue;
            const UInt64 column_override = override_value->safeGet<UInt64>();
            if (column_override == 0)
                continue;
            max_compress_block_size = std::max(
                max_compress_block_size, std::min<UInt64>(column_override, MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE));
        }
        return 2 * max_compress_block_size;
    };
    const UInt64 local_write_buffer_size = resolve_local_write_buffer_size(settings, output_columns, columns_description);

    /// Worst case: every stream allocates all of its buffers in full. A zero remote_write_buffer_size
    /// means the output is not written through multipart upload buffers (a local disk, a known remote disk
    /// without them, or a local pre-disk-selection guess), so the local per-stream size applies.
    const auto worst_case_write_buffer_size = [&](UInt64 non_adaptive_buffer_size)
    {
        return remote_write_buffer_size != 0 ? remote_write_buffer_size : non_adaptive_buffer_size;
    };
    const UInt64 write_buffer_size = worst_case_write_buffer_size(local_write_buffer_size);
    const UInt64 output_worst_case = saturatingStreamsTimesBuffer(output_streams, write_buffer_size);

    /// However, only the compressor block and the file buffer are allocated eagerly (and they start at
    /// adaptive_write_buffer_initial_size when adaptive write buffers are active for this part). Object
    /// storage upload buffers - and the growth of adaptive buffers - only ever hold data that has already
    /// been written into them, so their total is bounded by the volume of data the merge writes. Bound it
    /// by the merged output volume, not by the compressed size of the source parts: a merge interleaves
    /// rows from several parts, and parts that each compressed very well on their own (for example even /
    /// odd primary keys with per-part constant values) can merge into a row order that compresses far
    /// worse, so the produced compressed output - which the multipart writers keep alive in their upload
    /// buffers - can be much larger than 2 * sum_input_bytes_compressed. The merged part never holds more
    /// uncompressed data than the merge reads from the source parts (a merge does not add rows, and
    /// dedup / cleanup / TTL only remove them; a dead dropped column is never written and the projection
    /// parts are priced separately, so their bytes rightly do not enter this bound), and its compressed
    /// size cannot exceed that uncompressed volume, so sum_input_bytes_uncompressed - accumulated over the
    /// columns actually read - is a sound upper bound on the produced compressed output for every column
    /// the merge reads (a default-filled column writes synthesized data the merge never read; it is
    /// priced separately below, see default_filled_term). Cap the
    /// data-dependent buffers at the compressed output in flight twice over (double buffering of uploads)
    /// plus one uncompressed working block per stream, all bounded by the uncompressed input volume.
    /// Without this cap a merge of tiny parts in a table with many columns on object storage would reserve
    /// gigabytes it can never touch, and concurrent merges would saturate the soft limit and starve each
    /// other for no reason.
    /// Which streams start at adaptive_write_buffer_initial_size is a PER-STREAM, PER-WRITER decision
    /// (MergeTreeDataPartWriterWide::addStreams): a stream is adaptive when the writer's own columns list
    /// reaches min_columns_to_activate_adaptive_write_buffer - the list of THAT writer, so a vertical
    /// merge's gathering stage, which writes one column per writer, never activates the count-based rule
    /// however wide the table is - or when use_adaptive_write_buffer_for_dynamic_subcolumns is on and the
    /// substream is dynamic, regardless of the column count. Price the two classes separately: charging a
    /// dynamic substream the full 2 * max_compress_block_size (as one shared per-stream size would) is the
    /// same over-reservation/starvation pattern the data-volume bounds unwind - a wide JSON / Dynamic merge
    /// has thousands of dynamic substreams whose eager buffers are 16 KiB, not megabytes - while charging a
    /// non-adaptive stream the adaptive initial size under-reserves its eagerly allocated full-size buffers.
    const UInt64 min_columns_for_adaptive = settings[MergeTreeSetting::min_columns_to_activate_adaptive_write_buffer];
    const bool adaptive_for_dynamic_subcolumns = settings[MergeTreeSetting::use_adaptive_write_buffer_for_dynamic_subcolumns];
    const UInt64 adaptive_eager_buffers_per_stream = 2 * settings[MergeTreeSetting::adaptive_write_buffer_initial_size];
    const auto non_adaptive_stream_count = [&](const WriterStreamCounts & counts, size_t writer_columns) -> size_t
    {
        if (min_columns_for_adaptive != 0 && writer_columns >= min_columns_for_adaptive)
            return 0;
        return adaptive_for_dynamic_subcolumns ? counts.non_adaptive : counts.total;
    };
    /// non_adaptive_buffer_size is the writer's own per-stream size: the parent table's for the base
    /// output, the projection's own for a rebuilt projection whose definition overrides
    /// max_compress_block_size. The adaptive sizes are not per-writer - none of the settings behind them
    /// is allowed in a projection's WITH SETTINGS (see ALLOWED_PROJECTION_SETTINGS), so every writer
    /// resolves them from the table settings.
    const auto eager_write_buffers = [&](const WriterStreamCounts & counts, size_t writer_columns, UInt64 non_adaptive_buffer_size) -> UInt64
    {
        const size_t non_adaptive = non_adaptive_stream_count(counts, writer_columns);
        return non_adaptive * non_adaptive_buffer_size + (counts.total - non_adaptive) * adaptive_eager_buffers_per_stream;
    };

    /// A stream whose data volume this estimate cannot derive from the source parts - a rebuilt projection,
    /// a variable-size DEFAULT-filled column, a delayed vertical stream - is priced at the buffers its
    /// writer allocates before any data flows through it, which is exactly eager_write_buffers above: the
    /// compressor block and the file buffer, at the stream's own write buffer size. That holds on multipart
    /// object storage too, because a stream's upload buffer STARTS at that same size and only grows with the
    /// data written into it (see remote_write_buffer_size above): the writer pins neither the full multipart
    /// ceiling (~100 GiB with default S3 settings, unbounded when in-flight parts are unlimited) nor even
    /// the allocation policy's first-part size (32 MiB on S3, 100 MiB on Azure by default). Charging either
    /// of them per stream is over-reservation, and over-reservation on unknowable-volume streams is a proven
    /// CI starvation regression: priced at the ceiling, a TTL merge of a 2-row table came to 200.09 GiB
    /// (two ceiling-priced streams) and was rejected by the admission gate for the whole 300 s window of
    /// 03365_column_ttl_should_rebuild_skp_idx_and_proj - concurrent merges always held some reservation, so
    /// it never ran and the column TTL never took effect. The growth of such a stream past its eager buffers
    /// is real data the reactive background_memory_tracker sees as it materializes, so under-pricing it here
    /// degrades to master's purely reactive behavior for that stream instead of blocking all progress.

    /// The input-volume bound holds only for data the merge actually READS. A column filled for the rows
    /// of parts that predate its ALTER ... ADD COLUMN (default_filled_columns above) is synthesized by
    /// IMergeTreeReader::fillMissingColumns / evaluateMissingDefaults - from the type's default values,
    /// or from an explicit DEFAULT expression when the metadata has one - not read: a default such as
    /// repeat(toString(k), 1000) writes orders of magnitude more bytes than the
    /// merge reads, so capping its upload buffers by the input volume would under-reserve the writer's
    /// real footprint on object storage. Price the synthesized volume instead: a fixed-size type writes
    /// exactly rows * value size, so 3x that volume (the same in-flight allowance as the base bound) is
    /// sound for it; a variable-size type's default expression is arbitrary, so its streams are priced at
    /// their eager write buffers with the growth left to the reactive tracker (see the unknowable-volume
    /// pricing above - the multipart alternatives are a proven starvation regression). A column present in only SOME
    /// parts is synthesized for the OTHER parts' rows alone, so only those rows are priced here
    /// (countRowsMissingColumn): the rows of the parts that do store the column are read, and their written
    /// bytes are already inside sum_input_bytes_uncompressed. Outside the ADD COLUMN ... DEFAULT upgrade
    /// window the extra term is zero.
    /// The eager write buffers of the default-filled streams themselves are NOT added here: the
    /// default-filled columns stay in output_columns, so every bound this term is added to already prices
    /// them exactly once - the horizontal base term through output_stream_counts, the vertical bound
    /// through its merging / gathering / delayed terms. Charging them again would double the eager side
    /// for the ADD COLUMN ... DEFAULT path - on a wide JSON / Dynamic default enough to saturate
    /// merges_mutations_memory_usage_soft_limit and reject background merges that fit, the same
    /// over-reservation/starvation pattern the rest of this estimate unwinds.
    UInt64 default_filled_value_bytes = 0;
    UInt64 sum_rows = 0;
    for (const auto & part : future_part.parts)
        sum_rows += part->rows_count;
    for (const auto & column : output_columns)
    {
        if (!default_filled_columns.contains(column.name))
            continue;
        if (column.type->haveMaximumSizeOfValue())
            default_filled_value_bytes
                += countRowsMissingColumn(future_part.parts, part_source_name(column.name)) * column.type->getMaximumSizeOfValueInMemory();
    }

    const UInt64 default_filled_term = 3 * default_filled_value_bytes;

    const UInt64 output_data_bound = eager_write_buffers(output_stream_counts, output_columns.size(), local_write_buffer_size)
        + 3 * sum_input_bytes_uncompressed
        + default_filled_term;

    UInt64 output_memory = std::min(output_worst_case, output_data_bound);

    /// Both bounds above assume every output stream's buffers are alive at once, which is only true for a
    /// HORIZONTAL merge. A vertical merge writes the merging (key) columns in its horizontal stage and then
    /// gathers the remaining columns ONE AT A TIME (MergeTask::VerticalMergeStage); on parallel-write
    /// (object) storage up to max_merge_delayed_streams_for_parallel_write finished column streams are kept
    /// alive with delayed finalization, each holding no more than its unflushed remnant, and everything
    /// else is already finalized. Pricing all streams as concurrent is a proven starvation regression for
    /// big wide-table merges: a 19-part merge of test.hits_s3 was priced above the whole
    /// merges_mutations_memory_usage_soft_limit (3x its uncompressed volume > 14.41 GiB) while actually
    /// using ~139 MiB, and its reservation closed the admission gate for every other merge on the server
    /// for the minutes it ran (04492_schedule_merge_retry_on_busy_pool timed out waiting for a manual
    /// merge). Mirror MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm conservatively:
    /// any case that is not certainly vertical (TTL, cleanup, deduplication, non-Wide output, compact
    /// sources when not allowed, unsupported merging mode, activation thresholds) keeps the horizontal
    /// pricing, which can only over-reserve. For a certainly-vertical merge, price the streams that are
    /// actually alive at once: the merging columns (fully concurrent in the horizontal stage), the single
    /// widest gathering column, and the delayed remnants.
    ///
    /// A TTL merge is not horizontal by itself: chooseMergeAlgorithm forces Horizontal for
    /// need_remove_expired_values only when MergeTask::canVerticalTTLDelete is false, so an Ordinary
    /// rows-TTL (or rows-WHERE-TTL) merge under vertical_merge_optimize_ttl_delete still gathers its
    /// columns one at a time and must not be priced at the full horizontal footprint - that is the same
    /// starvation pattern as above, and a rows-TTL merge of a big wide table on object storage is exactly
    /// where it hurts. Mirror canVerticalTTLDelete conservatively: everything it rejects (a GROUP BY TTL,
    /// a column TTL, a lightweight delete, a non-Ordinary mode) keeps the horizontal pricing. The
    /// lightweight-delete term is the broader source_and_patch_parts one, which can only reject more
    /// merges from the vertical pricing than the merge itself does - again the over-reserving direction.
    const bool can_vertical_ttl_delete = !future_part.parts.empty()
        && future_part.parts.front()->storage.merging_params.mode == MergeTreeData::MergingParams::Ordinary
        && settings[MergeTreeSetting::vertical_merge_optimize_ttl_delete]
        && !metadata_snapshot->hasAnyGroupByTTL()
        && !metadata_snapshot->hasAnyColumnTTL()
        && !has_lightweight_delete
        && (metadata_snapshot->hasRowsTTL() || metadata_snapshot->hasAnyRowsWhereTTL());

    if (!future_part.parts.empty()
        && future_part.part_format.part_type == MergeTreeDataPartType::Wide
        && future_part.part_format.storage_type == MergeTreeDataPartStorageType::Full
        && settings[MergeTreeSetting::enable_vertical_merge_algorithm] != 0
        && !deduplicate
        && !cleanup
        && (!need_remove_expired_values || can_vertical_ttl_delete)
        && future_part.parts.size() <= RowSourcePart::MAX_PARTS)
    {
        const auto & merging_params = future_part.parts.front()->storage.merging_params;
        const bool is_supported_mode = merging_params.mode == MergeTreeData::MergingParams::Ordinary
            || merging_params.mode == MergeTreeData::MergingParams::Collapsing
            || merging_params.mode == MergeTreeData::MergingParams::Replacing
            || merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

        bool sources_allow_vertical = settings[MergeTreeSetting::allow_vertical_merges_from_compact_to_wide_parts]
            || std::all_of(future_part.parts.begin(), future_part.parts.end(),
                [](const auto & part) { return part->getType() == MergeTreeDataPartType::Wide; });

        UInt64 sum_bytes_uncompressed = 0;
        for (const auto & part : future_part.parts)
            sum_bytes_uncompressed += part->getTotalColumnsSize().data_uncompressed;

        if (is_supported_mode && sources_allow_vertical
            && sum_rows >= settings[MergeTreeSetting::vertical_merge_algorithm_min_rows_to_activate]
            && sum_bytes_uncompressed >= settings[MergeTreeSetting::vertical_merge_algorithm_min_bytes_to_activate])
        {
            /// The merging (horizontal-stage) columns, mirroring extractMergingAndGatheringColumns as an
            /// over-approximation: every skip-index column and every projection's required columns join it
            /// (MergeTask adds only non-excluded indexes and the projections it actually rebuilds), and the
            /// min-max index columns join when the merge may reduce rows. Over-classifying a column as
            /// merging prices it fully concurrent - the over-reserving, safe direction - and shrinks the
            /// gathering set, which can only make the activation check below stricter than the merge's own.
            NameSet merging_names;
            const auto physical_names = output_columns.getNameSet();
            const auto insert_storage_name = [&](const String & name)
            {
                /// A required name may not resolve to a written column at all: a projection or a skip
                /// index can reference a column this merge does not write - a fully expired TTL column
                /// with no default was erased from output_columns above (its files are gone from every
                /// source part, see 04492_projection_ttl_default_divergence). A column the merge does not
                /// write cannot be classified as merging or gathering, so skip it instead of throwing.
                if (const auto storage_name = Nested::tryGetColumnNameInStorage(name, physical_names))
                    merging_names.insert(*storage_name);
            };
            for (const auto & name : metadata_snapshot->getColumnsRequiredForSortingKey())
                insert_storage_name(name);
            if (!merging_params.sign_column.empty())
                merging_names.insert(merging_params.sign_column);
            if (!merging_params.is_deleted_column.empty())
                merging_names.insert(merging_params.is_deleted_column);
            if (!merging_params.version_column.empty())
                merging_names.insert(merging_params.version_column);
            for (const auto & index : metadata_snapshot->getSecondaryIndices())
                for (const auto & name : index.expression->getRequiredColumns())
                    insert_storage_name(name);
            for (const auto & projection : metadata_snapshot->getProjections())
                for (const auto & name : projection.getRequiredColumns())
                    insert_storage_name(name);
            if (merge_may_reduce_rows)
                for (const auto & name : metadata_snapshot->getColumnsRequiredForPartitionKey())
                    insert_storage_name(name);
            /// A vertical TTL-delete merge evaluates the TTL filter in the horizontal stage, so
            /// extractMergingAndGatheringColumns pulls every column the TTL expressions read into the
            /// merging set (MergeTask.cpp, the canVerticalTTLDelete branch). Mirror it, or those columns
            /// would be priced as gathered one at a time while the merge keeps them all alive.
            if (need_remove_expired_values && can_vertical_ttl_delete)
            {
                const auto insert_ttl_expression_columns = [&](const TTLDescription & ttl_description)
                {
                    for (const auto & column : ttl_description.expression_columns)
                        insert_storage_name(column.name);
                    for (const auto & column : ttl_description.where_expression_columns)
                        insert_storage_name(column.name);
                };
                if (metadata_snapshot->hasRowsTTL())
                    insert_ttl_expression_columns(metadata_snapshot->getRowsTTL());
                for (const auto & rows_where_ttl : metadata_snapshot->getRowsWhereTTLs())
                    insert_ttl_expression_columns(rows_where_ttl);
            }
            if (merging_names.empty() && !output_columns.empty())
                merging_names.insert(output_columns.front().name);
            /// The persisted block columns can be merged in the horizontal stage (need_block_number_in_merge);
            /// classify them as merging - the over-reserving, safe direction for two single-stream columns.
            merging_names.insert(BlockNumberColumn::name);
            merging_names.insert(BlockOffsetColumn::name);

            NamesAndTypesList merging_columns;
            NamesAndTypesList gathering_columns;
            for (const auto & column : output_columns)
            {
                if (merging_names.contains(column.name))
                    merging_columns.push_back(column);
                else
                    gathering_columns.push_back(column);
            }

            if (gathering_columns.size() >= settings[MergeTreeSetting::vertical_merge_algorithm_min_columns_to_activate])
            {
                const NamesAndTypesList part_view_merging_columns = part_source_view(merging_columns);
                const auto merging_stream_counts
                    = countOutputStreams(part_view_merging_columns, source_and_patch_parts, settings, default_filled_dynamic_columns);

                /// The eager buffers are priced per WRITER, mirroring how a vertical merge writes: the
                /// horizontal stage's writer sees only the merging columns (so the count-based adaptive
                /// rule uses their count, not the table width), and each gathering column is written by
                /// its own single-column writer, for which the count-based rule never fires - take the
                /// most expensive single gathering writer, since only one gathers at a time.
                size_t gathering_streams_total = 0;
                size_t max_gathering_column_streams = 0;
                UInt64 max_gathering_column_eager_buffers = 0;
                UInt64 max_gathering_column_uncompressed = 0;
                for (const auto & column : gathering_columns)
                {
                    const NamesAndTypesList single_column{NameAndTypePair(part_source_name(column.name), column.type)};
                    const auto column_stream_counts
                        = countOutputStreams(single_column, source_and_patch_parts, settings, default_filled_dynamic_columns);
                    gathering_streams_total += column_stream_counts.total;
                    max_gathering_column_streams = std::max(max_gathering_column_streams, column_stream_counts.total);
                    max_gathering_column_eager_buffers
                        = std::max(max_gathering_column_eager_buffers, eager_write_buffers(column_stream_counts, 1, local_write_buffer_size));

                    UInt64 column_uncompressed = 0;
                    for (const auto & part : source_and_patch_parts)
                        column_uncompressed += partReadBytes(*part, single_column).data_uncompressed;
                    max_gathering_column_uncompressed = std::max(max_gathering_column_uncompressed, column_uncompressed);
                }

                UInt64 merging_uncompressed = 0;
                for (const auto & part : source_and_patch_parts)
                    merging_uncompressed += partReadBytes(*part, part_view_merging_columns).data_uncompressed;

                const UInt64 delayed_streams = remote_write_buffer_size != 0
                    ? std::min<UInt64>(gathering_streams_total, settings[MergeTreeSetting::max_merge_delayed_streams_for_parallel_write])
                    : 0;
                const UInt64 alive_streams = merging_stream_counts.total + max_gathering_column_streams + delayed_streams;

                const UInt64 vertical_worst_case = saturatingStreamsTimesBuffer(alive_streams, write_buffer_size);
                /// A delayed stream's adaptivity is not attributable (any of the gathering columns' streams
                /// can be the ones kept alive), so its eager buffers are priced at the full non-adaptive
                /// size - the direction that can only over-price, and bounded by
                /// max_merge_delayed_streams_for_parallel_write.
                const UInt64 vertical_data_bound = eager_write_buffers(merging_stream_counts, merging_columns.size(), local_write_buffer_size)
                    + max_gathering_column_eager_buffers
                    + delayed_streams * local_write_buffer_size
                    + 3 * (merging_uncompressed + max_gathering_column_uncompressed)
                    + default_filled_term;

                output_memory = std::min({output_memory, vertical_worst_case, vertical_data_bound});
            }
        }
    }

    /// Projections: the merge also reads and writes projection parts, and none of that IO flows through
    /// the base parts' readers and writers priced above. Mirror the decision made in
    /// MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild:
    ///  - a non-Ordinary merge (Replacing, Summing, ...) under the throw / drop
    ///    deduplicate_merge_projection_mode does not process projections at all;
    ///  - a merge that may reduce rows REBUILDS every projection from the merged rows regardless of
    ///    whether the source parts already have it, unless the mode is IGNORE and the projection is not a
    ///    special (parent-offset / block-number / block-offset) one. This is exactly the
    ///    merge_may_reduce_rows branch of prepareProjectionsToMergeAndRebuild, so a patch part that adds a
    ///    new JSON path or expands a projection expression is priced as a rebuild, not as a merge of the
    ///    stale source projection parts;
    ///  - a projection that requires an expired column (absent from every source part with no default
    ///    expression) is likewise rebuilt regardless of whether the source parts have it;
    ///  - otherwise, when every source part has the projection, the projection parts are merged by a
    ///    nested MergeTask over exactly those parts with the projection's own metadata
    ///    (MergeProjectionsStage::prepareProjections builds the very same FutureMergedMutatedPart), so
    ///    price that nested merge with this same estimate, recursively - a projection has no projections
    ///    of its own, so the recursion is one level deep;
    ///  - when some or all source parts lack the projection, the merge rebuilds it from the merged rows
    ///    only for commit-order projections (which are never written on insert) and under
    ///    materialize_projections_on_merge, and drops it from the result otherwise.
    /// A rebuild does not read the existing projection parts: it recalculates the projection from rows
    /// already flowing through the merge, writes temporary projection parts (one temp-part writer at a
    /// time per projection, see writeTempProjectionPart) and then merges the temporary parts back
    /// (MergeProjectionPartsTask, which merges up to max_parts_to_merge_in_one_level of them at once), so
    /// price one set of writer streams plus a read-back that can hold that many reader-buffer sets. For a
    /// table without projections all of this adds exactly nothing.
    UInt64 projection_memory = 0;
    const auto projection_mode = settings[MergeTreeSetting::deduplicate_merge_projection_mode];
    const bool merge_processes_projections = !future_part.parts.empty()
        && (future_part.parts.front()->storage.merging_params.mode == MergeTreeData::MergingParams::Ordinary
            || (projection_mode != DeduplicateMergeProjectionMode::THROW && projection_mode != DeduplicateMergeProjectionMode::DROP));
    if (merge_processes_projections)
    {
        /// An expired column (absent from every source part with no default expression, the full
        /// expired_columns set computed above) makes the merge rebuild every projection that requires it,
        /// again before checking whether the source parts have the projection.
        for (const auto & projection : metadata_snapshot->getProjections())
        {
            /// A special (parent-offset / block-number / block-offset) projection is rebuilt by a
            /// row-reducing merge even under the IGNORE mode; a plain one is rebuilt only when the mode is
            /// not IGNORE. A projection requiring an expired column is likewise rebuilt before the source
            /// parts are checked for it - mirroring prepareProjectionsToMergeAndRebuild exactly.
            const bool is_special_projection
                = projection.with_parent_part_offset || projection.with_block_number || projection.with_block_offset;
            const auto & required_columns = projection.getRequiredColumns();
            const bool some_source_column_expired = std::any_of(
                required_columns.begin(), required_columns.end(),
                [&](const String & name) { return expired_columns.contains(name); });
            const bool rebuild_regardless_of_presence
                = (merge_may_reduce_rows && (projection_mode != DeduplicateMergeProjectionMode::IGNORE || is_special_projection))
                || (some_source_column_expired && projection_mode != DeduplicateMergeProjectionMode::IGNORE);

            /// An existing projection part may lack a column the current projection metadata expects (an
            /// ALIAS selected by the projection re-pointed by ALTER): the merge rebuilds the projection
            /// from the parent rows rather than bake stale defaults into the merged part - the same check
            /// as prepareProjectionsToMergeAndRebuild, where a parent TABLE column the parent part also
            /// lacks is a legitimate late-add and does not count.
            const auto projection_columns = projection.metadata->getColumns().getAllPhysical();
            bool projection_part_misses_column = false;

            MergeTreeData::DataPartsVector projection_parts;
            for (const auto & part : future_part.parts)
            {
                auto it = part->getProjectionParts().find(projection.name);
                if (it != part->getProjectionParts().end() && !it->second->is_broken)
                {
                    for (const auto & column : projection_columns)
                    {
                        if (!it->second->tryGetColumn(column.name)
                            && (part->tryGetColumn(part_source_name(column.name))
                                || !columns_description.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column.name)))
                        {
                            projection_part_misses_column = true;
                            break;
                        }
                    }

                    projection_parts.push_back(it->second);
                }
            }

            /// Decide, exactly as the merge will, whether this projection is merged from the existing
            /// projection parts, rebuilt from the merged rows, or dropped from the result.
            bool rebuild_projection = rebuild_regardless_of_presence;
            if (!rebuild_regardless_of_presence)
            {
                if (projection_part_misses_column && projection_mode != DeduplicateMergeProjectionMode::IGNORE)
                {
                    rebuild_projection = true;
                }
                else if (projection_parts.size() != future_part.parts.size())
                {
                    /// Both commit-order projections rebuild here, mirroring
                    /// prepareProjectionsToMergeAndRebuild: a `_block_offset` one joined `_block_number`
                    /// in d673d9e5a6e ("Introduce Invalidated System Columns"), because a merge
                    /// invalidates `_block_offset` and the projection has to be recalculated from the
                    /// merged rows rather than merged from stale per-part offsets.
                    if (projection.with_block_number || projection.with_block_offset
                        || settings[MergeTreeSetting::materialize_projections_on_merge])
                        rebuild_projection = true;
                    else
                        continue; /// Dropped from the merged part - no IO.
                }
            }

            /// A projection is written with its OWN effective MergeTree settings: a projection definition
            /// may override writer settings with WITH SETTINGS (max_compress_block_size, the wide-part
            /// thresholds, the sparse-serialization ratio - see ALLOWED_PROJECTION_SETTINGS), and every
            /// projection writer resolves them through getSettings(&projection.settings_changes), exactly
            /// as writeProjectionPartImpl does. Pricing both projection paths from the parent table's
            /// settings would under-reserve a projection that raises max_compress_block_size - its writer
            /// allocates bigger eager buffers per stream than the reservation accounts for, so the
            /// admission gate would admit more concurrent merges than the reservation bounds. Without a
            /// WITH SETTINGS clause the changes are empty and this resolves to the table settings.
            const auto projection_settings_holder = projection.settings_changes.empty()
                ? nullptr
                : future_part.parts.front()->storage.getSettings(&projection.settings_changes);
            const MergeTreeSettings & projection_settings
                = projection_settings_holder ? *projection_settings_holder : settings;

            if (!rebuild_projection)
            {
                FutureMergedMutatedPart projection_future_part;
                projection_future_part.assign(std::move(projection_parts), /*patch_parts_=*/ {}, &projection);
                /// A projection part is never subject to a pending on-fly rename (renaming a column used
                /// in a projection is forbidden, and the nested merge runs over the projection's own
                /// columns), so the recursion needs no mutations snapshot.
                projection_memory += estimateNeededMemoryForMerge(
                    projection_future_part,
                    projection.metadata,
                    context,
                    projection_settings,
                    /*mutations_snapshot=*/ nullptr,
                    time_of_merge,
                    output_on_remote_disk,
                    remote_write_buffer_memory);
            }
            else
            {
                /// The temporary parts are written into the result part's own storage, so they share the
                /// destination disk's write buffer sizing and are read back from that same disk. The rebuilt
                /// projection is recalculated from the merged base rows, so a semi-structured (JSON / Dynamic)
                /// projection column materializes real dynamic substreams (writeTempProjectionPart writes one
                /// stream per substream); count them with countRebuiltProjectionStreams rather than the default
                /// serialization, which would collapse such a column to a single stream and undersize the
                /// reservation.
                const auto projection_column_list = projection.sample_block.getNamesAndTypesList();
                const auto projection_wide_stream_counts = countRebuiltProjectionStreams(
                    part_source_view(projection_column_list), source_and_patch_parts, projection_settings, default_filled_dynamic_columns);

                /// The temp-part writer's per-stream buffers are sized by the projection's own
                /// max_compress_block_size and by the projection columns' own column-level overrides
                /// (the projection metadata carries them), not by the parent table's.
                const UInt64 projection_local_write_buffer_size = resolve_local_write_buffer_size(
                    projection_settings, projection_column_list, projection.metadata->getColumns());

                /// A temporary projection part is written as Wide only when it is big enough:
                /// writeTempProjectionPart passes the projected block's size to choosePartFormat, which picks
                /// Compact below min_bytes_for_wide_part (default 10 MiB) / min_rows_for_wide_part, and a merge
                /// output part's level always clears min_level_for_wide_part. A Compact temp part writes every
                /// column through ONE shared writer buffer, and MergeProjectionPartsTask reads it back through
                /// ONE shared reader buffer per part - not one per substream. Pricing a per-substream stream
                /// count for a Compact temp part would over-reserve by orders of magnitude on a semi-structured
                /// projection and serialize background merges, the very starvation this estimate avoids on the
                /// base path. Estimate the projected data volume from the projection's own columns in the source
                /// parts (whole part size for a compact part, whose per-column sizes are not tracked) and let
                /// choosePartFormat decide the format exactly as the merge will, honoring any per-projection
                /// wide-part settings. An expression that expands bytes per row can push a small input over the
                /// wide threshold; underestimating the projected size here can only misclassify a Wide temp part
                /// as Compact, which merely weakens throttling of concurrent merges (a single merge is always
                /// admitted), the safe direction. writeTempProjectionPart formats the temporary part after
                /// patches are applied to the merged rows, so include the patch parts (the same patched input
                /// set the stream count above uses): a patch that inflates a projected column can push the real
                /// temp part over min_bytes_for_wide_part, and counting the patch bytes here lets them
                /// participate in the Wide-vs-Compact decision. Patch parts update EXISTING rows and never add
                /// any, so the rebuilt rows are the base parts' rows alone, and a part that does not physically
                /// store any projection-required column contributes no projected bytes at all: a patch on an
                /// unrelated column, or an old part predating an ALTER ... ADD COLUMN ... DEFAULT the projection
                /// reads, must not flip a genuinely Compact rebuild to Wide with bytes the temp part never
                /// writes. A projection may require a SUBCOLUMN (SELECT json.a ORDER BY json.a): the merge
                /// normalizes such a name through getColumnNameInStorage and reads only that subcolumn's
                /// streams, but columns_sizes is keyed by the storage column, so getColumnSize returns nothing
                /// for it - price it the way the read path does (MergeTreeBlockReadUtils), through
                /// getSubcolumnSize, or the whole (possibly huge) parent column would stand in for a tiny
                /// subcolumn. The whole-part size stands in only for a part that DOES store a required column
                /// but tracks no per-column sizes at all (a Compact part, whose each_columns_size is left
                /// empty) - an over-approximation bounded by bytes that part really stores. Rows of parts that lack a
                /// required column get it synthesized from its default: a fixed-size type writes exactly
                /// (rows of those parts alone, see countRowsMissingColumn) * value size, so add that - the
                /// rows of the parts that do store it are already counted through their own projected column
                /// bytes above; a variable-size default's volume is unknowable, so its
                /// streams stay priced at their eager write buffers below with the growth left to the reactive
                /// tracker - the same accounting as the base path's default_filled_term.
                UInt64 projection_uncompressed_bytes = 0;
                UInt64 projection_rows = 0;
                for (const auto & part : future_part.parts)
                    projection_rows += part->rows_count;
                for (const auto & part : source_and_patch_parts)
                {
                    UInt64 part_projection_bytes = 0;
                    bool part_stores_required_column = false;
                    for (const auto & required_column : required_columns)
                    {
                        const String & required_source_name = part_source_name(required_column);
                        const auto part_column = part->tryGetColumn(required_source_name);
                        if (!part_column)
                            continue;
                        part_stores_required_column = true;
                        part_projection_bytes += part_column->isSubcolumn()
                            ? part->getSubcolumnSize(required_source_name).data_uncompressed
                            : part->getColumnSize(part_column->getNameInStorage()).data_uncompressed;
                    }
                    if (part_stores_required_column && part->getColumnSizes()->empty())
                        part_projection_bytes = part->getBytesUncompressedOnDisk();
                    projection_uncompressed_bytes += part_projection_bytes;
                }
                for (const auto & required_column : required_columns)
                {
                    const UInt64 missing_rows = countRowsMissingColumn(future_part.parts, part_source_name(required_column));
                    if (missing_rows == 0)
                        continue;
                    const auto column = columns_description.tryGetColumn(GetColumnsOptions::AllPhysical, required_column);
                    if (column && column->type->haveMaximumSizeOfValue())
                        projection_uncompressed_bytes += missing_rows * column->type->getMaximumSizeOfValueInMemory();
                }

                const auto temp_projection_format = future_part.parts.front()->storage.choosePartFormat(
                    projection_uncompressed_bytes, projection_rows, future_part.part_info.level, &projection);
                const bool temp_projection_is_compact = temp_projection_format.part_type == MergeTreeDataPartType::Compact;
                const size_t projection_streams = temp_projection_is_compact ? 1 : projection_wide_stream_counts.total;

                /// The rebuild writes the projected rows twice (the temporary parts, then the read-back merge
                /// into the final projection part), so its data-dependent buffers are bounded by twice the
                /// projected volume with the same 3x in-flight allowance as the base output, plus the
                /// eager per-stream write buffers every writer allocates regardless of volume. A projection
                /// expression is not size-monotone (repeat(...), JSON / array construction can expand the
                /// bytes per row, an aggregate projection can materialize states larger than the raw input),
                /// so the source-derived projected volume can undershoot for a data-expanding projection; that
                /// growth is real data the reactive background_memory_tracker sees as it materializes. The
                /// alternative - pricing every rebuilt-projection stream at the full multipart ceiling - is a
                /// proven CI starvation regression: a TTL merge of a 2-row table with one projection was
                /// priced at 200.09 GiB (two Wide temp-part streams at the ~100 GiB default S3 ceiling) and
                /// rejected by the admission gate for the whole 300 s window of
                /// 03365_column_ttl_should_rebuild_skp_idx_and_proj, because concurrent merges always held
                /// some reservation, so the column TTL never took effect.
                ///
                /// The read-back stage (MergeProjectionPartsTask) does not merge the temporary parts one at a
                /// time: it merges up to max_parts_to_merge_in_one_level of them in a single nested merge, so
                /// it can hold that many reader-buffer sets open at once. A rebuild that squashes into more
                /// than one temporary part therefore reads back through several readers simultaneously; size
                /// the read-back for that worst case rather than a single reader set.
                /// The temporary parts are read back from the destination disk, whose cache state is not
                /// resolvable here (only whether it is remote), so price a remote read-back at the
                /// cache-promoted buffer size - the same size the readers get on a cache-backed disk, and
                /// identical to the plain one under default settings.
                const UInt64 projection_read_buffer_size
                    = output_on_remote_disk ? cached_remote_read_buffer_size : local_read_buffer_size;
                const UInt64 projection_worst_case = saturatingStreamsTimesBuffer(
                    projection_streams, worst_case_write_buffer_size(projection_local_write_buffer_size));
                /// A Wide temp part is written by the same wide writer as the base output, so its
                /// eager per-stream buffers follow the same per-stream adaptive split (the count-based rule
                /// sees the temp-part writer's own columns list - the projection's columns); a Compact
                /// temp part's single shared stream is non-adaptive.
                const UInt64 projection_eager_write_buffers = temp_projection_is_compact
                    ? projection_local_write_buffer_size
                    : eager_write_buffers(
                          projection_wide_stream_counts, projection.sample_block.columns(), projection_local_write_buffer_size);
                const UInt64 projection_data_bound = projection_eager_write_buffers
                    + 3 * 2 * projection_uncompressed_bytes;
                projection_memory += std::min(projection_worst_case, projection_data_bound)
                    + MergeProjectionPartsTask::max_parts_to_merge_in_one_level * projection_streams * projection_read_buffer_size;
            }
        }
    }

    return input_memory + output_memory + projection_memory;
}

DiskWriteBufferMemory getDiskWriteBufferMemory(const DiskPtr & disk)
{
    /// Unwrap decorator disks (encrypted, read-only, ...) down to the disk they delegate to: they forward
    /// object-storage writes to the wrapped disk (see DiskEncrypted::getObjectStorage), so a wrapped
    /// S3 / Azure disk allocates the same multipart upload buffers as a bare one and its sizing must come
    /// from the same request settings. Only a real object-storage disk exposes settings-dependent write
    /// buffer sizes; for everything else - a plain local disk, or a remote disk such as HDFS whose writer
    /// has no multipart upload buffers - return zeroes, which the estimator takes as "use the local
    /// per-stream estimate" (dynamic_cast avoids the exception that IDisk::getObjectStorage throws for
    /// disks that do not support object storage).
    for (DiskPtr current = disk; current; current = current->getDelegateDiskIfExists())
    {
        if (auto * object_storage_disk = dynamic_cast<DiskObjectStorage *>(current.get()))
        {
            const auto object_storage = object_storage_disk->getObjectStorage();
            return DiskWriteBufferMemory{.ceiling = object_storage->getWriteBufferMemoryCeiling()};
        }
    }
    return {};
}

UInt64 estimateAtLeastAvailableSpace(const PartsRange & range)
{
    size_t bytes_size = 0;

    for (const auto & part : range)
        bytes_size += part.size;

    return static_cast<UInt64>(static_cast<double>(bytes_size) * DISK_USAGE_COEFFICIENT_TO_SELECT);
}

UInt64 getMaxSourcePartsBytesForMerge(const MergeTreeData & data)
{
    size_t scheduled_tasks_count = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    auto max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();
    return getMaxSourcePartsBytesForMerge(data, max_tasks_count, scheduled_tasks_count);
}

UInt64 getMaxSourcePartsBytesForMerge(const MergeTreeData & data, size_t max_count, size_t scheduled_tasks_count)
{
    const auto data_settings = data.getSettings();
    return getMaxSourcePartsBytesForMerge(
        /*max_count=*/max_count,
        /*scheduled_tasks_count=*/scheduled_tasks_count,
        /*max_unreserved_free_space*/data.getStoragePolicy()->getMaxUnreservedFreeSpace(),
        /*size_lowering_threshold=*/(*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_lower_max_size_of_merge],
        /*size_limit_at_min_pool_space=*/(*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_min_space_in_pool],
        /*size_limit_at_max_pool_space=*/(*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool]);
}

UInt64 getMaxSourcePartsBytesForMerge(
    size_t max_count,
    size_t scheduled_tasks_count,
    size_t max_unreserved_free_space,
    size_t size_lowering_threshold,
    size_t size_limit_at_min_pool_space,
    size_t size_limit_at_max_pool_space)
{
    if (scheduled_tasks_count > max_count)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid argument passed to getMaxSourcePartsSize: scheduled_tasks_count = {} > max_count = {}",
            scheduled_tasks_count, max_count);
    }

    if (size_limit_at_max_pool_space == 0)
        return 0;

    size_limit_at_min_pool_space = std::min(size_limit_at_min_pool_space, size_limit_at_max_pool_space);
    size_t free_entries = max_count - scheduled_tasks_count;

    /// Always allow maximum size if one or less pool entries is busy.
    /// One entry is probably the entry where this function is executed.
    /// This will protect from bad settings.
    UInt64 max_size = 0;
    if (scheduled_tasks_count <= 1 || free_entries >= size_lowering_threshold)
    {
        max_size = size_limit_at_max_pool_space;
    }
    else
    {
        /// interpolation only possible if 0 < min <= max.
        size_limit_at_min_pool_space = std::max<size_t>(1, size_limit_at_min_pool_space);

        max_size = static_cast<UInt64>(interpolateExponential(
            static_cast<double>(size_limit_at_min_pool_space),
            static_cast<double>(size_limit_at_max_pool_space),
            static_cast<double>(free_entries) / static_cast<double>(size_lowering_threshold)));
    }

    return std::min(max_size, static_cast<UInt64>(static_cast<double>(max_unreserved_free_space) / DISK_USAGE_COEFFICIENT_TO_SELECT));
}

UInt64 getMaxSourcePartBytesForMutation(const MergeTreeData & data, String * out_log_comment)
{
    const auto data_settings = data.getSettings();
    Int64 occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    Int64 max_number_of_mutations_for_replica = (*data_settings)[MergeTreeSetting::max_number_of_mutations_for_replica];
    if (max_number_of_mutations_for_replica > 0 && occupied >= max_number_of_mutations_for_replica)
    {
        if (out_log_comment)
            *out_log_comment = fmt::format("occupied ({}) >= max_number_of_mutations_for_replica ({})", occupied, max_number_of_mutations_for_replica);

        return 0;
    }

    /// A DataPart can be stored only at a single disk. Get the maximum reservable free space at all disks.
    UInt64 disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();
    Int64 max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();

    /// Allow mutations only if there are enough threads, otherwise, leave free threads for merges.
    Int64 number_of_free_entries_in_pool_to_execute_mutation = (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_execute_mutation];
    if (occupied <= 1 || max_tasks_count - occupied >= number_of_free_entries_in_pool_to_execute_mutation)
        return static_cast<UInt64>(static_cast<double>(disk_space) / DISK_USAGE_COEFFICIENT_TO_RESERVE);

    if (out_log_comment)
        *out_log_comment = fmt::format("max_tasks_count ({}) - occupied ({}) < number_of_free_entries_in_pool_to_execute_mutation ({})", max_tasks_count, occupied, number_of_free_entries_in_pool_to_execute_mutation);

    return 0;
}

UInt64 getMaxResultPartRowsCount(const MergeTreeData & data)
{
    auto metadata_snapshot = data.getInMemoryMetadataPtr(data.getContext(), false);
    const auto & secondary_indices = metadata_snapshot->getSecondaryIndices();
    /// Text index and vector similarity indexes don't support UInt64 indexes of rows.
    bool has_index_with_limit_on_rows = secondary_indices.hasType("text") || secondary_indices.hasType("vector_similarity");
    return has_index_with_limit_on_rows ? std::numeric_limits<UInt32>::max() : std::numeric_limits<UInt64>::max();
}

UInt64 estimateResultPartRowsCount(const PartsRange & parts)
{
    size_t total_rows = 0;
    for (const auto & part : parts)
        total_rows += part.rows;
    return total_rows;
}

}

}
