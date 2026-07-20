#include <Interpreters/Context.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeProjectionPartsTask.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <IO/S3Defines.h>
#include <Core/Defines.h>
#include <Core/Settings.h>

#include <base/interpolate.h>

#include <algorithm>
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
    extern const MergeTreeSettingsDeduplicateMergeProjectionMode deduplicate_merge_projection_mode;
    extern const MergeTreeSettingsBool materialize_projections_on_merge;
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_max_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_min_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_compress_block_size;
    extern const MergeTreeSettingsUInt64 max_number_of_mutations_for_replica;
    extern const MergeTreeSettingsUInt64 min_columns_to_activate_adaptive_write_buffer;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_buckets_for_wide_part;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_execute_mutation;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_lower_max_size_of_merge;
}

namespace Setting
{
    extern const SettingsUInt64 s3_max_single_part_upload_size;
    extern const SettingsUInt64 s3_min_upload_part_size;
    extern const SettingsUInt64 s3_max_upload_part_size;
    extern const SettingsUInt64 s3_max_inflight_parts_for_one_file;
    extern const SettingsUInt64 azure_max_single_part_upload_size;
    extern const SettingsUInt64 azure_min_upload_part_size;
    extern const SettingsUInt64 azure_max_upload_part_size;
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

/// Worst-case number of on-disk streams a single materialized variant of a Dynamic value writes. A variant's
/// concrete type is runtime data, invisible in the declared Dynamic / JSON type, so it cannot be enumerated
/// statically: a scalar variant is one data stream, and the nested wrappers a value commonly carries -
/// Nullable, Array, Map, a small Tuple - add a null-map / offsets / element stream each. An arbitrarily wide
/// composite variant (for example CAST(tuple(<many columns>) AS Dynamic), or a wide tuple stored in a
/// Dynamic column) can write more than this and has no bound derivable from the declared type. This constant
/// backs the type-capacity fallback used wherever a column's real streams are not visible at selection time:
/// a synthesized rebuilt-projection column (no source data exists yet), a type-widened output column (priced
/// at max(capacity, the streams visible in the source parts), see countOutputStreams), and a
/// dynamic-structure column of a compact source part that records no substreams (a single data.bin, nothing
/// per-column to recover from disk). On those paths a wide composite variant can therefore still be
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
/// priced by their nested semi-structured components. Zero for types without dynamic structure.
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
            return dynamic_capacity(dynamic->getMaxDynamicTypes());
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
/// nullopt when no source part records this name with a matching type, so the caller falls back to the
/// type's own write-time capacity. Also returns nullopt as soon as any same-name source part stores a
/// DIFFERENT type than the projection output (a capacity-changing ALTER: an old JSON part merged with a
/// newer JSON(val UInt32) one, or Dynamic parts of different max_types): the old part is reserialized under
/// the current metadata during the rebuild, but its dynamic paths are named for its own type and so are
/// invisible to the union over the same-type parts - the type-capacity fallback covers them safely. Also
/// returns nullopt if any matching source part is a legacy wide part
/// that records no substreams for the column (a pre-columns_substreams.txt upgrade path): its dynamic
/// (JSON / Dynamic) substreams are invisible to the union by name, so trusting only the newer parts' recorded
/// union would drop the legacy part's dynamic paths - exactly the mixed legacy/new undercount countOutputStreams
/// has to recover explicitly for base parts. The type-capacity fallback the caller then takes cannot be
/// exceeded by any rebuilt column, a safe over-estimate for that transient window.
///
/// The same bailout applies to a base column the merge materializes from its DEFAULT expression
/// (default_filled_dynamic_columns, see estimateNeededMemoryForMerge): after ALTER ... ADD COLUMN d JSON(...)
/// DEFAULT ..., parts that predate the ALTER do not store d at all, yet the projection rebuild runs on the
/// merged base rows AFTER IMergeTreeReader has filled and evaluated the missing defaults, so a rebuilt
/// SELECT d ... projection can write dynamic substreams that come only from the old, default-filled rows.
/// The recorded union over the parts that do store d cannot see those, so it must not be treated as exact.
std::optional<size_t> tryCountBareIdentifierProjectionSubstreams(
    const NameAndTypePair & column, const MergeTreeDataPartsVector & source_parts, const NameSet & default_filled_dynamic_columns)
{
    if (default_filled_dynamic_columns.contains(column.name))
        return std::nullopt;

    std::unordered_set<std::string_view> union_substreams;
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
            return std::nullopt;
        recorded = true;
        for (const auto & substream : *substreams)
            union_substreams.insert(substream);
    }

    if (!recorded)
        return std::nullopt;
    return union_substreams.size();
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
/// so no bound taken from the source parts' dynamic substreams is sound. Bound such a column by its type's
/// own write-time capacity instead (countDynamicCapacityStreams), which no written column can exceed. For
/// simple projection columns this equals the default serialization count.
size_t countRebuiltProjectionStreams(
    const NamesAndTypesList & projection_columns,
    const MergeTreeDataPartsVector & source_parts,
    const MergeTreeSettings & settings,
    const NameSet & default_filled_dynamic_columns)
{
    size_t streams = 0;
    for (const auto & column : projection_columns)
    {
        if (auto recorded = tryCountBareIdentifierProjectionSubstreams(column, source_parts, default_filled_dynamic_columns))
            streams += *recorded;
        else
            streams += countColumnStreams({column}) + countDynamicCapacityStreams(*column.type, settings);
    }
    return streams;
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
size_t countOutputStreams(
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
    /// the floor prices them again.
    size_t streams = 0;
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
            capacity_priced_columns.insert(column.name);
        else
            streams += tryCountColumnSubstreamsFromParts(column.name, source_parts).value_or(countColumnStreams({column}));
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
            std::vector<std::string> recoverable_escaped_columns;
            std::vector<std::pair<std::string, std::string>> capacity_priced_escaped_columns;
            for (const auto & column : part_columns)
            {
                if (!output_columns.contains(column.name))
                    continue;
                if (capacity_priced_columns.contains(column.name))
                    capacity_priced_escaped_columns.emplace_back(escapeForFileName(column.name), column.name);
                else
                    recoverable_escaped_columns.push_back(escapeForFileName(column.name));
            }

            for (const auto & file_name : collectWidePartDataFileNames(*part))
            {
                if (static_files.contains(file_name))
                    continue;
                if (std::any_of(
                        recoverable_escaped_columns.begin(), recoverable_escaped_columns.end(),
                        [&](const auto & escaped) { return streamFileBelongsToColumn(file_name, escaped); }))
                {
                    unrecorded_dynamic_files.insert(file_name);
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
    streams += unrecorded_dynamic_files.size() + compact_dynamic_streams;

    /// Price the capacity-priced (widened / default-filled) columns now that every stream visible for them in
    /// the source parts is known: the recorded substream union by name (tryCountColumnSubstreamsFromParts does
    /// not require type equality, so it sees the old, narrower type's recorded substreams too) plus the real
    /// dynamic .bin files recovered above from unrecorded legacy wide parts. The output type's write-time
    /// capacity prices a variant at a fixed worst case (STREAMS_PER_DYNAMIC_VARIANT), which a wide composite
    /// variant a source part already materialized can exceed - there the visible streams are the ground truth -
    /// while the capacity covers what the sources cannot show (paths and variants the wider output type
    /// materializes beyond what the narrower source type could record, and the substreams a DEFAULT expression
    /// materializes for the rows of parts that predate an ADD COLUMN, which no source part records at all).
    /// Taking the max never prices such a column below either bound and stays proportional to real data, so it
    /// cannot re-introduce the saturating over-reservation.
    for (const auto & column : output_columns)
    {
        if (!capacity_priced_columns.contains(column.name))
            continue;
        size_t visible_streams = tryCountColumnSubstreamsFromParts(column.name, source_parts).value_or(0);
        if (const auto it = capacity_priced_dynamic_files.find(column.name); it != capacity_priced_dynamic_files.end())
            visible_streams += it->second.size();
        streams += countColumnStreams({column}) + std::max(countDynamicCapacityStreams(*column.type, settings), visible_streams);
    }

    /// The merged wide part is never narrower than any single source part, so floor the estimate at the
    /// widest source part's actual stream count - but counting only the columns the merged part still writes,
    /// so a column an old part carries yet the current metadata dropped does not inflate the floor. For simple
    /// columns and modern parts this floor equals the per-column union, so it never raises the estimate above
    /// what the union already accounts for.
    size_t max_source_streams = 0;
    for (const auto & part : source_parts)
        max_source_streams = std::max(max_source_streams, countPartStreamsForColumns(*part, output_columns));

    return std::max(streams, max_source_streams);
}

}

UInt64 estimateNeededMemoryForMerge(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const MergeTreeSettings & settings,
    bool output_on_remote_disk,
    std::optional<UInt64> remote_write_buffer_ceiling,
    bool deduplicate,
    bool cleanup)
{
    /// Per-stream read buffer size, from the effective server settings (merges read through the global
    /// context). A read buffer is later shrunk to the granule size, and it is smaller for the local
    /// filesystem than for remote (object storage).
    const auto read_settings = context->getReadSettings();
    const UInt64 local_read_buffer_size = read_settings.local_fs_settings.buffer_size;
    const UInt64 remote_read_buffer_size = read_settings.remote_fs_settings.buffer_size;

    /// Per-stream write buffer size on a local disk: a writer stream keeps the compressor block and the
    /// file buffer, both sized by max_compress_block_size.
    UInt64 max_compress_block_size = settings[MergeTreeSetting::max_compress_block_size];
    if (max_compress_block_size == 0)
        max_compress_block_size = DBMS_DEFAULT_BUFFER_SIZE;
    const UInt64 local_write_buffer_size = 2 * max_compress_block_size;

    /// Per-stream write buffer size on multipart object storage (S3 / Azure). A stream's upload buffers
    /// follow the multipart buffer allocation policy (see BufferAllocationPolicy / WriteBufferFromS3 /
    /// WriteBufferFromAzureBlobStorage): the first buffer is max(*_max_single_part_upload_size,
    /// *_min_upload_part_size) (ExpBufferAllocationPolicy::first_size), later buffers grow up to
    /// *_max_upload_part_size, and up to *_max_inflight_parts_for_one_file of them can be held in memory at
    /// once while their uploads are in flight.
    ///
    /// Prefer the actual destination disk's ceiling (remote_write_buffer_ceiling from
    /// getDiskWriteBufferMemoryCeiling) when the caller knows the disk: a background merge's
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
    /// below, because an upload buffer never holds more than the data written into it.
    UInt64 remote_write_buffer_size = 0;
    if (remote_write_buffer_ceiling.has_value())
    {
        remote_write_buffer_size = *remote_write_buffer_ceiling;
    }
    else if (output_on_remote_disk)
    {
        auto remote_stream_ceiling = [](UInt64 max_single, UInt64 min_upload, UInt64 max_upload, UInt64 max_inflight) -> UInt64
        {
            return std::max(max_single, min_upload) + max_inflight * max_upload;
        };
        const auto & query_settings = context->getSettingsRef();
        remote_write_buffer_size = std::max(
            remote_stream_ceiling(
                std::max<UInt64>(S3::DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE, query_settings[Setting::s3_max_single_part_upload_size]),
                query_settings[Setting::s3_min_upload_part_size],
                query_settings[Setting::s3_max_upload_part_size],
                query_settings[Setting::s3_max_inflight_parts_for_one_file]),
            remote_stream_ceiling(
                std::max<UInt64>(S3::DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE, query_settings[Setting::azure_max_single_part_upload_size]),
                query_settings[Setting::azure_min_upload_part_size],
                query_settings[Setting::azure_max_upload_part_size],
                query_settings[Setting::azure_max_inflight_parts_for_one_file]));
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
    const auto output_columns = metadata_snapshot->getColumns().getAllPhysical();
    NamesAndTypesList input_columns = output_columns;
    input_columns.emplace_back(RowExistsColumn::name, RowExistsColumn::type);

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
            ? countPartStreamsForColumns(*part, input_columns)
            : 1;
        const UInt64 read_buffer_size = part->isStoredOnRemoteDisk() ? remote_read_buffer_size : local_read_buffer_size;
        const ColumnSize read_bytes = partReadBytes(*part, input_columns);
        input_memory += std::min<UInt64>(streams * read_buffer_size, read_bytes.data_compressed + read_bytes.data_uncompressed);
        sum_input_bytes_uncompressed += read_bytes.data_uncompressed;
    }
    for (const auto & part : future_part.patch_parts)
    {
        const size_t streams = part->getType() == MergeTreeDataPartType::Wide
            ? countPartStreams(*part)
            : 1;
        const UInt64 read_buffer_size = part->isStoredOnRemoteDisk() ? remote_read_buffer_size : local_read_buffer_size;
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
    /// A dynamic-structure (JSON / Dynamic) output column absent from some base part but with a default
    /// expression in the merged metadata is materialized from that default during the merge: MergeTask keeps
    /// such a column live (a missing column is expired only when it has no default), and IMergeTreeReader
    /// fills and evaluates the missing defaults for the rows of the parts that predate the
    /// ALTER ... ADD COLUMN ... DEFAULT. Those rows can carry real dynamic substreams that no source part
    /// records, so neither the recorded per-part substream union nor the legacy .bin recovery can see them -
    /// both countOutputStreams and the rebuilt-projection pricing must fall back to the output type's
    /// write-time capacity for such a column instead of trusting the union as exact. Only base parts decide
    /// missing-ness: a patch part physically stores just the columns it patches and its rows are applied as
    /// patches, never default-filled. A column with dynamic structure but no default, or one missing from no
    /// part, keeps the exact union pricing, so this is a no-op outside the ADD COLUMN ... DEFAULT upgrade
    /// window.
    NameSet default_filled_dynamic_columns;
    {
        const auto & columns_description = metadata_snapshot->getColumns();
        for (const auto & column : output_columns)
        {
            if (!column.type->hasDynamicStructure())
                continue;
            const bool missing_from_some_part = std::any_of(
                future_part.parts.begin(), future_part.parts.end(),
                [&](const auto & part) { return !part->getColumns().contains(column.name); });
            if (missing_from_some_part && columns_description.getDefault(column.name))
                default_filled_dynamic_columns.insert(column.name);
        }
    }

    const size_t output_streams = future_part.part_format.part_type == MergeTreeDataPartType::Wide
        ? countOutputStreams(output_columns, source_and_patch_parts, settings, default_filled_dynamic_columns)
        : 1;

    /// Worst case: every stream allocates all of its buffers in full. A zero remote_write_buffer_size
    /// means the output is not written through multipart upload buffers (a local disk, a known remote disk
    /// without them, or a local pre-disk-selection guess), so the local per-stream size applies.
    const UInt64 write_buffer_size = remote_write_buffer_size != 0 ? remote_write_buffer_size : local_write_buffer_size;
    const UInt64 output_worst_case = output_streams * write_buffer_size;

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
    /// columns actually read - is a sound upper bound on the produced compressed output. Cap the
    /// data-dependent buffers at the compressed output in flight twice over (double buffering of uploads)
    /// plus one uncompressed working block per stream, all bounded by the uncompressed input volume.
    /// Without this cap a merge of tiny parts in a table with many columns on object storage would reserve
    /// gigabytes it can never touch, and concurrent merges would saturate the soft limit and starve each
    /// other for no reason.
    const UInt64 min_columns_for_adaptive = settings[MergeTreeSetting::min_columns_to_activate_adaptive_write_buffer];
    const bool adaptive_write_buffer = min_columns_for_adaptive != 0 && output_columns.size() >= min_columns_for_adaptive;
    const UInt64 eager_buffers_per_stream = adaptive_write_buffer
        ? 2 * settings[MergeTreeSetting::adaptive_write_buffer_initial_size]
        : local_write_buffer_size;
    const UInt64 output_data_bound = output_streams * eager_buffers_per_stream
        + 3 * sum_input_bytes_uncompressed;

    const UInt64 output_memory = std::min(output_worst_case, output_data_bound);

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
        /// merge_may_reduce_rows exactly as the merge itself will compute it
        /// (MergeTask::ExecuteAndFinalizeHorizontalPart::prepare): a deduplicating or cleanup merge, a
        /// merge that removes expired TTL values, one that applies lightweight-delete masks or patch
        /// parts, or a non-Ordinary merging mode. All of these are knowable at selection time: deduplicate
        /// and cleanup come from the caller (an OPTIMIZE query or a replication log entry - background
        /// selection never sets them), the TTL state and delete masks from the source parts themselves.
        /// The merge evaluates the TTL threshold slightly later than this estimate and skips TTL removal
        /// while a ttl_merges_blocker is held; both can only make the merge rebuild fewer projections than
        /// priced here, which is the safe direction for a reservation.
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
            if (merged_ttl_infos.part_min_ttl && merged_ttl_infos.part_min_ttl <= std::time(nullptr))
                need_remove_expired_values = true;
        }

        const bool has_lightweight_delete = std::any_of(
            source_and_patch_parts.begin(), source_and_patch_parts.end(),
            [](const auto & part) { return part->hasLightweightDelete(); });

        const bool merge_may_reduce_rows = deduplicate
            || cleanup
            || need_remove_expired_values
            || has_lightweight_delete
            || !future_part.patch_parts.empty()
            || future_part.parts.front()->storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary;

        /// A storage column absent from every source part and without a default expression is expired: the
        /// merge marks it so (new_data_part->expired_columns) and rebuilds every projection that requires
        /// it, again before checking whether the source parts have the projection.
        NameSet columns_present_in_parts;
        for (const auto & part : future_part.parts)
            for (const auto & part_column : part->getColumns())
                columns_present_in_parts.insert(part_column.name);

        NameSet expired_columns;
        const auto & columns_description = metadata_snapshot->getColumns();
        for (const auto & storage_column : output_columns)
            if (!columns_present_in_parts.contains(storage_column.name) && !columns_description.getDefault(storage_column.name))
                expired_columns.insert(storage_column.name);

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

            MergeTreeData::DataPartsVector projection_parts;
            for (const auto & part : future_part.parts)
            {
                auto it = part->getProjectionParts().find(projection.name);
                if (it != part->getProjectionParts().end() && !it->second->is_broken)
                    projection_parts.push_back(it->second);
            }

            /// Decide, exactly as the merge will, whether this projection is merged from the existing
            /// projection parts, rebuilt from the merged rows, or dropped from the result.
            bool rebuild_projection = rebuild_regardless_of_presence;
            if (!rebuild_regardless_of_presence && projection_parts.size() != future_part.parts.size())
            {
                if (projection.with_block_number || settings[MergeTreeSetting::materialize_projections_on_merge])
                    rebuild_projection = true;
                else
                    continue; /// Dropped from the merged part - no IO.
            }

            if (!rebuild_projection)
            {
                FutureMergedMutatedPart projection_future_part;
                projection_future_part.assign(std::move(projection_parts), /*patch_parts_=*/ {}, &projection);
                projection_memory += estimateNeededMemoryForMerge(
                    projection_future_part, projection.metadata, context, settings, output_on_remote_disk, remote_write_buffer_ceiling);
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
                const size_t projection_wide_streams = countRebuiltProjectionStreams(
                    projection.sample_block.getNamesAndTypesList(), source_and_patch_parts, settings, default_filled_dynamic_columns);

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
                /// admitted), the safe direction.
                const auto projection_required_columns = projection.getRequiredColumns();
                UInt64 projection_uncompressed_bytes = 0;
                UInt64 projection_rows = 0;
                for (const auto & part : future_part.parts)
                {
                    projection_rows += part->rows_count;
                    UInt64 part_projection_bytes = 0;
                    for (const auto & required_column : projection_required_columns)
                        part_projection_bytes += part->getColumnSize(required_column).data_uncompressed;
                    projection_uncompressed_bytes
                        += part_projection_bytes != 0 ? part_projection_bytes : part->getBytesUncompressedOnDisk();
                }

                const auto temp_projection_format = future_part.parts.front()->storage.choosePartFormat(
                    projection_uncompressed_bytes, projection_rows, future_part.part_info.level, &projection);
                const size_t projection_streams
                    = temp_projection_format.part_type == MergeTreeDataPartType::Compact ? 1 : projection_wide_streams;

                /// Unlike the base output above, a rebuilt projection is NOT size-bounded by the merge input:
                /// a projection expression is not size-monotone (repeat(...), JSON / array construction can
                /// expand the bytes per row, an aggregate projection can materialize states larger than the raw
                /// input), so the uncompressed-input cap used for the base output is not a valid cap here and
                /// would let the writer's upload buffers and the read-back grow past the reservation.
                /// Reserve the per-stream worst case instead: a writer stream never holds more than
                /// write_buffer_size and a read-back stream never more than its read buffer, whatever the
                /// projected data volume. On a local disk write_buffer_size is a small per-stream constant; on
                /// object storage it is the full multipart ceiling, which a data-expanding projection can
                /// genuinely approach - a single such merge is always admitted (see MergeMemoryReservation),
                /// it only throttles concurrent merges while it holds the reservation.
                ///
                /// The read-back stage (MergeProjectionPartsTask) does not merge the temporary parts one at a
                /// time: it merges up to max_parts_to_merge_in_one_level of them in a single nested merge, so
                /// it can hold that many reader-buffer sets open at once. A rebuild that squashes into more
                /// than one temporary part therefore reads back through several readers simultaneously; size
                /// the read-back for that worst case rather than a single reader set.
                const UInt64 projection_read_buffer_size = output_on_remote_disk ? remote_read_buffer_size : local_read_buffer_size;
                projection_memory += projection_streams * write_buffer_size
                    + MergeProjectionPartsTask::max_parts_to_merge_in_one_level * projection_streams * projection_read_buffer_size;
            }
        }
    }

    return input_memory + output_memory + projection_memory;
}

UInt64 getDiskWriteBufferMemoryCeiling(const DiskPtr & disk)
{
    /// Unwrap decorator disks (encrypted, read-only, ...) down to the disk they delegate to: they forward
    /// object-storage writes to the wrapped disk (see DiskEncrypted::getObjectStorage), so a wrapped
    /// S3 / Azure disk allocates the same multipart upload buffers as a bare one and its ceiling must come
    /// from the same request settings. Only a real object-storage disk exposes a settings-dependent
    /// ceiling; for everything else - a plain local disk, or a remote disk such as HDFS whose writer has
    /// no multipart upload buffers - return 0, which the estimator takes as "use the local per-stream
    /// estimate" (dynamic_cast avoids the exception that IDisk::getObjectStorage throws for disks that do
    /// not support object storage).
    for (DiskPtr current = disk; current; current = current->getDelegateDiskIfExists())
        if (auto * object_storage_disk = dynamic_cast<DiskObjectStorage *>(current.get()))
            return object_storage_disk->getObjectStorage()->getWriteBufferMemoryCeiling();
    return 0;
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
