#include <Interpreters/Context.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
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

/// Number of on-disk column streams for a set of columns, recovering the dynamic (JSON / Dynamic) substreams
/// of each column, by name, from the source parts (tryCountColumnSubstreamsFromParts), falling back to the
/// default serialization for a column whose substreams no part records. For simple column types the union
/// equals the default serialization count, so this only ever raises the estimate for semi-structured columns.
/// Unlike countOutputStreams below it adds no whole-part floor, so it is exact for a column set that is narrower
/// than the parts it is matched against - a projection's columns are derived from the base parts, so its
/// semi-structured columns are priced against the base parts by name here.
size_t countColumnStreamsFromParts(const NamesAndTypesList & columns, const MergeTreeDataPartsVector & source_parts)
{
    size_t streams = 0;
    for (const auto & column : columns)
        streams += tryCountColumnSubstreamsFromParts(column.name, source_parts).value_or(countColumnStreams({column}));
    return streams;
}

/// Number of on-disk streams the temporary part of a REBUILT projection writes. A rebuild recalculates the
/// projection from the merged base rows, so a semi-structured (JSON / Dynamic) projection column carries the
/// dynamic substreams of the base data it is derived from, and writeTempProjectionPart writes one stream per
/// substream. When the projection output column shares its name with a base column (a bare-identifier
/// projection, SELECT json ORDER BY ...) it is priced precisely from that column's recorded substreams. But a
/// projection may materialize a semi-structured value through an expression under a name no base part records
/// (SELECT identity(json) ..., a CAST to JSON, ...); its real substream count cannot be enumerated from the
/// default serialization (which collapses it to one stream) nor traced to a base column by name. Bound such a
/// column by source_dynamic_substreams - the total dynamic substreams present in the source parts - on top of
/// its statically enumerable skeleton: a value recomputed from the merged rows cannot contain more dynamic
/// paths than the input holds. For simple projection columns this equals the default serialization count.
size_t countRebuiltProjectionStreams(
    const NamesAndTypesList & projection_columns, const MergeTreeDataPartsVector & source_parts, size_t source_dynamic_substreams)
{
    size_t streams = 0;
    for (const auto & column : projection_columns)
    {
        if (auto recorded = tryCountColumnSubstreamsFromParts(column.name, source_parts))
            streams += *recorded;
        else if (column.type->hasDynamicStructure())
            streams += countColumnStreams({column}) + source_dynamic_substreams;
        else
            streams += countColumnStreams({column});
    }
    return streams;
}

/// Number of on-disk column streams the merged wide part will write. Its substream set is the union of
/// the source parts' substreams (countColumnStreamsFromParts), matched by column name. The per-column union
/// can still miss dynamic substreams that live only in an old source part without columns_substreams.txt
/// (there the default serialization collapses JSON / Dynamic to a single stream, and tryGetColumnSubstreams
/// returns nothing). Those old parts' dynamic streams are added back explicitly below (see
/// unrecorded_dynamic_files), so a mixed old/new merge with disjoint dynamic paths is not undercounted.
/// The result is also floored at the widest source part's actual stream count (countPartStreams reads an old
/// wide part's real .bin count), since the merged part is never narrower than any single source part. For
/// simple columns and modern parts both adjustments are no-ops.
size_t countOutputStreams(const NamesAndTypesList & output_columns, const MergeTreeDataPartsVector & source_parts, const MergeTreeSettings & settings)
{
    size_t streams = countColumnStreamsFromParts(output_columns, source_parts);

    /// The per-column union above can only see substreams recorded in columns_substreams.txt. A source part
    /// written before that file existed (a pre-25.8 upgrade path) records nothing, so the dynamic substreams
    /// of its JSON / Dynamic columns are invisible to the union. When such an old part is merged with newer
    /// parts, its dynamic paths can be disjoint from theirs (old part has path 'a', new part has 'b', and the
    /// merged part writes both), so the union - which only saw the newer part's 'b' - undercounts the result.
    /// The whole-part max floor below does not close this: neither the old part nor the new part is on its own
    /// as wide as their union. Recover each old wide part's unrecorded dynamic files - its actual .bin file
    /// names minus the names accountable to its non-dynamic columns (collectStaticStreamFileNames; already
    /// covered by the union above) - and UNION them across parts, by name, rather than summing their counts.
    /// Two old parts that both physically store the same dynamic file (e.g. the same JSON path resolved to
    /// the same type) name it identically (ISerialization::getFileNameForStream depends only on column,
    /// path and resolved type, not on which part wrote it), and the merged part writes that stream only
    /// once; summing per-part counts would charge it once per part instead. Treating genuinely distinct
    /// dynamic files as disjoint from every other part is still the safe direction for a reservation. For
    /// parts written after columns_substreams.txt exists, and for merges of only simple columns, this adds
    /// nothing.
    const ISerialization::StreamFileNameSettings stream_file_name_settings(settings);
    std::unordered_set<std::string> unrecorded_dynamic_files;
    for (const auto & part : source_parts)
    {
        if (part->getType() != MergeTreeDataPartType::Wide || !part->getColumnsSubstreams().empty())
            continue;

        /// Only types with a dynamic structure (JSON, Dynamic, and composites containing them) have
        /// substreams that the default serialization cannot enumerate, so only such parts need the
        /// recovery at all. hasDynamicSubcolumns() would be too broad as the gate: a plain Map or Variant
        /// reports true there, yet its physical streams are fully enumerable from the default serialization.
        const auto & part_columns = part->getColumns();
        const bool has_dynamic_structure_column = std::any_of(
            part_columns.begin(), part_columns.end(),
            [](const auto & column) { return column.type->hasDynamicStructure(); });
        if (!has_dynamic_structure_column)
            continue;

        /// Subtract every file name the default serialization can enumerate from the part's actual on-disk
        /// file names. For a column without dynamic structure that is all of its files; for a column with
        /// dynamic structure it is the static skeleton of its layout - and composites keep a real one:
        /// Tuple(UInt64, JSON) still has the UInt64 element stream, Array(JSON) its offsets, JSON its
        /// shared-data streams. Both kinds are already counted once by the per-column union above, so
        /// subtracting only whole non-dynamic columns (as if a dynamic-structure column had no enumerable
        /// streams at all) would count that static skeleton twice and over-reserve upgrade-path merges.
        /// What remains after the subtraction is exactly the part's dynamic files, which nothing else
        /// accounts for.
        const auto static_files = collectStaticStreamFileNames(part_columns, stream_file_name_settings);
        for (const auto & file_name : collectWidePartDataFileNames(*part))
            if (!static_files.contains(file_name))
                unrecorded_dynamic_files.insert(file_name);
    }
    streams += unrecorded_dynamic_files.size();

    /// The merged wide part is never narrower than any single source part, so floor the estimate at the
    /// widest source part's actual stream count. For simple columns and modern parts this floor equals the
    /// per-column union, so it never raises the estimate above what the union already accounts for.
    size_t max_source_streams = 0;
    for (const auto & part : source_parts)
        max_source_streams = std::max(max_source_streams, countPartStreams(*part));

    return std::max(streams, max_source_streams);
}

}

UInt64 estimateNeededMemoryForMerge(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const MergeTreeSettings & settings,
    bool output_on_remote_disk,
    std::optional<UInt64> remote_write_buffer_ceiling)
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

    /// Input side: one reader stream per column substream of every source part. The reader buffers hold
    /// a window of the compressed file plus the decompressed block, so they can never hold more than the
    /// part's own data: cap the per-part estimate by the part size.
    UInt64 input_memory = 0;
    UInt64 sum_input_bytes_compressed = 0;
    UInt64 sum_input_bytes_uncompressed = 0;
    for (const auto & part : source_and_patch_parts)
    {
        /// Compact and in-memory parts read all columns through a single shared stream.
        const size_t streams = part->getType() == MergeTreeDataPartType::Wide
            ? countPartStreams(*part)
            : 1;
        const UInt64 read_buffer_size = part->isStoredOnRemoteDisk() ? remote_read_buffer_size : local_read_buffer_size;
        const UInt64 part_bytes = part->getBytesOnDisk() + part->getBytesUncompressedOnDisk();
        input_memory += std::min<UInt64>(streams * read_buffer_size, part_bytes);
        sum_input_bytes_compressed += part->getBytesOnDisk();
        sum_input_bytes_uncompressed += part->getBytesUncompressedOnDisk();
    }

    /// Output side: one writer stream per column substream of the result part. The result part is not
    /// written yet, so its dynamic substreams (JSON, Dynamic) are not known up front and the default
    /// serialization count would collapse such columns to a single stream. Estimate the result substreams
    /// as the union of the source parts' substreams (see countOutputStreams) - this both counts the actual
    /// dynamic substreams and covers the case where different source parts contribute disjoint dynamic
    /// paths that all appear in the merged part. Patch parts are included: a patched JSON / Dynamic column
    /// can carry a path that exists only in a patch, not in any base part.
    const auto output_columns = metadata_snapshot->getColumns().getAllPhysical();
    const size_t output_streams = future_part.part_format.part_type == MergeTreeDataPartType::Wide
        ? countOutputStreams(output_columns, source_and_patch_parts, settings)
        : 1;

    /// Worst case: every stream allocates all of its buffers in full. A zero remote_write_buffer_size
    /// means the output is not written through multipart upload buffers (a local disk, a known remote disk
    /// without them, or a local pre-disk-selection guess), so the local per-stream size applies.
    const UInt64 write_buffer_size = remote_write_buffer_size != 0 ? remote_write_buffer_size : local_write_buffer_size;
    const UInt64 output_worst_case = output_streams * write_buffer_size;

    /// However, only the compressor block and the file buffer are allocated eagerly (and they start at
    /// adaptive_write_buffer_initial_size when adaptive write buffers are active for this part). Object
    /// storage upload buffers - and the growth of adaptive buffers - only ever hold data that has already
    /// been written into them, so their total is bounded by the volume of data the merge writes, which
    /// cannot exceed the input data volume: at most the compressed output in flight twice over (due to
    /// double buffering of uploads) plus one uncompressed block per stream. Without this cap a merge of
    /// tiny parts in a table with many columns on object storage would reserve gigabytes it can never
    /// touch, and concurrent merges would saturate the soft limit and starve each other for no reason.
    const UInt64 min_columns_for_adaptive = settings[MergeTreeSetting::min_columns_to_activate_adaptive_write_buffer];
    const bool adaptive_write_buffer = min_columns_for_adaptive != 0 && output_columns.size() >= min_columns_for_adaptive;
    const UInt64 eager_buffers_per_stream = adaptive_write_buffer
        ? 2 * settings[MergeTreeSetting::adaptive_write_buffer_initial_size]
        : local_write_buffer_size;
    const UInt64 output_data_bound = output_streams * eager_buffers_per_stream
        + 2 * sum_input_bytes_compressed + sum_input_bytes_uncompressed;

    const UInt64 output_memory = std::min(output_worst_case, output_data_bound);

    /// Projections: the merge also reads and writes projection parts, and none of that IO flows through
    /// the base parts' readers and writers priced above. Mirror the decision made in
    /// MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild:
    ///  - a non-Ordinary merge (Replacing, Summing, ...) under the throw / drop
    ///    deduplicate_merge_projection_mode does not process projections at all;
    ///  - when every source part has the projection, the projection parts are merged by a nested
    ///    MergeTask over exactly those parts with the projection's own metadata
    ///    (MergeProjectionsStage::prepareProjections builds the very same FutureMergedMutatedPart), so
    ///    price that nested merge with this same estimate, recursively - a projection has no projections
    ///    of its own, so the recursion is one level deep;
    ///  - when some or all source parts lack the projection, the merge rebuilds it from the merged rows
    ///    only for commit-order projections (which are never written on insert) and under
    ///    materialize_projections_on_merge, and drops it from the result otherwise. A rebuild does not
    ///    read the existing projection parts: it recalculates the projection from rows already flowing
    ///    through the merge, writes temporary projection parts (one temp-part writer at a time per
    ///    projection, see writeTempProjectionPart) and then merges the temporary parts back
    ///    (MergeProjectionPartsTask), so price one set of writer streams plus the read-back of the
    ///    temporary parts, both bounded by the merge's input data volume - projected data cannot exceed
    ///    the data it is projected from.
    /// A row-reducing merge (deduplication, cleanup) rebuilds even fully-present projections instead of
    /// merging them; that is not knowable at selection time, and the nested-merge estimate over the
    /// existing projection parts is a fair proxy for such a rebuild's temp-part IO. For a table without
    /// projections all of this adds exactly nothing.
    UInt64 projection_memory = 0;
    const auto projection_mode = settings[MergeTreeSetting::deduplicate_merge_projection_mode];
    const bool merge_processes_projections = !future_part.parts.empty()
        && (future_part.parts.front()->storage.merging_params.mode == MergeTreeData::MergingParams::Ordinary
            || (projection_mode != DeduplicateMergeProjectionMode::THROW && projection_mode != DeduplicateMergeProjectionMode::DROP));
    if (merge_processes_projections)
    {
        /// Upper bound on the dynamic (JSON / Dynamic) substreams a rebuilt projection column can materialize
        /// when it is produced through an expression rather than a bare source identifier: the total dynamic
        /// substreams present in the source parts (their merged stream count minus the statically enumerable
        /// skeleton), since a value recomputed from the merged rows cannot hold more dynamic paths than the
        /// input does. Zero for a merge of only simple columns.
        const size_t source_static_streams = countColumnStreams(output_columns);
        const size_t source_all_streams = countOutputStreams(output_columns, source_and_patch_parts, settings);
        const size_t source_dynamic_substreams
            = source_all_streams > source_static_streams ? source_all_streams - source_static_streams : 0;

        for (const auto & projection : metadata_snapshot->getProjections())
        {
            MergeTreeData::DataPartsVector projection_parts;
            for (const auto & part : future_part.parts)
            {
                auto it = part->getProjectionParts().find(projection.name);
                if (it != part->getProjectionParts().end())
                    projection_parts.push_back(it->second);
            }

            if (projection_parts.size() == future_part.parts.size())
            {
                FutureMergedMutatedPart projection_future_part;
                projection_future_part.assign(std::move(projection_parts), /*patch_parts_=*/ {}, &projection);
                projection_memory += estimateNeededMemoryForMerge(
                    projection_future_part, projection.metadata, context, settings, output_on_remote_disk, remote_write_buffer_ceiling);
            }
            else if (projection.with_block_number || settings[MergeTreeSetting::materialize_projections_on_merge])
            {
                /// The temporary parts are written into the result part's own storage, so they share the
                /// destination disk's write buffer sizing and are read back from that same disk. The rebuilt
                /// projection is recalculated from the merged base rows, so a semi-structured (JSON / Dynamic)
                /// projection column carries the dynamic substreams of the base data it is derived from
                /// (writeTempProjectionPart writes one stream per substream); count them with
                /// countRebuiltProjectionStreams rather than the default serialization, which would collapse
                /// such a column to a single stream and undersize the reservation.
                const size_t projection_streams = countRebuiltProjectionStreams(
                    projection.sample_block.getNamesAndTypesList(), source_and_patch_parts, source_dynamic_substreams);

                /// Unlike the base output above, a rebuilt projection is NOT size-bounded by the merge input:
                /// a projection expression is not size-monotone (repeat(...), JSON / array construction can
                /// expand the bytes per row, an aggregate projection can materialize states larger than the raw
                /// input), so 2 * sum_input_bytes_compressed + sum_input_bytes_uncompressed is not a valid cap
                /// here and would let the writer's upload buffers and the read-back grow past the reservation.
                /// Reserve the per-stream worst case instead: a writer stream never holds more than
                /// write_buffer_size and a read-back stream never more than its read buffer, whatever the
                /// projected data volume. On a local disk write_buffer_size is a small per-stream constant; on
                /// object storage it is the full multipart ceiling, which a data-expanding projection can
                /// genuinely approach - a single such merge is always admitted (see MergeMemoryReservation),
                /// it only throttles concurrent merges while it holds the reservation.
                const UInt64 projection_read_buffer_size = output_on_remote_disk ? remote_read_buffer_size : local_read_buffer_size;
                projection_memory += projection_streams * write_buffer_size + projection_streams * projection_read_buffer_size;
            }
            /// Otherwise the projection is dropped from the merged part and costs no IO.
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
