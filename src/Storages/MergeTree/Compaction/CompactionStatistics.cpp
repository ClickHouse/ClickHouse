#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/S3Defines.h>
#include <Core/Defines.h>
#include <Core/Settings.h>

#include <base/interpolate.h>

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

/// Number of on-disk column data files (.bin) a wide part physically stores: one per non-ephemeral
/// substream. This is the ground truth for the stream count of an old part that predates
/// columns_substreams.txt and, unlike the default serialization, it accounts for the dynamic substreams
/// of JSON / Dynamic columns (which are written as separate files on disk but cannot be enumerated from
/// the default serialization).
size_t countWidePartDataFiles(const IMergeTreeDataPart & part)
{
    size_t data_files = 0;
    for (const auto & [file_name, _] : part.checksums.files)
        if (file_name.ends_with(IMergeTreeDataPart::DATA_FILE_EXTENSION))
            ++data_files;
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
        if (const size_t data_files = countWidePartDataFiles(part); data_files != 0)
            return data_files;
    return countColumnStreams(part.getColumns());
}

/// Number of on-disk column streams the merged wide part will write. Its substream set is the union of
/// the source parts' substreams: for JSON / Dynamic columns the merged dynamic structure is chosen from
/// all source columns (ColumnObject::chooseDynamicStructureForMerge / ColumnDynamic::chooseDynamicStructureForMerge),
/// so paths or variants that appear in only some of the source parts all end up in the result part. A plain
/// max over the source parts would undercount exactly that case (part A has only path 'a', part B only 'b',
/// yet the merged part writes both). Count the union of substream names per column from each source part's
/// columns_substreams.txt (the reliable source of truth for dynamic substreams), falling back to the default
/// serialization for columns whose substreams are not recorded (parts written before that file existed, or a
/// column absent from every source part). The union is an upper bound on the real count - the merge may
/// collapse some dynamic substreams via max_dynamic_paths / max_dynamic_types - which is the safe direction
/// for a reservation. For simple column types the union equals the default serialization count, so this only
/// ever raises the estimate for semi-structured columns.
///
/// The per-column union can still miss dynamic substreams that live only in an old source part without
/// columns_substreams.txt (there the default serialization collapses JSON / Dynamic to a single stream).
/// Guard against that by flooring the result at the widest source part's actual stream count: the merged
/// part is never narrower than any single source part, and countPartStreams reads an old wide part's real
/// stream count from its on-disk .bin files. For simple columns and modern parts this floor equals the
/// union count, so it never raises the estimate above what the union already accounts for.
size_t countOutputStreams(const NamesAndTypesList & output_columns, const MergeTreeDataPartsVector & source_parts)
{
    size_t streams = 0;
    for (const auto & column : output_columns)
    {
        std::unordered_set<std::string_view> union_substreams;
        bool recorded = false;
        for (const auto & part : source_parts)
        {
            if (const auto * substreams = part->getColumnsSubstreams().tryGetColumnSubstreams(column.name))
            {
                recorded = true;
                for (const auto & substream : *substreams)
                    union_substreams.insert(substream);
            }
        }

        streams += recorded ? union_substreams.size() : countColumnStreams({column});
    }

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
    bool output_on_remote_disk)
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

    /// Per-stream write buffer size on object storage (S3 / Azure). A stream's upload buffers follow the
    /// multipart buffer allocation policy (see BufferAllocationPolicy / WriteBufferFromS3 /
    /// WriteBufferFromAzureBlobStorage): the first buffer is max(*_max_single_part_upload_size,
    /// *_min_upload_part_size) (ExpBufferAllocationPolicy::first_size), later buffers grow up to
    /// *_max_upload_part_size, and up to *_max_inflight_parts_for_one_file of them can be held in memory at
    /// once while their uploads are in flight. Take that worst-case per-stream ceiling from the effective
    /// settings over both back ends (a given disk is only one of them, so the max is a safe upper bound), so
    /// a deployment that raises the multipart sizes cannot allocate more per stream than is reserved here -
    /// pinning the first buffer to *_max_single_part_upload_size alone would underestimate it when
    /// *_min_upload_part_size is the larger of the two. This is only a ceiling: the output side is separately
    /// capped by the merge's data volume below, because an upload buffer never holds more than the data
    /// written into it.
    const auto & query_settings = context->getSettingsRef();
    auto remote_stream_ceiling = [](UInt64 max_single, UInt64 min_upload, UInt64 max_upload, UInt64 max_inflight) -> UInt64
    {
        return std::max(max_single, min_upload) + max_inflight * max_upload;
    };
    const UInt64 remote_write_buffer_size = std::max(
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

    /// Input side: one reader stream per column substream of every source part. The reader buffers hold
    /// a window of the compressed file plus the decompressed block, so they can never hold more than the
    /// part's own data: cap the per-part estimate by the part size.
    UInt64 input_memory = 0;
    UInt64 sum_input_bytes_compressed = 0;
    UInt64 sum_input_bytes_uncompressed = 0;
    for (const auto & part : future_part.parts)
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
    /// paths that all appear in the merged part.
    const auto output_columns = metadata_snapshot->getColumns().getAllPhysical();
    const size_t output_streams = future_part.part_format.part_type == MergeTreeDataPartType::Wide
        ? countOutputStreams(output_columns, future_part.parts)
        : 1;

    /// Worst case: every stream allocates all of its buffers in full.
    const UInt64 write_buffer_size = output_on_remote_disk ? remote_write_buffer_size : local_write_buffer_size;
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

    return input_memory + output_memory;
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
