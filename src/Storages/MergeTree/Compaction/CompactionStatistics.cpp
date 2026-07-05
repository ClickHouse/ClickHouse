#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/S3Defines.h>
#include <Core/Defines.h>

#include <base/interpolate.h>

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
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_max_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_min_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_compress_block_size;
    extern const MergeTreeSettingsUInt64 max_number_of_mutations_for_replica;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_execute_mutation;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_lower_max_size_of_merge;
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

}

UInt64 estimateNeededMemoryForMerge(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettings & settings,
    bool output_on_remote_disk)
{
    /// Per-stream read buffer size. During a merge reads use the default read buffer size (which is later
    /// shrunk to the granule size), smaller for the local filesystem than for remote (object storage).
    static constexpr UInt64 local_read_buffer_size = 128 * 1024; /// max_read_buffer_size_local_fs default
    static constexpr UInt64 remote_read_buffer_size = DBMS_DEFAULT_BUFFER_SIZE; /// max_read_buffer_size default (1 MiB)

    /// Per-stream write buffer size on a local disk: a writer stream keeps the compressor block and the
    /// file buffer, both sized by max_compress_block_size.
    UInt64 max_compress_block_size = settings[MergeTreeSetting::max_compress_block_size];
    if (max_compress_block_size == 0)
        max_compress_block_size = DBMS_DEFAULT_BUFFER_SIZE;
    const UInt64 local_write_buffer_size = 2 * max_compress_block_size;

    /// Per-stream write buffer size on object storage (S3): upload parts are buffered whole in memory and
    /// there can be more than one part buffered per stream at a time due to background (double) buffering.
    const UInt64 remote_write_buffer_size = 2 * S3::DEFAULT_MAX_SINGLE_PART_UPLOAD_SIZE;

    /// Input side: one reader stream per column substream of every source part.
    UInt64 input_memory = 0;
    for (const auto & part : future_part.parts)
    {
        /// Compact and in-memory parts read all columns through a single shared stream.
        const size_t streams = part->getType() == MergeTreeDataPartType::Wide
            ? countColumnStreams(part->getColumns())
            : 1;
        const UInt64 read_buffer_size = part->isStoredOnRemoteDisk() ? remote_read_buffer_size : local_read_buffer_size;
        input_memory += streams * read_buffer_size;
    }

    /// Output side: one writer stream per column substream of the result part.
    const size_t output_streams = future_part.part_format.part_type == MergeTreeDataPartType::Wide
        ? countColumnStreams(metadata_snapshot->getColumns().getAllPhysical())
        : 1;
    const UInt64 write_buffer_size = output_on_remote_disk ? remote_write_buffer_size : local_write_buffer_size;
    const UInt64 output_memory = output_streams * write_buffer_size;

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
