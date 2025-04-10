#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>

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

    return static_cast<UInt64>(bytes_size * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

UInt64 estimateAtLeastAvailableSpace(const PartsRange & range)
{
    size_t bytes_size = 0;

    for (const auto & part : range)
        bytes_size += part.size;

    return static_cast<UInt64>(bytes_size * DISK_USAGE_COEFFICIENT_TO_SELECT);
}

UInt64 getMaxSourcePartsSizeForMerge(const MergeTreeData & data)
{
    size_t scheduled_tasks_count = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    auto max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();
    return getMaxSourcePartsSizeForMerge(data, max_tasks_count, scheduled_tasks_count);
}


UInt64 getMaxSourcePartsSizeForMerge(const MergeTreeData & data, size_t max_count, size_t scheduled_tasks_count)
{
    if (scheduled_tasks_count > max_count)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid argument passed to getMaxSourcePartsSize: scheduled_tasks_count = {} > max_count = {}",
            scheduled_tasks_count, max_count);
    }

    size_t free_entries = max_count - scheduled_tasks_count;
    const auto data_settings = data.getSettings();

    /// Always allow maximum size if one or less pool entries is busy.
    /// One entry is probably the entry where this function is executed.
    /// This will protect from bad settings.
    UInt64 max_size = 0;
    if (scheduled_tasks_count <= 1 || free_entries >= (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_lower_max_size_of_merge])
        max_size = (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool];
    else
        max_size = static_cast<UInt64>(interpolateExponential(
            (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_min_space_in_pool],
            (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool],
            static_cast<double>(free_entries) / (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_lower_max_size_of_merge]));

    return std::min(max_size, static_cast<UInt64>(data.getStoragePolicy()->getMaxUnreservedFreeSpace() / DISK_USAGE_COEFFICIENT_TO_SELECT));
}


UInt64 getMaxSourcePartSizeForMutation(const MergeTreeData & data)
{
    const auto data_settings = data.getSettings();
    size_t occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    if ((*data_settings)[MergeTreeSetting::max_number_of_mutations_for_replica] > 0 &&
        occupied >= (*data_settings)[MergeTreeSetting::max_number_of_mutations_for_replica])
        return 0;

    /// A DataPart can be stored only at a single disk. Get the maximum reservable free space at all disks.
    UInt64 disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();
    auto max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();

    /// Allow mutations only if there are enough threads, otherwise, leave free threads for merges.
    if (occupied <= 1
        || max_tasks_count - occupied >= (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_execute_mutation])
        return static_cast<UInt64>(disk_space / DISK_USAGE_COEFFICIENT_TO_RESERVE);

    return 0;
}

}

}
