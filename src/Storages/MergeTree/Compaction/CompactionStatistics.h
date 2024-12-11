#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Do not start to merge parts, if free space is less than sum size of parts times specified coefficient.
/// This value is chosen to not allow big merges to eat all free space. Thus allowing small merges to proceed.
static const double DISK_USAGE_COEFFICIENT_TO_SELECT = 2;

/// To do merge, reserve amount of space equals to sum size of parts times specified coefficient.
/// Must be strictly less than DISK_USAGE_COEFFICIENT_TO_SELECT,
/// because between selecting parts to merge and doing merge, amount of free space could have decreased.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

class CompactionStatistics
{
public:
    CompactionStatistics() = delete;

    /// Estimate approximate amount of disk space needed for merge or mutation. With a surplus.
    static UInt64 estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts, const bool & account_for_deleted = false);

    /** Get maximum total size of parts to do merge, at current moment of time.
      * It depends on number of free threads in background_pool and amount of free space in disk.
      */
    static UInt64 getMaxSourcePartsSizeForMerge(const MergeTreeData & data);

    /** For explicitly passed size of pool and number of used tasks.
      * This method could be used to calculate threshold depending on number of tasks in replication queue.
      */
    static UInt64 getMaxSourcePartsSizeForMerge(const MergeTreeData & data, size_t max_count, size_t scheduled_tasks_count);

    /** Get maximum total size of parts to do mutation, at current moment of time.
      * It depends only on amount of free space in disk.
      */
    static UInt64 getMaxSourcePartSizeForMutation(const MergeTreeData & data);
};

}
