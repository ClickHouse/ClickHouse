#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace CompactionStatistics
{

/** Estimate approximate amount of disk space needed for merge or mutation. With a surplus.
  */
UInt64 estimateNeededDiskSpace(const MergeTreeDataPartsVector & source_parts, const bool & account_for_deleted = false);

/** Estimate approximate amount of disk space needed to be free before schedule such merge.
  */
UInt64 estimateAtLeastAvailableSpace(const PartsRange & range);

/** Get maximum total size of parts to do merge, at current moment of time.
  * It depends on number of free threads in background_pool and amount of free space in disk.
  */
UInt64 getMaxSourcePartsSizeForMerge(const MergeTreeData & data);

/** For explicitly passed size of pool and number of used tasks.
  * This method could be used to calculate threshold depending on number of tasks in replication queue.
  */
UInt64 getMaxSourcePartsSizeForMerge(const MergeTreeData & data, size_t max_count, size_t scheduled_tasks_count);

/** Same as above but with settings specification.
  */
UInt64 getMaxSourcePartsSizeForMerge(
    size_t max_count,
    size_t scheduled_tasks_count,
    size_t max_unreserved_free_space,
    size_t size_lowering_threshold,
    size_t size_limit_at_min_pool_space,
    size_t size_limit_at_max_pool_space);

/** Get maximum total size of parts to do mutation, at current moment of time.
  * It depends only on amount of free space in disk.
  */
UInt64 getMaxSourcePartSizeForMutation(const MergeTreeData & data, String * out_log_comment = nullptr);

};

}
