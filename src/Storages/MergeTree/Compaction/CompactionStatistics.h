#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

namespace CompactionStatistics
{

/** Estimate approximate amount of disk space needed for merge or mutation. With a surplus.
  * `current_time` is the moment expired parts are recognized at; 0 means the local clock.
  */
UInt64 estimateNeededDiskSpace(
    const MergeTreeDataPartsVector & source_parts, const bool & account_for_deleted = false, time_t current_time = 0);

/** Estimate approximate amount of disk space needed to be free before schedule such merge.
  */
UInt64 estimateAtLeastAvailableSpace(const PartsRange & range);

/** Get maximum total size of parts to do merge, at current moment of time.
  * It depends on number of free threads in background_pool and amount of free space in disk.
  * `respect_min_unreserved_space` keeps the min_unreserved_disk_space_for_merge headroom out of the limit.
  */
UInt64 getMaxSourcePartsBytesForMerge(const MergeTreeData & data, bool respect_min_unreserved_space = true);

/** For explicitly passed size of pool and number of used tasks.
  * This method could be used to calculate threshold depending on number of tasks in replication queue.
  */
UInt64 getMaxSourcePartsBytesForMerge(
    const MergeTreeData & data, size_t max_count, size_t scheduled_tasks_count, bool respect_min_unreserved_space = true);

/** Same as above but with settings specification.
  */
UInt64 getMaxSourcePartsBytesForMerge(
    size_t max_count,
    size_t scheduled_tasks_count,
    size_t max_unreserved_free_space,
    size_t size_lowering_threshold,
    size_t size_limit_at_min_pool_space,
    size_t size_limit_at_max_pool_space);

/** Get maximum total size of parts to do mutation, at current moment of time.
  * It depends only on amount of free space in disk.
  */
UInt64 getMaxSourcePartBytesForMutation(const MergeTreeData & data, String * out_log_comment = nullptr);

/** Get the maximum number of parts to merge at once that fits the memory limit of this server, to be passed
  * to the merge selector as `MergeConstraint::max_parts`; `MergeConstraint::unlimited_parts` means no cap.
  *
  * A merge keeps one block from every source part alive at the same time, and every column of every one
  * of those blocks costs at least a minimum allocation, even when the block holds a handful of rows. That
  * fixed cost is proportional to `source parts * columns` and does not shrink with the amount of data:
  * merging 100 parts of `system.metric_log`, which has over two thousand columns, takes about 700 MiB
  * before a single row is read.
  *
  * On a server with little memory such a merge fails with `MEMORY_LIMIT_EXCEEDED`, its source parts stay
  * where they were, and the next selection round picks the same parts again - the table stops compacting
  * while its part count keeps growing. The returned cap keeps the fixed cost of one merge within a
  * sixteenth of the memory limit. Ordinary tables, and even very wide tables on a large server, get a cap
  * above the configured `max_parts_to_merge_at_once`, which then has no effect.
  *
  * Every merge is priced as a horizontal one, which holds all columns of every source part at once: a
  * vertical merge holds only the key columns, so it is over-priced, which costs some merge width but never
  * the merge. The columns of the widest projection count as well, because its projection parts are merged
  * with the same number of sources.
  *
  * Controlled by the `merge_memory_estimate_per_source_part_column` setting.
  */
size_t getMaxPartsToMergeAtOnceForMemory(const MergeTreeData & data);

/** Same as above, for an explicitly passed memory limit, number of columns, and estimate.
  */
size_t getMaxPartsToMergeAtOnceForMemory(Int64 memory_limit, size_t num_columns, size_t bytes_per_source_part_column);

/** Returns maximal allowed number of rows in part for the storage.
  * If storage has text or vector similarity indexes, the number of rows in part cannot exceed 2^32.
  */
UInt64 getMaxResultPartRowsCount(const MergeTreeData & data);

/** Returns upper bound on number of rows in result part after merge.
  * Actual rows count may be less than this value for special MergeTree storages.
 */
UInt64 estimateResultPartRowsCount(const PartsRange & parts);

};

}
