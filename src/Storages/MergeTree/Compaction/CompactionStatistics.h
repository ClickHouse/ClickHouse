#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct FutureMergedMutatedPart;
struct MergeTreeSettings;

namespace CompactionStatistics
{

/** Estimate approximate amount of disk space needed for merge or mutation. With a surplus.
  */
UInt64 estimateNeededDiskSpace(const MergeTreeDataPartsVector & source_parts, const bool & account_for_deleted = false);

/** Estimate the amount of memory used by the input and output IO buffers of a merge:
  *   (number of input column streams over all source parts) * read IO buffer size
  * + (number of output column streams of the result part)   * write IO buffer size.
  * "Source parts" here includes future_part.patch_parts as well as future_part.parts: when
  * apply_patches_on_merge applies a patch part during the merge, MergeTreeReadTask::createReaders opens a
  * genuine reader (and IO buffers) for it too, and its columns count towards the output substream estimate
  * exactly like any other source part.
  * The number of on-disk streams of a wide part is taken from its actual substream layout
  * (columns_substreams.txt), so that dynamic substreams of JSON / Dynamic columns are counted correctly
  * instead of being collapsed to a single stream by the default serialization.
  * Multipart object storage (S3 / Azure) write buffers are large and double-buffered, so they are
  * accounted separately, controlled by remote_write_buffer_ceiling:
  *   - a positive value is the known per-stream ceiling of the destination disk (see
  *     getDiskWriteBufferMemoryCeiling - background writes take their sizes from the disk configuration,
  *     not from the query/session settings);
  *   - zero means the destination disk is known and has no multipart upload buffers (a local disk, or a
  *     remote disk such as HDFS whose writer uses a normal buffer), so the local per-stream estimate
  *     applies even when output_on_remote_disk is true;
  *   - nullopt means the destination disk is not chosen yet, so if output_on_remote_disk is true the
  *     worst-case ceiling over the S3 / Azure upload settings of the context is used as a
  *     pre-disk-selection guess.
  * Since upload buffers only ever hold data that has already flown through them, their contribution is
  * capped by the data volume of the merge (see the implementation for details).
  * Projection IO is included as well: a projection whose parts are present in every source part is merged
  * by a nested MergeTask over those parts, priced by applying this same estimate recursively, and a
  * projection the merge rebuilds from scratch (a commit-order projection, or
  * materialize_projections_on_merge) is priced as one set of temp-part writer streams plus the read-back
  * of the temporary parts, both bounded by the merge's input data volume.
  * A merge reserves this amount up front (see MergeMemoryReservation) so that many merges starting
  * at once - for example right after a mutation - do not all grow their buffers and oversubscribe memory.
  */
UInt64 estimateNeededMemoryForMerge(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const MergeTreeSettings & settings,
    bool output_on_remote_disk,
    std::optional<UInt64> remote_write_buffer_ceiling = std::nullopt);

/** The per-stream multipart write buffer memory ceiling of a merge's destination disk, or 0 for disks
  * whose writer has no multipart upload buffers (a plain local disk, or a remote disk such as HDFS that
  * writes through a normal buffer). Decorator disks (encrypted, read-only) are unwrapped down to the disk
  * they delegate to, so a wrapped S3 / Azure disk reports the same ceiling as a bare one. Pass the result
  * into estimateNeededMemoryForMerge as remote_write_buffer_ceiling once the destination disk is known, so
  * the reservation reflects the disk's own multipart upload sizes rather than the query/session settings
  * that a background writer ignores.
  */
UInt64 getDiskWriteBufferMemoryCeiling(const DiskPtr & disk);

/** Estimate approximate amount of disk space needed to be free before schedule such merge.
  */
UInt64 estimateAtLeastAvailableSpace(const PartsRange & range);

/** Get maximum total size of parts to do merge, at current moment of time.
  * It depends on number of free threads in background_pool and amount of free space in disk.
  */
UInt64 getMaxSourcePartsBytesForMerge(const MergeTreeData & data);

/** For explicitly passed size of pool and number of used tasks.
  * This method could be used to calculate threshold depending on number of tasks in replication queue.
  */
UInt64 getMaxSourcePartsBytesForMerge(const MergeTreeData & data, size_t max_count, size_t scheduled_tasks_count);

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
