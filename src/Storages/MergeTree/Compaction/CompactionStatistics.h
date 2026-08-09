#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct FutureMergedMutatedPart;
struct MergeTreeSettings;

namespace CompactionStatistics
{

/** Per-stream multipart write buffer memory of a merge's destination disk, for the up-front merge memory
  * reservation (see estimateNeededMemoryForMerge below). It comes from the disk's own request settings,
  * which a background writer uses instead of the query/session settings, and is 0 for disks whose writer
  * has no multipart upload buffers (a plain local disk, or a remote disk such as HDFS that writes through
  * a normal buffer).
  */
struct DiskWriteBufferMemory
{
    /// The most one stream's buffers can hold at once: the buffer being filled plus all in-flight upload
    /// parts, or MultipartUploadMemory::UNLIMITED when the disk allows unlimited in-flight parts. This is a
    /// ceiling only - a writer's buffer starts at the size its caller passes and grows toward it with the
    /// data written into it, so the estimate caps the output side by the merge's data volume as well.
    UInt64 ceiling = 0;
};

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
  * accounted separately, controlled by remote_write_buffer_memory:
  *   - a value with a positive ceiling is the known per-stream sizing of the destination disk (see
  *     getDiskWriteBufferMemory - background writes take their sizes from the disk configuration,
  *     not from the query/session settings);
  *   - a value with a zero ceiling means the destination disk is known and has no multipart upload
  *     buffers (a local disk, or a remote disk such as HDFS whose writer uses a normal buffer), so the
  *     local per-stream estimate applies even when output_on_remote_disk is true;
  *   - nullopt means the destination disk is not chosen yet, so if output_on_remote_disk is true the
  *     worst case over the S3 / Azure upload settings of the context is used as a
  *     pre-disk-selection guess.
  * Since upload buffers only ever hold data that has already flown through them, their contribution is
  * capped by the data volume of the merge (see the implementation for details).
  * Projection IO is included as well: a projection whose parts are present in every source part is merged
  * by a nested MergeTask over those parts, priced by applying this same estimate recursively, and a
  * projection the merge rebuilds from scratch (a row-reducing merge, a commit-order projection, or
  * materialize_projections_on_merge) is priced as one set of temp-part writer streams plus the read-back
  * of the temporary parts, at the per-stream worst case (a projection expression is not size-monotone,
  * so the merge's input data volume is not a valid cap there).
  * Pass deduplicate / cleanup when the merge was requested with them (OPTIMIZE ... DEDUPLICATE / CLEANUP,
  * or the corresponding flags of a replication log entry): together with the TTL state of the source
  * parts and their lightweight-delete masks they decide, exactly as the merge itself will
  * (merge_may_reduce_rows in MergeTask), whether projections are rebuilt from the merged rows even when
  * some source parts do not have them - a rebuild that would otherwise be priced as "dropped".
  * time_of_merge must be the same timestamp the merge itself will run with (MergeTask's time_of_merge:
  * the selection time stored in MergeMutateSelectedEntry for a non-replicated merge, entry.create_time
  * for a replicated one). The TTL trigger of merge_may_reduce_rows compares the source parts'
  * part_min_ttl against it; pricing against a different clock than the merge executes with would let a
  * merge selected just before a TTL boundary flip to the row-reducing TTL path after its reservation is
  * already fixed.
  * mutations_snapshot must be the pending-mutations snapshot for the source parts, built with the same
  * parameters MergeTask builds its own (see the call sites): a pending, not yet materialized
  * RENAME COLUMN old TO new is applied on-fly at read time, so the merge keeps `new` alive and reads the
  * source parts' `old` while writing `new` - the estimate mirrors that by probing the source parts under
  * the old name (see the pending-rename handling in the implementation). Pass nullptr when the source
  * parts can have no pending on-fly renames (the nested projection-merge recursion does: renaming a
  * column used in a projection is forbidden).
  * A merge reserves this amount up front (see MergeMemoryReservation) so that many merges starting
  * at once - for example right after a mutation - do not all grow their buffers and oversubscribe memory.
  */
UInt64 estimateNeededMemoryForMerge(
    const FutureMergedMutatedPart & future_part,
    const StorageMetadataPtr & metadata_snapshot,
    const ContextPtr & context,
    const MergeTreeSettings & settings,
    const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot,
    time_t time_of_merge,
    bool output_on_remote_disk,
    std::optional<DiskWriteBufferMemory> remote_write_buffer_memory = std::nullopt,
    bool deduplicate = false,
    bool cleanup = false);

/** The per-stream multipart write buffer memory sizing of a merge's destination disk (see
  * DiskWriteBufferMemory above); both values are 0 for disks whose writer has no multipart upload buffers
  * (a plain local disk, or a remote disk such as HDFS that writes through a normal buffer). Decorator
  * disks (encrypted, read-only) are unwrapped down to the disk they delegate to, so a wrapped S3 / Azure
  * disk reports the same sizes as a bare one. Pass the result into estimateNeededMemoryForMerge as
  * remote_write_buffer_memory once the destination disk is known, so the reservation reflects the disk's
  * own multipart upload sizes rather than the query/session settings that a background writer ignores.
  */
DiskWriteBufferMemory getDiskWriteBufferMemory(const DiskPtr & disk);

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
