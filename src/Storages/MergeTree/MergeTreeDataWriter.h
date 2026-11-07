#pragma once

#include <Core/Block.h>
#include <Common/ProfileEventsScope.h>
#include <Common/Stopwatch.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>


#include <Interpreters/sortBlock.h>

#include <Processors/Chunk.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/InsertBlockInfo.h>


namespace DB
{

class DeduplicationInfo;
using DeduplicationInfoPtr = std::shared_ptr<DeduplicationInfo>;

void buildScatterSelector(
    const ColumnRawPtrs & columns,
    PODArray<size_t> & partition_num_to_first_row,
    IColumn::Selector & selector,
    size_t max_parts,
    ContextPtr context);

struct MergeTreeTemporaryPart
{
    /// temporary_directory_lock must be declared before part, because members are destroyed
    /// in reverse declaration order. The part destructor removes the temporary directory on disk
    /// (via removeIfNeeded), and this must happen while the lock is still held. Otherwise,
    /// ReplicatedMergeTreeCleanupThread can race: it checks temporary_parts, finds the name
    /// already unregistered, and removes the directory before the part destructor gets to it.
    scope_guard temporary_directory_lock;

    MergeTreeData::MutableDataPartPtr part;

    struct Stream
    {
        std::unique_ptr<MergedBlockOutputStream> stream;
        MergedBlockOutputStream::Finalizer finalizer;
    };

    std::vector<Stream> streams;

    bool finalization_started = false;

    /// UNIQUE KEY tables only: part row `i` was written from block row `sort_permutation[i]`.
    /// Empty when the part keeps the block's row order.
    IColumn::Permutation sort_permutation;

    void cancel();
    void startFinalization();
    /// Waits for the work started by `startFinalization`. Does nothing if it was not started.
    void waitFinalization() const;
    /// Size of the archives of the part and its projections whose finalization is started but not finished.
    size_t getPendingFinalizationBytes() const;
    void finalize();
    void prewarmCaches();
};

using MergeTreeTemporaryPartPtr = std::unique_ptr<MergeTreeTemporaryPart>;
using BlocksWithPartition = std::vector<BlockWithPartition>;

/// The limit of the bytes of started but unfinished packed-part finalizations of a delayed insert batch, 0 means unlimited.
size_t getMaxPendingBytesOfPackedFinalization(const MergeTreeData & data);

/// Called by `startPackedPartsFinalization` before it waits for an earlier part, holds a failpoint for tests.
void onWaitingForEarlierPackedFinalization();

/// Starts the finalization of the packed parts of a delayed insert batch, so that their uploads overlap.
/// If the buffered data of packed parts may be moved to temporary files (`max_bytes_to_buffer_for_packed_part`),
/// the started archives are read back into the upload buffers: wait for the earliest started parts before
/// starting the next one while the archives of the started parts take at least that many bytes.
/// Each element of `partitions` has the `temp_part`, `part_counters` and `elapsed_ns` fields.
template <typename Partitions, typename ProcessListElementPtr>
void startPackedPartsFinalization(Partitions & partitions, const MergeTreeData & data, const ProcessListElementPtr & process_list_element)
{
    const size_t max_pending_bytes = getMaxPendingBytesOfPackedFinalization(data);
    size_t pending_bytes = 0;
    auto first_pending = partitions.begin();

    for (auto it = partitions.begin(); it != partitions.end(); ++it)
    {
        if (process_list_element)
            process_list_element->checkTimeLimit();

        if (it->temp_part->part->getDataPartStorage().getType() != MergeTreeDataPartStorageType::Packed)
            continue;

        while (max_pending_bytes && pending_bytes >= max_pending_bytes)
        {
            chassert(first_pending != it);
            onWaitingForEarlierPackedFinalization();
            Stopwatch watch;
            ProfileEventsScope profile_events_scope(&first_pending->part_counters);
            pending_bytes -= first_pending->temp_part->getPendingFinalizationBytes();
            first_pending->temp_part->waitFinalization();
            first_pending->elapsed_ns += watch.elapsed();
            ++first_pending;
        }

        Stopwatch watch;
        ProfileEventsScope profile_events_scope(&it->part_counters);
        it->temp_part->startFinalization();
        pending_bytes += it->temp_part->getPendingFinalizationBytes();
        it->elapsed_ns += watch.elapsed();
    }
}

/** Writes new parts of data to the merge tree.
  */
class MergeTreeDataWriter
{
public:
    explicit MergeTreeDataWriter(MergeTreeData & data_)
        : data(data_)
        , log(getLogger(data.getLogName() + " (Writer)"))
    {
    }

    /** Split the block to blocks, each of them must be written as separate part.
      *  (split rows by partition)
      * Works deterministically: if same block was passed, function will return same result in same order.
      * When out_selector is set, it receives the row -> partition-index mapping (empty when the block
      * is not split, i.e. a single resulting partition). Deduplication needs it to attribute each
      * source row to the partition it landed in.
      */
    static BlocksWithPartition splitBlockIntoParts(
        Block && block,
        size_t max_parts,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        IColumn::Selector * out_selector = nullptr);

    /// This structure contains not completely written temporary part.
    /// Some writes may happen asynchronously, e.g. for blob storages.
    /// You should call finalize() to wait until all data is written.

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      * `may_have_leftover`: see `MergeTreeData::claimTemporaryPartDirectory`.
      * `txn`, when set, stamps the part's `creation_tid` instead of the context's transaction: an
      * INSERT commits each partition under its own, and for `INSERT ... SELECT` the context's is the SELECT's.
      */
    MergeTreeTemporaryPartPtr writeTempPart(
        BlockWithPartition & block,
        StorageMetadataPtr metadata_snapshot,
        ContextPtr context,
        bool may_have_leftover = true,
        const MergeTreeTransactionPtr & txn = nullptr);

    MergeTreeTemporaryPartPtr writeTempPatchPart(
        BlockWithPartition & block,
        StorageMetadataPtr metadata_snapshot,
        String partition_id,
        PatchPartIndex patch_part_index,
        ContextPtr context,
        bool may_have_leftover = true);

    MergeTreeData::MergingParams::Mode getMergingMode() const
    {
        return data.merging_params.mode;
    }

    /// For insertion.
    /// `compression_codec` is the codec chosen for the parent part; the projection inherits it so
    /// that a projection of a large (`ZSTD(3)`) part is not always written with `LZ4`.
    static MergeTreeTemporaryPartPtr writeProjectionPart(
        const MergeTreeData & data,
        Block block,
        const ProjectionDescription & projection,
        IMergeTreeDataPart * parent_part,
        CompressionCodecPtr compression_codec,
        bool merge_is_needed,
        ContextPtr context);

    /// For mutation: MATERIALIZE PROJECTION.
    /// `compression_codec` is the codec chosen for the parent part; see `writeProjectionPart`.
    static MergeTreeTemporaryPartPtr writeTempProjectionPart(
        const MergeTreeData & data,
        Block block,
        const ProjectionDescription & projection,
        IMergeTreeDataPart * parent_part,
        CompressionCodecPtr compression_codec,
        size_t block_num,
        bool use_selected_codec,
        bool is_explicit_recompression,
        ContextPtr context);

    static Block mergeBlock(
        Block && block,
        const StorageMetadataPtr & metadata_snapshot,
        SortDescription sort_description,
        IColumn::Permutation *& permutation,
        const MergeTreeData::MergingParams & merging_params);

private:
    MergeTreeTemporaryPartPtr writeTempPartImpl(
        BlockWithPartition & block_with_partition,
        StorageMetadataPtr metadata_snapshot,
        String partition_id,
        std::optional<PatchPartIndex> patch_part_index,
        ContextPtr context,
        UInt64 block_number,
        bool may_have_leftover,
        const MergeTreeTransactionPtr & txn = nullptr);

    static MergeTreeTemporaryPartPtr writeProjectionPartImpl(
        const String & part_name,
        bool is_temp,
        IMergeTreeDataPart * parent_part,
        const MergeTreeData & data,
        Block block,
        const ProjectionDescription & projection,
        CompressionCodecPtr compression_codec,
        MergeTreeIndices indices,
        bool merge_is_needed,
        bool try_adaptive_codec,
        ContextPtr context,
        bool use_selected_codec = false);

    MergeTreeData & data;
    LoggerPtr log;
};

}
