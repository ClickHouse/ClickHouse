#pragma once

#include <Core/Block.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>

#include <Processors/Chunk.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>

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

    void cancel();
    void finalize();
    void prewarmCaches();
};

using MergeTreeTemporaryPartPtr = std::unique_ptr<MergeTreeTemporaryPart>;
using BlocksWithPartition = std::vector<BlockWithPartition>;

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
      */
    static BlocksWithPartition splitBlockIntoParts(Block && block, size_t max_parts, const StorageMetadataPtr & metadata_snapshot, ContextPtr context);

    /// This structure contains not completely written temporary part.
    /// Some writes may happen asynchronously, e.g. for blob storages.
    /// You should call finalize() to wait until all data is written.

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeTemporaryPartPtr writeTempPart(BlockWithPartition & block, StorageMetadataPtr metadata_snapshot, ContextPtr context);

    MergeTreeTemporaryPartPtr writeTempPatchPart(
        BlockWithPartition & block,
        StorageMetadataPtr metadata_snapshot,
        String partition_id,
        SourcePartsSetForPatch source_parts_set,
        ContextPtr context);

    MergeTreeData::MergingParams::Mode getMergingMode() const
    {
        return data.merging_params.mode;
    }

    /// For insertion.
    static MergeTreeTemporaryPartPtr writeProjectionPart(
        const MergeTreeData & data,
        LoggerPtr log,
        Block block,
        const ProjectionDescription & projection,
        IMergeTreeDataPart * parent_part,
        bool merge_is_needed);

    /// For mutation: MATERIALIZE PROJECTION.
    static MergeTreeTemporaryPartPtr writeTempProjectionPart(
        const MergeTreeData & data,
        LoggerPtr log,
        Block block,
        const ProjectionDescription & projection,
        IMergeTreeDataPart * parent_part,
        size_t block_num);

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
        SourcePartsSetForPatch source_parts_set,
        ContextPtr context,
        UInt64 block_number);

    static MergeTreeTemporaryPartPtr writeProjectionPartImpl(
        const String & part_name,
        bool is_temp,
        IMergeTreeDataPart * parent_part,
        const MergeTreeData & data,
        LoggerPtr log,
        Block block,
        const ProjectionDescription & projection,
        bool merge_is_needed);

    MergeTreeData & data;
    LoggerPtr log;
};

}
