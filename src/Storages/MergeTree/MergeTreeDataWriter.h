#pragma once

#include <Core/Block.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>

#include <Processors/Chunk.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/InsertBlockInfo.h>


namespace DB
{

struct MergeTreeTemporaryPart
{
    MergeTreeData::MutableDataPartPtr part;

    struct Stream
    {
        std::unique_ptr<MergedBlockOutputStream> stream;
        MergedBlockOutputStream::Finalizer finalizer;
    };

    std::vector<Stream> streams;
    scope_guard temporary_directory_lock;

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
    static BlocksWithPartition splitBlockIntoParts(Block && block, size_t max_parts, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, AsyncInsertInfoPtr async_insert_info = nullptr);

    /// This structure contains not completely written temporary part.
    /// Some writes may happen asynchronously, e.g. for blob storages.
    /// You should call finalize() to wait until all data is written.

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeTemporaryPartPtr writeTempPart(BlockWithPartition & block, StorageMetadataPtr metadata_snapshot, ContextPtr context);

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
