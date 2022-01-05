#pragma once

#include <Core/Block.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

struct BlockWithPartition
{
    Block block;
    Row partition;

    BlockWithPartition(Block && block_, Row && partition_)
        : block(block_), partition(std::move(partition_))
    {
    }
};

using BlocksWithPartition = std::vector<BlockWithPartition>;

/** Writes new parts of data to the merge tree.
  */
class MergeTreeDataWriter
{
public:
    explicit MergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(&Poco::Logger::get(data.getLogName() + " (Writer)")) {}

    /** Split the block to blocks, each of them must be written as separate part.
      *  (split rows by partition)
      * Works deterministically: if same block was passed, function will return same result in same order.
      */
    static BlocksWithPartition splitBlockIntoParts(const Block & block, size_t max_parts, const StorageMetadataPtr & metadata_snapshot, ContextPtr context);

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeData::MutableDataPartPtr writeTempPart(BlockWithPartition & block, const StorageMetadataPtr & metadata_snapshot, bool optimize_on_insert);

    MergeTreeData::MutableDataPartPtr
    writeTempPart(BlockWithPartition & block, const StorageMetadataPtr & metadata_snapshot, ContextPtr context);

    /// For insertion.
    static MergeTreeData::MutableDataPartPtr writeProjectionPart(
        MergeTreeData & data,
        Poco::Logger * log,
        Block block,
        const ProjectionDescription & projection,
        const IMergeTreeDataPart * parent_part);

    /// For mutation: MATERIALIZE PROJECTION.
    static MergeTreeData::MutableDataPartPtr writeTempProjectionPart(
        MergeTreeData & data,
        Poco::Logger * log,
        Block block,
        const ProjectionDescription & projection,
        const IMergeTreeDataPart * parent_part,
        size_t block_num);

    /// For WriteAheadLog AddPart.
    static MergeTreeData::MutableDataPartPtr writeInMemoryProjectionPart(
        const MergeTreeData & data,
        Poco::Logger * log,
        Block block,
        const ProjectionDescription & projection,
        const IMergeTreeDataPart * parent_part);

    static Block mergeBlock(
        const Block & block,
        SortDescription sort_description,
        const Names & partition_key_columns,
        IColumn::Permutation *& permutation,
        const MergeTreeData::MergingParams & merging_params);

private:
    static MergeTreeData::MutableDataPartPtr writeProjectionPartImpl(
        const String & part_name,
        MergeTreeDataPartType part_type,
        const String & relative_path,
        bool is_temp,
        const IMergeTreeDataPart * parent_part,
        const MergeTreeData & data,
        Poco::Logger * log,
        Block block,
        const ProjectionDescription & projection);

    MergeTreeData & data;

    Poco::Logger * log;
};

}
