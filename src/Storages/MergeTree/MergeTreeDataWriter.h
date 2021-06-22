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
    MergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(&Poco::Logger::get(data.getLogName() + " (Writer)")) {}

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

    MergeTreeData::MutableDataPartPtr writeProjectionPart(
        Block block, const ProjectionDescription & projection, const IMergeTreeDataPart * parent_part);

    static MergeTreeData::MutableDataPartPtr writeTempProjectionPart(
        MergeTreeData & data,
        Poco::Logger * log,
        Block block,
        const ProjectionDescription & projection,
        const IMergeTreeDataPart * parent_part,
        size_t block_num);

    Block mergeBlock(const Block & block, SortDescription sort_description, Names & partition_key_columns, IColumn::Permutation *& permutation);

private:
    static MergeTreeData::MutableDataPartPtr writeProjectionPartImpl(
        MergeTreeData & data,
        Poco::Logger * log,
        Block block,
        const StorageMetadataPtr & metadata_snapshot,
        MergeTreeData::MutableDataPartPtr && new_data_part);

    MergeTreeData & data;

    Poco::Logger * log;
};

}
