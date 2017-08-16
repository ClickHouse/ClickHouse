#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>
#include <Interpreters/Context.h>

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

struct BlockWithPartitionIndex
{
    Block block;
    MergeTreePartitionIndex partition_idx;

    BlockWithPartitionIndex(const Block & block_, MergeTreePartitionIndex && partition_idx_)
        : block(block_), partition_idx(std::move(partition_idx_))
    {
    }
};

using BlocksWithPartitionIndex = std::list<BlockWithPartitionIndex>;

 /** Writes new parts of data to the merge tree.
  */
class MergeTreeDataWriter
{
public:
    MergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(&Logger::get(data.getLogName() + " (Writer)")) {}

    /** Split the block to blocks, each of them must be written as separate part.
      *  (split rows by partition)
      * Works deterministically: if same block was passed, function will return same result in same order.
      */
    BlocksWithPartitionIndex splitBlockIntoParts(const Block & block);

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeData::MutableDataPartPtr writeTempPart(BlockWithPartitionIndex & block);

private:
    MergeTreeData & data;

    Logger * log;
};

}
