#pragma once

#include <Core/Block.h>
#include <Core/Row.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Columns/ColumnsNumber.h>

#include <Interpreters/sortBlock.h>
#include <Interpreters/Context.h>

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
    MergeTreeDataWriter(MergeTreeData & data_) : data(data_), log(&Logger::get(data.getLogName() + " (Writer)")) {}

    /** Split the block to blocks, each of them must be written as separate part.
      *  (split rows by partition)
      * Works deterministically: if same block was passed, function will return same result in same order.
      */
    BlocksWithPartition splitBlockIntoParts(const Block & block, size_t max_parts);

    /** All rows must correspond to same partition.
      * Returns part with unique name starting with 'tmp_', yet not added to MergeTreeData.
      */
    MergeTreeData::MutableDataPartPtr writeTempPart(BlockWithPartition & block);

private:
    MergeTreeData & data;

    Logger * log;
};

}
