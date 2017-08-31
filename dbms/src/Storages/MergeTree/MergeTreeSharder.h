#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/sortBlock.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Core/Block.h>

namespace DB
{

struct ShardedBlockWithPartitionIndex final
{
    ShardedBlockWithPartitionIndex(const Block & block_, size_t shard_no_, const MergeTreePartitionIndex & partition_idx_);
    ShardedBlockWithPartitionIndex(const ShardedBlockWithPartitionIndex &) = delete;
    ShardedBlockWithPartitionIndex & operator=(const ShardedBlockWithPartitionIndex &) = delete;

    Block block;
    size_t shard_no;
    MergeTreePartitionIndex partition_idx;
};

using ShardedBlocksWithPartitionIndex = std::list<ShardedBlockWithPartitionIndex>;

struct ReshardingJob;

/** Creates new shard parts of data.
  */
class MergeTreeSharder final
{
public:
    MergeTreeSharder(MergeTreeData & data_, const ReshardingJob & job_, const Field & partition);
    MergeTreeSharder(const MergeTreeSharder &) = delete;
    MergeTreeSharder & operator=(const MergeTreeSharder &) = delete;

    /** Breaks the block into blocks by the sharding key, each of which
      * must be written to a separate part. It works deterministically: if
      * give the same block to the input, the output will be the same blocks in the
      * in the same order.
      */
    ShardedBlocksWithPartitionIndex shardBlock(const Block & block);

private:
    IColumn::Selector createSelector(Block block);

    MergeTreeData & data;
    const ReshardingJob & job;
    const Field & partition;
    Logger * log;
    std::vector<size_t> slots;
    ExpressionActionsPtr sharding_key_expr;
    std::string sharding_key_column_name;
};

}
