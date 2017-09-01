#pragma once

#include <IO/WriteBufferFromFile.h>
#include <IO/CompressedWriteBuffer.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/sortBlock.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Core/Block.h>

namespace DB
{

struct BlockWithShardNum final
{
    BlockWithShardNum(Block && block_, size_t shard_no_)
        : block(std::move(block_)), shard_no(shard_no_)
    {
    }

    BlockWithShardNum(const BlockWithShardNum &) = delete;
    BlockWithShardNum & operator=(const BlockWithShardNum &) = delete;

    Block block;
    size_t shard_no;
};

using BlocksWithShardNum = std::list<BlockWithShardNum>;

struct ReshardingJob;

/** Creates new shard parts of data.
  */
class MergeTreeSharder final
{
public:
    MergeTreeSharder(MergeTreeData & data_, const ReshardingJob & job_);
    MergeTreeSharder(const MergeTreeSharder &) = delete;
    MergeTreeSharder & operator=(const MergeTreeSharder &) = delete;

    /** Breaks the block into blocks by the sharding key, each of which
      * must be written to a separate part. It works deterministically: if
      * give the same block to the input, the output will be the same blocks in the
      * in the same order.
      */
    BlocksWithShardNum shardBlock(const Block & block);

private:
    IColumn::Selector createSelector(Block block);

    MergeTreeData & data;
    const ReshardingJob & job;
    Logger * log;
    std::vector<UInt64> slots;
    ExpressionActionsPtr sharding_key_expr;
    std::string sharding_key_column_name;
};

}
