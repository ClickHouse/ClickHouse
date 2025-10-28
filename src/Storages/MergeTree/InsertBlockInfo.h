#pragma once
#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Interpreters/InsertDeduplication.h>

namespace DB
{

struct BlockWithPartition
{
    Block block;
    MergeTreePartition partition;
    std::string partition_id;
    DeduplicationInfo::Ptr deduplication_info;

    BlockWithPartition() = default;
    BlockWithPartition(const BlockWithPartition & block_) = default;
    BlockWithPartition(BlockWithPartition && block_) = default;

    BlockWithPartition(Block block_, Row partition_);
};

}
