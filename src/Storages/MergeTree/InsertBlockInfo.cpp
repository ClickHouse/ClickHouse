#include <Storages/MergeTree/InsertBlockInfo.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnsNumber.h>
#include <IO/WriteHelpers.h>
#include <filesystem>

namespace DB
{

BlockWithPartition::BlockWithPartition(std::shared_ptr<Block> block_, Row partition_)
    : block(std::move(block_))
    , partition(std::move(partition_))
{
}

}
