#pragma once
#include <Core/Block.h>
#include <Storages/MergeTree/MergeTreePartition.h>


namespace DB
{

struct BlockWithPartition
{
    Block block;
    MergeTreePartition partition;

    std::vector<size_t> offsets;
    std::vector<String> tokens;

    BlockWithPartition(Block block_, Row partition_)
        : BlockWithPartition(std::move(block_), std::move(partition_), {}, {})
    {
    }

    BlockWithPartition(Block block_, Row partition_, std::vector<size_t> offsets_, std::vector<String> tokens_)
        : block(std::move(block_)) , partition(std::move(partition_)) , offsets(std::move(offsets_)), tokens(std::move(tokens_))
    {
    }
};

struct SyncInsertBlockInfo
{
    using BlockIDsType = std::string;

    SyncInsertBlockInfo(
        LoggerPtr /*log_*/,
        BlockIDsType && block_id_,
        BlockWithPartition && /*block_*/,
        std::optional<BlockWithPartition> && /*unmerged_block_with_partition_*/)
        : block_id(std::move(block_id_))
    {
    }

    explicit SyncInsertBlockInfo(BlockIDsType block_id_): block_id(std::move(block_id_))
    {
    }

    BlockIDsType block_id;
};

struct AsyncInsertBlockInfo
{
    using BlockIDsType = std::vector<std::string>;

    LoggerPtr log;
    BlockIDsType block_id;
    BlockWithPartition block_with_partition;

    /// Some merging algorithms can mofidy the block which loses the information about the async insert offsets
    /// when preprocessing or filtering data for asnyc inserts deduplication we want to use the initial, unmerged block
    std::optional<BlockWithPartition> unmerged_block_with_partition;
    std::unordered_map<String, std::vector<size_t>> block_id_to_offset_idx;

    AsyncInsertBlockInfo(
        LoggerPtr log_,
        BlockIDsType && block_id_,
        BlockWithPartition && block_,
        std::optional<BlockWithPartition> && unmerged_block_with_partition_);

    void initBlockIDMap();

    /// This function check if the block contains duplicate inserts.
    /// if so, we keep only one insert for every duplicate ones.
    bool filterSelfDuplicate();

    /// remove the conflict parts of block for rewriting again.
    void filterBlockDuplicate(const std::vector<String> & block_paths, bool self_dedup);
    static BlockIDsType getHashesForBlocks(BlockWithPartition & block, String partition_id);
};

}
