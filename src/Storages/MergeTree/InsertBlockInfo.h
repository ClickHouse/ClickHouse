#pragma once

#include <Storages/MergeTree/MergeTreeDataWriter.h>

namespace DB
{

struct SyncInsertBlockInfo
{
    SyncInsertBlockInfo(
        Poco::Logger * /*log_*/,
        std::string && block_id_,
        BlockWithPartition && /*block_*/,
        std::optional<BlockWithPartition> && /*unmerged_block_with_partition_*/)
        : block_id(std::move(block_id_))
    {
    }

    explicit SyncInsertBlockInfo(std::string block_id_)
        : block_id(std::move(block_id_))
    {}

    std::string block_id;
};

struct AsyncInsertBlockInfo
{
    Poco::Logger * log;
    std::vector<std::string> block_id;
    BlockWithPartition block_with_partition;
    /// Some merging algorithms can mofidy the block which loses the information about the async insert offsets
    /// when preprocessing or filtering data for asnyc inserts deduplication we want to use the initial, unmerged block
    std::optional<BlockWithPartition> unmerged_block_with_partition;
    std::unordered_map<String, std::vector<size_t>> block_id_to_offset_idx;

    AsyncInsertBlockInfo(
        Poco::Logger * log_,
        std::vector<std::string> && block_id_,
        BlockWithPartition && block_,
        std::optional<BlockWithPartition> && unmerged_block_with_partition_);

    void initBlockIDMap();

    /// this function check if the block contains duplicate inserts.
    /// if so, we keep only one insert for every duplicate ones.
    bool filterSelfDuplicate();

    /// remove the conflict parts of block for rewriting again.
    void filterBlockDuplicate(const std::vector<String> & block_paths, bool self_dedup);
        /// Convert block id vector to string. Output at most 50 ids.

    static std::vector<String> getHashesForBlocks(BlockWithPartition & block, String partition_id);
};

}
