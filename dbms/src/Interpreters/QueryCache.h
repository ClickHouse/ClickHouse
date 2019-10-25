#pragma once

#include <Core/Block.h>
#include <Core/QueryProcessingStage.h>

#include <set>
#include <mutex>
#include <vector>
#include <condition_variable>

namespace DB
{

class Set;
using SetPtr = std::shared_ptr<Set>;

// TODO: Refactor
struct QueryCacheItem
{
    QueryCacheItem() : shard_num(0), query(""), processed_stage(QueryProcessingStage::FetchColumns)
    {
    }

    QueryCacheItem(UInt32 shard_num_, String query_, QueryProcessingStage::Enum processed_stage_) :
        shard_num(shard_num_), query(query_), processed_stage(processed_stage_)
    {
    }

    QueryCacheItem(UInt32 shard_num_, String query_, QueryProcessingStage::Enum processed_stage_, Block block_) :
        shard_num(shard_num_), query(query_), processed_stage(processed_stage_)
    {
        blocks.push_back(block_);
    }

    QueryCacheItem(UInt32 shard_num_, String query_, QueryProcessingStage::Enum processed_stage_, SetPtr set_) :
        shard_num(shard_num_), query(query_), processed_stage(processed_stage_), set(set_)
    {
    }

    bool operator < (const QueryCacheItem & rhs) const
    {
        if (query == rhs.query)
            if (processed_stage == rhs.processed_stage)
                return shard_num < rhs.shard_num;
            else
                return processed_stage < rhs.processed_stage;
        else 
            return query < rhs.query;
    }

    std::vector<Block> getBlocks() const { return blocks; }
    SetPtr getSet() const { return set; }

    UInt32 shard_num;
    String query;
    QueryProcessingStage::Enum processed_stage;

    mutable std::vector<Block> blocks;
    mutable SetPtr set;
};

bool getQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, QueryCacheItem & cache);
bool reserveQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage);
void waitAndGetQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, QueryCacheItem & cache);
void addQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, Block block);
void addQueryCache(UInt32 shard_num, String query, QueryProcessingStage::Enum processed_stage, SetPtr set);

extern std::set<QueryCacheItem> g_cache;
extern std::set<QueryCacheItem> g_resv;
extern std::mutex g_cache_lock;
extern std::condition_variable g_cache_cv;

}
