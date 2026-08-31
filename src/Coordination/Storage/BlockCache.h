#pragma once

#include <Coordination/Storage/Common.h>
#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>

#include <functional>

namespace Coordination::Storage
{

/// Cache of blocks (BlockData) read from files (SortedFile).
/// Accessed infrequently, usually to populate the node hash map in StorageState. Most individual
/// node lookups find a cached block through weak_ptr in NodeRefCache or SortedFile, without touching
/// BlockCache directly. So the LRU/SLRU queue in this cache doesn't track the access pattern well.

struct BlockCacheKey
{
    uint32_t file_id = 0;
    uint32_t block_idx = 0;

    uint64_t pack() const { return (static_cast<uint64_t>(file_id) << 32) | static_cast<uint64_t>(block_idx); }
};

struct BlockCacheWeightFunction
{
    size_t operator()(const BlockPtr & block) const;
};

/// TODO: Make it auto-resize to use most available memory, like PageCache (hopefully reusing code).
class BlockCache
{
public:
    explicit BlockCache(size_t max_size_in_bytes);

    BlockPtr get(BlockCacheKey key);

    /// If the block is not in cache, calls the function to load a group of blocks, then adds these
    /// blocks to cache. Does everything under a per-block-group lock to avoid loading the same
    /// group multiple times concurrently. Caller should first call `get` for fast path on cache hit.
    BlockPtr getBlockOrLoadGroup(BlockCacheKey key, uint32_t group_start_block_idx, std::function<std::vector<BlockPtr>()> load_func);

    /// Add a block into the middle of LRU list, or something like that.
    /// If lots of unneeded blocks are added like this, it won't flush out the whole cache.
    /// If an added block is accessed soon, it's promoted to a normal recently-used block.
    /// Used for blocks written to files.
    /// (This is how SLRU's set behaves: new entries go to the probationary segment and get
    ///  promoted to the protected segment only when accessed again.)
    void insertProbationary(BlockCacheKey key, BlockPtr block);

    void remove(BlockCacheKey key);

private:
    /// (BlockData ends up double-refcounted as shared_ptr<BlockPtr> because CacheBase always uses
    ///  shared_ptr. That's fine, this CacheBase is not touched very often.)
    DB::CacheBase<uint64_t, BlockPtr, DefaultHash<uint64_t>, BlockCacheWeightFunction> cache;

    std::vector<std::mutex> striped_mutex{512};
};

}
