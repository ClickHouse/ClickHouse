#include <Coordination/Storage/BlockCache.h>

#include <Coordination/Storage/Node.h>

namespace CurrentMetrics
{
    extern const Metric KeeperBlockCacheBytes;
    extern const Metric KeeperBlockCacheBlocks;
}

namespace Coordination::Storage
{

size_t BlockCacheWeightFunction::operator()(const BlockPtr & block) const
{
    return sizeof(BlockData) + block->capacity;
}

BlockCache::BlockCache(size_t max_size_in_bytes)
    : cache(CurrentMetrics::KeeperBlockCacheBytes, CurrentMetrics::KeeperBlockCacheBlocks, max_size_in_bytes)
{
}

BlockPtr BlockCache::get(BlockCacheKey key)
{
    const auto holder = cache.get(key.pack());
    return holder ? *holder : nullptr;
}

BlockPtr BlockCache::getBlockOrLoadGroup(BlockCacheKey key, uint32_t group_start_block_idx, std::function<std::vector<BlockPtr>()> load_func)
{
    chassert(key.block_idx >= group_start_block_idx);

    BlockCacheKey group_key = key;
    group_key.block_idx = group_start_block_idx;

    std::lock_guard lock(striped_mutex[intHash64(group_key.pack()) % striped_mutex.size()]);

    const auto cached = cache.get(key.pack());
    if (cached)
        return *cached;

    /// Load the blocks.
    std::vector<BlockPtr> blocks = load_func();

    chassert(key.block_idx < group_start_block_idx + uint32_t(blocks.size()));

    /// Put the loaded blocks into cache.
    for (uint32_t i = 0; i < blocks.size(); ++i)
    {
        BlockCacheKey cur_key = key;
        cur_key.block_idx = group_start_block_idx + i;

        /// If a block we incidentally loaded was already in cache, avoid calling set/getOrSet/get
        /// for it as we don't want to move it up in the LRU list.
        if (!cache.contains(cur_key.pack()))
            cache.set(cur_key.pack(), std::make_shared<BlockPtr>(blocks[i]));
    }

    /// Touch the actually requested block again to promote it to SLRU protected list, ahead of the
    /// blocks that were loaded incidentally or added by background flushes and merges.
    /// (We have to do this because the caller will add the returned block to other caches, so later
    ///  accesses to this block won't go through our `cache`. This is our last chance to report
    ///  usage to the SLRU.)
    cache.get(key.pack());

    return blocks[key.block_idx - group_start_block_idx];
}

void BlockCache::insertProbationary(BlockCacheKey key, BlockPtr block)
{
    cache.set(key.pack(), std::make_shared<BlockPtr>(std::move(block)));
}

void BlockCache::remove(BlockCacheKey key)
{
    cache.remove(key.pack());
}

}
