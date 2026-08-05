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

BlockPtr BlockCache::getOrSet(BlockCacheKey key, std::function<BlockPtr()> load_func)
{
    auto holder = cache.getOrSet(key.pack(), [&] { return std::make_shared<BlockPtr>(load_func()); }).first;
    return *holder;
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
