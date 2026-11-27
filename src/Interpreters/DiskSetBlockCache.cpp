#include <Interpreters/DiskSetBlockCache.h>

#include <Common/CacheBase.h>

#include <algorithm>

namespace CurrentMetrics
{
extern const Metric ExternalSetCacheBytes;
extern const Metric ExternalSetCacheCells;
}

namespace DB
{

struct DiskSetBlockCache::Weight
{
    /// The bytes that the cache spends on each block in its hash map, its queues and the shared pointer, as
    /// the mark cache estimates for its marks.
    static constexpr size_t overhead = 128;

    size_t operator()(const DiskSetCachedBlock & block) const { return block.body.allocated_bytes() + overhead; }
};

class DiskSetBlockCache::Shard : public CacheBase<size_t, DiskSetCachedBlock, std::hash<size_t>, Weight>
{
    using Base = CacheBase<size_t, DiskSetCachedBlock, std::hash<size_t>, Weight>;

public:
    Shard(size_t max_bytes, std::atomic<bool> & full_)
        : Base(CurrentMetrics::ExternalSetCacheBytes, CurrentMetrics::ExternalSetCacheCells, max_bytes)
        , full(full_)
    {
    }

private:
    /// Only the first eviction writes the flag, because writing it on each eviction would make the threads
    /// that evict blocks contend for it.
    void onEntryRemoval(size_t /*weight_loss*/, const BlockPtr & /*block*/) override
    {
        if (!full.load(std::memory_order_relaxed))
            full.store(true, std::memory_order_relaxed);
    }

    std::atomic<bool> & full;
};

DiskSetBlockCache::DiskSetBlockCache(size_t max_bytes)
    : shards(std::clamp(max_bytes / min_bytes_per_shard, 1uz, max_shard_count)) /// 64 shards for the default budget of 128 MiB
{
    for (auto & shard : shards)
        shard = std::make_unique<Shard>(max_bytes / shards.size(), full); /// Equal shares of the budget
}

DiskSetBlockCache::~DiskSetBlockCache() = default;

template <typename Function>
void DiskSetBlockCache::forEachShard(const std::vector<size_t> & blocks, Function && function)
{
    std::vector<std::vector<size_t>> shard_blocks(shards.size());
    std::vector<std::vector<size_t>> positions(shards.size());
    for (size_t i = 0; i < blocks.size(); ++i)
    {
        shard_blocks[blocks[i] % shards.size()].push_back(blocks[i]);
        positions[blocks[i] % shards.size()].push_back(i); /// Where the result for the block goes
    }

    for (size_t shard = 0; shard < shards.size(); ++shard)
        if (!shard_blocks[shard].empty())
            function(*shards[shard], shard_blocks[shard], positions[shard]);
}

DiskSetBlockCache::CallBlocks DiskSetBlockCache::getMany(const std::vector<size_t> & blocks)
{
    CallBlocks result{.cached = std::vector<BlockPtr>(blocks.size()), .admitted = std::vector<bool>(blocks.size())};
    forEachShard(blocks, [&](Shard & shard, const std::vector<size_t> & shard_blocks, const std::vector<size_t> & positions)
    {
        auto shard_result = shard.getMany(shard_blocks);
        for (size_t i = 0; i < positions.size(); ++i)
            result.cached[positions[i]] = std::move(shard_result[i]);
    });

    const size_t admission_period = full.load(std::memory_order_relaxed) ? admission_period_when_full : 1;
    size_t missing_blocks = 0;
    for (size_t i = 0; i < blocks.size(); ++i)
        if (!result.cached[i])
            result.admitted[i] = missing_blocks++ % admission_period == 0; /// Every missing block, or one in 8 once the cache is full
    return result;
}

void DiskSetBlockCache::setMany(const std::vector<size_t> & blocks, const std::vector<BlockPtr> & read_blocks)
{
    forEachShard(blocks, [&](Shard & shard, const std::vector<size_t> & shard_blocks, const std::vector<size_t> & positions)
    {
        std::vector<BlockPtr> shard_read_blocks;
        shard_read_blocks.reserve(positions.size());
        for (const size_t position : positions)
            shard_read_blocks.push_back(read_blocks[position]);
        shard.setMany(shard_blocks, shard_read_blocks);
    });
}

}
