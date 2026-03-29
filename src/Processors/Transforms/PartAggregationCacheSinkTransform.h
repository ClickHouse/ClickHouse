#pragma once

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// A passthrough transform that stores intermediate aggregation blocks into the PartAggregationCache.
/// Placed after per-part aggregation to cache the result before it is merged with other parts.
/// Expects a single block per part (produced by AggregatingTransform with final=false).
class PartAggregationCacheSinkTransform : public ISimpleTransform
{
public:
    PartAggregationCacheSinkTransform(
        const Block & header_,
        PartAggregationCachePtr cache_,
        PartAggregationCache::Key key_)
        : ISimpleTransform(header_, header_, /* skip_empty_chunks = */ true)
        , cache(std::move(cache_))
        , key(std::move(key_))
    {
    }

    String getName() const override { return "PartAggregationCacheSinkTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        /// Store the block in cache. The chunk passes through unchanged.
        Block block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        cache->set(key, std::move(block));
    }

private:
    PartAggregationCachePtr cache;
    PartAggregationCache::Key key;
};

}
