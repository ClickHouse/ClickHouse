#pragma once

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

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
        Block block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        cache->set(key, std::move(block));
    }

private:
    PartAggregationCachePtr cache;
    PartAggregationCache::Key key;
};

}
