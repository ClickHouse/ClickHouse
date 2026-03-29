#pragma once

#include <Interpreters/Cache/PartAggregationCache.h>
#include <Processors/ISource.h>

namespace DB
{

/// Source that emits cached intermediate aggregation blocks from the PartAggregationCache.
/// Each cached entry corresponds to a pre-aggregated result for one MergeTree data part.
class PartAggregationCacheSource : public ISource
{
public:
    PartAggregationCacheSource(
        const Block & header,
        std::vector<PartAggregationCache::EntryPtr> entries_)
        : ISource(std::make_shared<const Block>(header))
        , entries(std::move(entries_))
    {
    }

    String getName() const override { return "PartAggregationCacheSource"; }

protected:
    Chunk generate() override
    {
        if (current >= entries.size())
            return {};

        const auto & block = entries[current]->block;
        ++current;

        return Chunk(block.getColumns(), block.rows());
    }

private:
    std::vector<PartAggregationCache::EntryPtr> entries;
    size_t current = 0;
};

}
