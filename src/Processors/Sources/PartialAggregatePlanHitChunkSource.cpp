/// One-row-per-part source for a plan-time cache hit: emits a zero-row chunk with `PartialAggregatePlanHitInfo` so `AggregatingTransform`
/// merges embedded partial states without reading the part. Progress row count stays 0; aggregation accounts for the merged states.

#include <Processors/Sources/PartialAggregatePlanHitChunkSource.h>

#include <Columns/IColumn.h>
#include <Processors/Chunk.h>

namespace DB
{

PartialAggregatePlanHitChunkSource::PartialAggregatePlanHitChunkSource(
    SharedHeader header_, PartialAggregateCache::Key cache_key_, Block cached_partial_states_)
    : ISource(std::move(header_))
    , cache_key(std::move(cache_key_))
    , cached_partial_states(std::move(cached_partial_states_))
{
}

Chunk PartialAggregatePlanHitChunkSource::generate()
{
    if (!emitted)
    {
        emitted = true;
        const auto & header_block = getPort().getHeader();
        Columns columns;
        columns.reserve(header_block.columns());
        for (size_t i = 0; i < header_block.columns(); ++i)
        {
            const auto & col = header_block.getByPosition(i);
            columns.push_back(col.column->cloneEmpty()->assumeMutable());
        }
        Chunk chunk(std::move(columns), 0);
        chunk.getChunkInfos().add(std::make_shared<PartialAggregatePlanHitInfo>(cache_key, std::move(cached_partial_states)));
        return chunk;
    }
    return {};
}

}
