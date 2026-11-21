#pragma once

#include <Interpreters/Cache/PartialAggregateCache.h>
#include <Processors/Chunk.h>
#include <Core/Block.h>

namespace DB
{

/// Planning-stage cache hit: `ReadFromMergeTree` probed `PartialAggregateCache::get` and merged states are embedded here.
/// `AggregatingTransform` merges `cached_partial_states` without calling `PartialAggregateCache::get` again (even if execution-time cache is disabled).
/// The chunk has zero data rows; header matches the read pipeline.
struct PartialAggregatePlanHitInfo : public ChunkInfoCloneable<PartialAggregatePlanHitInfo>
{
    PartialAggregatePlanHitInfo() = default;
    PartialAggregatePlanHitInfo(const PartialAggregatePlanHitInfo & other) = default;

    PartialAggregatePlanHitInfo(PartialAggregateCache::Key cache_key_, Block cached_partial_states_)
        : cache_key(cache_key_)
        , cached_partial_states(std::move(cached_partial_states_))
    {
    }

    PartialAggregateCache::Key cache_key;
    /// Intermediate GROUP BY states (same as `PartialAggregateCache` entry).
    Block cached_partial_states;
};

using PartialAggregatePlanHitInfoPtr = std::shared_ptr<PartialAggregatePlanHitInfo>;

}
