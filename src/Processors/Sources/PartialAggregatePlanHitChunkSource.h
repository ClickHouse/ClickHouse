#pragma once

#include <Processors/ISource.h>
#include <Interpreters/Cache/PartialAggregateCache.h>
#include <Interpreters/Cache/PartialAggregatePlanHitInfo.h>
#include <Core/Block.h>

namespace DB
{

/// Emits a single zero-row chunk (read header) tagged with `PartialAggregatePlanHitInfo` so
/// `AggregatingTransform` merges cached per-part states without scanning the part.
class PartialAggregatePlanHitChunkSource final : public ISource
{
public:
    PartialAggregatePlanHitChunkSource(
        SharedHeader header_,
        PartialAggregateCache::Key cache_key_,
        Block cached_partial_states_);

    String getName() const override { return "PartialAggregatePlanHitChunk"; }

protected:
    Chunk generate() override;

private:
    PartialAggregateCache::Key cache_key;
    Block cached_partial_states;
    bool emitted = false;
};

}
