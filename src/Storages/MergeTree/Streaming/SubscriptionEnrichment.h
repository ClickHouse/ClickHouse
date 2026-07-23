#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/Streaming/CursorPromoter.h>
#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>

#include <base/types.h>

#include <map>
#include <vector>

namespace DB
{

using LocalPartsByPartition = std::map<String, std::vector<MergeTreePartInfo>>;

/// Outcome of one enrichment pass over a subscription.
struct EnrichmentResult
{
    /// At least one partition's `safe_block_number` was advanced.
    bool enriched = false;
    /// A partition is blocked by an in-flight block in its gap, so a bounded stream must keep waiting.
    bool pending = false;
};

/// Advance the subscription's `safe_block_number` to the highest block reachable without crossing a
/// not-fetched/committing block.
EnrichmentResult enrichSubscription(
    MergeTreeBoundsSubscription & subscription,
    const LocalPartsByPartition & local_parts,
    const CursorPromotersMap & promoters);

}
