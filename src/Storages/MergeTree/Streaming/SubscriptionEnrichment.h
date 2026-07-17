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
    /// At least one partition is blocked because a block is still in flight in its gap (being
    /// committed, or not yet fetched). The safe segment is not fully determined, so a bounded stream
    /// must keep waiting rather than finish on an empty snapshot.
    bool pending = false;
};

/// Advance the subscription's `safe_block_number` to the highest block reachable without crossing a
/// not-fetched/committing block.
EnrichmentResult enrichSubscription(
    MergeTreeBoundsSubscription & subscription,
    const LocalPartsByPartition & local_parts,
    const CursorPromotersMap & promoters);

}
