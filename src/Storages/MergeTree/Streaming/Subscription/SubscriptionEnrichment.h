#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorPromoter.h>
#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>

#include <base/types.h>

#include <map>
#include <vector>

namespace DB
{

using LocalPartsByPartition = std::map<String, std::vector<MergeTreePartInfo>>;

/// Advance the subscription's `safe_block_number` to the highest block reachable without crossing a
/// not-fetched/committing block. Returns true if the round produced new work for the readers.
bool enrichSubscription(
    MergeTreeBoundsSubscription & subscription,
    const LocalPartsByPartition & local_parts,
    const CursorPromotersMap & promoters);

}
