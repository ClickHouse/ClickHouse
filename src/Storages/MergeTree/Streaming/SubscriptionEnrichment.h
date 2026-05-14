#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/Streaming/CursorPromoter.h>
#include <Storages/MergeTree/Streaming/MergeTreeBoundsSubscription.h>

#include <base/types.h>

#include <map>
#include <set>
#include <vector>

namespace DB
{

using LocalPartsByPartition = std::map<String, std::vector<MergeTreePartInfo>>;

/// Advance the subscription's `safe_block_number` to the highest block reachable without crossing a
/// not-fetched/committing block.
bool enrichSubscription(
    MergeTreeBoundsSubscription & subscription,
    const LocalPartsByPartition & local_parts,
    const CursorPromotersMap & promoters);

/// Build the per-partition cursor promoter map for streaming reads.
CursorPromotersMap constructPromoters(
    std::map<String, std::set<Int64>> committing_block_numbers,
    std::map<String, PartBlockNumberRanges> partition_ranges);

}
