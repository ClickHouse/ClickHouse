#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Streaming/CursorPromoter.h>
#include <Storages/Streaming/IStreamSubscription.h>

namespace DB
{

struct RangesInDataPart;

using PartsByPartitionAndBlockNumber = std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>>;
using RangesByPartitionAndBlockNumber = std::map<String, std::map<Int64, RangesInDataPart>>;

/// Walk parts in block-number order per partition; push parts whose partition
/// hashes to this subscription, advancing the per-partition cursor.
/// Returns true if at least one range was pushed.
bool enrichSubscription(
    StreamSubscriptionPtr subscription,
    const MergeTreeData & storage,
    const PartsByPartitionAndBlockNumber & data_parts,
    const CursorPromotersMap & promoters);

bool enrichSubscription(
    StreamSubscriptionPtr subscription,
    const MergeTreeData & storage,
    const RangesByPartitionAndBlockNumber & data_parts,
    const CursorPromotersMap & promoters);

/// Build the `{partition_id → {max_block → part}}` index used by enrichSubscription.
PartsByPartitionAndBlockNumber buildRightPartsIndex(MergeTreeData::DataPartsVector data_parts);
RangesByPartitionAndBlockNumber buildRightPartsIndex(RangesInDataParts ranges);

/// Construct per-partition promoters.
CursorPromotersMap constructPromoters(
    std::map<String, std::set<Int64>> committing_block_numbers,
    std::map<String, PartBlockNumberRanges> partition_ranges);

}
