#pragma once

#include <Storages/MergeTree/Streaming/CursorPromoter.h>
#include <Storages/MergeTree/Streaming/CursorUtils.h>
#include <Storages/Streaming/IStreamSubscription.h>

namespace DB
{

bool enrichSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> & data_parts,
    const CursorPromotersMap & promoters);

bool enrichSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, RangesInDataPart>> & data_parts,
    const CursorPromotersMap & promoters);

std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> buildRightPartsIndex(MergeTreeData::DataPartsVector data_parts);
std::map<String, std::map<Int64, RangesInDataPart>> buildRightPartsIndex(RangesInDataParts ranges);

CursorPromotersMap constructPromoters(
    std::map<String, std::set<Int64>> committing_block_numbers,
    std::map<String, PartBlockNumberRanges> partition_ranges);

}
