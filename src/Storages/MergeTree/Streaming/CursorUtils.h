#pragma once

#include <Core/Streaming/CursorTree.h>

#include <Storages/Streaming/IStreamSubscription.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/Streaming/CursorPromoter.h>

namespace DB
{

struct PartitionCursor
{
    Int64 block_number = -1;
    Int64 block_offset = -1;

    bool operator==(const PartitionCursor & other) const = default;

    bool operator<(const PartitionCursor & other) const;
    bool operator<=(const PartitionCursor & other) const;
};

using MergeTreeCursor = std::map<String, PartitionCursor>;

MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree);

bool populateSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> & data_parts,
    const std::map<String, MergeTreeCursorPromoter> & promoters);

bool populateSubscription(
    StreamSubscriptionPtr subscription_holder,
    const MergeTreeData & storage,
    const std::map<String, std::map<Int64, RangesInDataPart>> & data_parts,
    const std::map<String, MergeTreeCursorPromoter> & promoters);

std::map<String, std::map<Int64, MergeTreeData::DataPartPtr>> buildRightPartsIndex(MergeTreeData::DataPartsVector data_parts);
std::map<String, std::map<Int64, RangesInDataPart>> buildRightPartsIndex(RangesInDataParts ranges);

std::map<String, Int64> buildInitialBlockNumberOffsets(
    const MergeTreeCursor & cursor,
    const MergeTreeData::DataPartsVector & snapshot_data_parts,
    const RangesInDataParts & analyzed_data_parts);

}
