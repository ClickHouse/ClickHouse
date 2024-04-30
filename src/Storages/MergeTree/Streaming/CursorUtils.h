#pragma once

#include <memory>

#include <Core/Streaming/CursorTree.h>

#include <Processors/CursorInfo.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct PartitionCursor
{
    Int64 block_number = -1;
    Int64 block_offset = -1;

    bool operator<(const PartitionCursor & other) const;
    bool operator<=(const PartitionCursor & other) const;
    bool operator==(const PartitionCursor & other) const = default;
};

using MergeTreeCursor = std::map<String, PartitionCursor>;

MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree);

std::shared_ptr<CursorInfo> buildMergeTreeCursorInfo(
    const String & storage_full_name,
    const String & partition_id,
    const std::optional<String> & keeper_key,
    Int64 block_number,
    Int64 block_offset);

std::map<String, Int64> buildInitialBlockNumberOffsets(
    const MergeTreeCursor & cursor,
    const MergeTreeData::DataPartsVector & snapshot_data_parts,
    const RangesInDataParts & analyzed_data_parts);

std::optional<FilterDAGInfo> convertCursorToFilter(const MergeTreeCursor & cursor, SelectQueryInfo & info);

}
