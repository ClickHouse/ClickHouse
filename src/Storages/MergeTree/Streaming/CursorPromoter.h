#pragma once

#include <map>
#include <optional>
#include <set>

#include <base/types.h>

namespace DB
{

/// Interval map over per-partition (min_block, max_block) ranges of visible parts.
class PartBlockNumberRanges
{
    struct BlockNumberRange
    {
        Int64 left;
        Int64 right;
    };

public:
    void addPart(Int64 left, Int64 right);

    bool isCovered(Int64 block_number) const;
    std::optional<BlockNumberRange> getNext(Int64 block_number) const;

    String dumpStructure() const;

private:
    std::map<Int64, BlockNumberRange> left_index;
    std::map<Int64, BlockNumberRange> right_index;
};

/// Determines whether enrichment may push the next-higher part given a per-partition cursor.
class MergeTreeCursorPromoter
{
public:
    MergeTreeCursorPromoter(std::set<Int64> committing_parts_, PartBlockNumberRanges virtual_parts_);

    bool canPromote(Int64 block_number, Int64 left) const;

    String dumpStructure() const;

private:
    std::set<Int64> committing_parts;
    PartBlockNumberRanges virtual_parts;
};

/// partition id -> partition promoter
using CursorPromotersMap = std::map<String, MergeTreeCursorPromoter>;

/// Build the per-partition cursor promoter map for streaming reads.
CursorPromotersMap constructPromoters(
    std::map<String, std::set<Int64>> committing_block_numbers,
    std::map<String, PartBlockNumberRanges> partition_ranges);

}
