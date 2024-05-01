#pragma once

#include <map>
#include <set>

#include <base/types.h>

namespace DB
{

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

    Int64 getMaxBlockNumber() const;

    String dumpStructure() const;

private:
    std::map<Int64, BlockNumberRange> left_index;
    std::map<Int64, BlockNumberRange> right_index;
};

class MergeTreeCursorPromoter
{
public:
    MergeTreeCursorPromoter(std::set<Int64> committing_parts_, PartBlockNumberRanges virtual_parts_);

    bool canPromote(Int64 block_number, Int64 left) const;

    Int64 getMaxBlockNumber() const;

    String dumpStructure() const;

private:
    std::set<Int64> committing_parts;
    PartBlockNumberRanges virtual_parts;
};

using CursorPromotersMap = std::map<String, MergeTreeCursorPromoter>;

}
