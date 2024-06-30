#include <fmt/core.h>

#include <boost/range/algorithm/transform.hpp>
#include <boost/algorithm/string/join.hpp>

#include <base/defines.h>

#include <Storages/MergeTree/Streaming/CursorPromoter.h>

namespace DB
{

void PartBlockNumberRanges::addPart(Int64 left, Int64 right)
{
    left_index[left] = {left, right};
    right_index[right] = {left, right};
}

bool PartBlockNumberRanges::isCovered(Int64 block_number) const
{
    auto it = right_index.upper_bound(block_number);

    if (it == right_index.end())
        return false;

    return it->second.left <= block_number;
}

std::optional<PartBlockNumberRanges::BlockNumberRange> PartBlockNumberRanges::getNext(Int64 block_number) const
{
    auto it = left_index.upper_bound(block_number);

    if (it == left_index.end())
        return std::nullopt;

    return it->second;
}

Int64 PartBlockNumberRanges::getMaxBlockNumber() const
{
    if (right_index.empty())
        return -1;

    return right_index.rbegin()->second.right;
}

String PartBlockNumberRanges::dumpStructure() const
{
    std::vector<String> left_index_dump, right_index_dump;

    for (const auto & part : left_index)
        left_index_dump.push_back(fmt::format("({}, {})", part.second.left, part.second.right));

    for (const auto & part : right_index)
        right_index_dump.push_back(fmt::format("({}, {})", part.second.left, part.second.right));

    return fmt::format("left: [{}], right: [{}]", boost::join(left_index_dump, ", "), boost::join(right_index_dump, ", "));
}

MergeTreeCursorPromoter::MergeTreeCursorPromoter(std::set<Int64> committing_parts_, PartBlockNumberRanges virtual_parts_)
    : committing_parts(std::move(committing_parts_)), virtual_parts(std::move(virtual_parts_))
{
}

bool MergeTreeCursorPromoter::canPromote(Int64 block_number, Int64 left) const
{
    chassert(block_number < left);

    if (auto it = committing_parts.upper_bound(block_number); it != committing_parts.end() && *it < left)
        return false;

    if (auto part_range = virtual_parts.getNext(block_number))
    {
        chassert(block_number < part_range->left);

        if (part_range->left < left)
            return false;
    }

    if (virtual_parts.isCovered(block_number))
        return false;

    return true;
}

Int64 MergeTreeCursorPromoter::getMaxBlockNumber() const
{
    Int64 max_block_number = virtual_parts.getMaxBlockNumber();

    if (!committing_parts.empty())
        max_block_number = std::max(max_block_number, *committing_parts.rbegin());

    return max_block_number;
}

String MergeTreeCursorPromoter::dumpStructure() const
{
    std::vector<String> committing_parts_strs;
    committing_parts_strs.reserve(committing_parts.size());

    for (const auto & part : committing_parts_strs)
        committing_parts_strs.push_back(fmt::format("{}", part));

    return fmt::format("committing: [{}], ranges: {{{}}}", boost::join(committing_parts_strs, ", "), virtual_parts.dumpStructure());
}

}
