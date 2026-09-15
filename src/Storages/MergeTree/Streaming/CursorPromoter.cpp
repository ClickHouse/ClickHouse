#include <Storages/MergeTree/Streaming/CursorPromoter.h>

#include <fmt/core.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

#include <base/defines.h>

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

String PartBlockNumberRanges::dumpStructure() const
{
    std::vector<String> dump;
    dump.reserve(left_index.size());
    for (const auto & part : left_index)
        dump.push_back(fmt::format("({}, {})", part.second.left, part.second.right));
    return fmt::format("[{}]", boost::algorithm::join(dump, ", "));
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

String MergeTreeCursorPromoter::dumpStructure() const
{
    std::vector<String> committing_parts_strs;
    committing_parts_strs.reserve(committing_parts.size());
    for (const auto & part : committing_parts)
        committing_parts_strs.push_back(fmt::format("{}", part));
    return fmt::format("committing: [{}], ranges: {}", boost::algorithm::join(committing_parts_strs, ", "), virtual_parts.dumpStructure());
}

CursorPromotersMap constructPromoters(
    std::map<String, std::set<Int64>> committing_block_numbers,
    std::map<String, PartBlockNumberRanges> partition_ranges)
{
    CursorPromotersMap promoters;

    for (auto && [partition_id, ranges] : partition_ranges)
        promoters.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(partition_id),
            std::forward_as_tuple(std::move(committing_block_numbers[partition_id]), std::move(ranges)));

    return promoters;
}

}
