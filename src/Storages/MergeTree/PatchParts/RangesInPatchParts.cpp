#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeDataPartInfoForReader.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
    extern const Event AnalyzePatchRangesMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

MarkRanges optimizeRanges(const MarkRanges & ranges)
{
    if (ranges.empty())
        return MarkRanges{};

    MarkRanges result_ranges;
    result_ranges.push_back(ranges[0]);

    for (size_t i = 1; i < ranges.size(); ++i)
    {
        auto & last_range = result_ranges.back();
        if (ranges[i].begin < last_range.begin)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Ranges for patch parts must be sorted");

        if (ranges[i].begin > last_range.end)
            result_ranges.push_back(ranges[i]);
        else
            last_range.end = std::max(last_range.end, ranges[i].end);
    }

    return result_ranges;
}

MarkRanges getRangesInPatchPartMerge(const DataPartPtr & original_part, const PatchPartInfoForReader & patch, const MarkRanges & original_ranges)
{
    chassert(patch.mode == PatchMode::Merge);
    if (patch.source_parts.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Applying patch parts with mode {} requires only one part", PatchMode::Merge);

    if (patch.source_parts.front() != original_part->name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected {} source part, got: {}", original_part->name, patch.source_parts.front());

    MarkRanges patch_part_ranges;
    const auto & index_granularity = original_part->index_granularity;
    auto patch_index = patch.part->getIndexPtr();

    /// Index may be empty if part is empty.
    if (patch_index->empty())
        return {};

    if (patch_index->size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index of patch part must have 2 columns, got {}", patch_index->size());

    const auto & patch_name_column = assert_cast<const ColumnLowCardinality &>(*patch_index->at(0));
    const auto & patch_offset_data = assert_cast<const ColumnUInt64 &>(*patch_index->at(1)).getData();

    for (const auto & range : original_ranges)
    {
        size_t begin_row = index_granularity->getMarkStartingRow(range.begin);
        size_t end_row = index_granularity->getMarkStartingRow(range.end);

        auto [begin_range, end_range] = getPartNameOffsetRange(
            patch_name_column, patch_offset_data, original_part->name, begin_row, end_row);

        if (begin_range == patch_name_column.size() || end_range == 0)
            continue;

        if (begin_range != 0)
            --begin_range;

        patch_part_ranges.emplace_back(begin_range, end_range);
    }

    std::ranges::sort(patch_part_ranges, std::less{}, &MarkRange::begin);

    return optimizeRanges(patch_part_ranges);
}

MarkRanges getRangesInPatchPartJoin(const PatchPartInfoForReader & patch)
{
    chassert(patch.mode == PatchMode::Join);
    MarkRanges patch_part_ranges;
    auto patch_index = patch.part->getIndexPtr();

    /// Index may be empty if part is empty.
    if (patch_index->empty())
        return {};

    if (patch_index->size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index of patch part must have 2 columns, got {}", patch_index->size());

    const auto & patch_name_column = assert_cast<const ColumnLowCardinality &>(*patch_index->at(0));

    for (const auto & source_part_name : patch.source_parts)
    {
        auto [begin_range, end_range] = getPartNameRange(patch_name_column, source_part_name);

        if (begin_range == patch_name_column.size() || end_range == 0)
            continue;

        if (begin_range != 0)
            --begin_range;

        patch_part_ranges.emplace_back(begin_range, end_range);
    }

    std::sort(patch_part_ranges.begin(), patch_part_ranges.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.begin < rhs.begin;
    });

    return optimizeRanges(patch_part_ranges);
}

MarkRanges getRangesInPatchPart(const DataPartPtr & original_part, const PatchPartInfoForReader & patch, const MarkRanges & ranges)
{
    switch (patch.mode)
    {
        case PatchMode::Merge:
            return getRangesInPatchPartMerge(original_part, patch, ranges);
        case PatchMode::Join:
            return getRangesInPatchPartJoin(patch);
    }
}

std::vector<MarkRanges> getRangesInPatchParts(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges)
{
    std::vector<MarkRanges> patches_ranges;
    patches_ranges.reserve(patch_parts.size());

    for (const auto & patch_part : patch_parts)
    {
        auto patch_ranges = getRangesInPatchPart(original_part, patch_part, ranges);
        patches_ranges.push_back(std::move(patch_ranges));
    }

    return patches_ranges;
}

}

void RangesInPatchParts::addPart(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & original_ranges)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    for (const auto & patch_part : patch_parts)
    {
        auto patch_ranges = getRangesInPatchPart(original_part, patch_part, original_ranges);

        if (!patch_ranges.empty())
        {
            auto & current_ranges = ranges_by_name[patch_part.part->getPartName()];
            current_ranges.insert(current_ranges.end(), patch_ranges.begin(), patch_ranges.end());
        }
    }
}

void RangesInPatchParts::optimize()
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    for (auto & [_, ranges] : ranges_by_name)
    {
        MarkRanges split_ranges;

        std::sort(ranges.begin(), ranges.end(), [](const auto & lhs, const auto & rhs) { return lhs.begin < rhs.begin; });
        auto optimized_ranges = optimizeRanges(ranges);

        for (auto & range : optimized_ranges)
        {
            size_t num_full_splits = (range.end - range.begin) / max_granules_in_range;
            for (size_t i = 0; i < num_full_splits; ++i)
                split_ranges.emplace_back(range.begin + max_granules_in_range * i, range.begin + max_granules_in_range * (i + 1));

            if ((range.end - range.begin) % max_granules_in_range != 0)
               split_ranges.emplace_back(range.begin + max_granules_in_range * num_full_splits, range.end);
        }

        ranges = std::move(split_ranges);
    }
}

std::vector<MarkRanges> RangesInPatchParts::getRanges(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges) const
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::AnalyzePatchRangesMicroseconds);

    auto raw_ranges = getRangesInPatchParts(original_part, patch_parts, ranges);
    chassert(patch_parts.size() == raw_ranges.size());

    std::vector<MarkRanges> optimized_ranges(raw_ranges.size());

    for (size_t i = 0; i < raw_ranges.size(); ++i)
    {
        auto ranges_for_patch = getIntersectingRanges(patch_parts[i].part->getPartName(), raw_ranges[i]);
        optimized_ranges[i] = MarkRanges(ranges_for_patch.begin(), ranges_for_patch.end());
    }

    return optimized_ranges;
}

std::set<MarkRange> RangesInPatchParts::getIntersectingRanges(const String & patch_name, const MarkRanges & ranges) const
{
    std::set<MarkRange> res;

    auto it = ranges_by_name.find(patch_name);
    if (it == ranges_by_name.end())
        return res;

    const auto & patch_ranges = it->second;

    for (const auto & range : ranges)
    {
        auto left = std::lower_bound(patch_ranges.begin(), patch_ranges.end(), range.begin, [](const MarkRange & r, UInt64 value) { return r.end < value; });
        auto right = std::upper_bound(patch_ranges.begin(), patch_ranges.end(), range.end, [](UInt64 value, const MarkRange & r) { return value < r.begin; });

        res.insert(left, right);
    }

    return res;
}

}
