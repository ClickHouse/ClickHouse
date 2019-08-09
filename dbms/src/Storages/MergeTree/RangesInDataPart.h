#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MarkRange.h>


namespace DB
{


struct RangesInDataPart
{
    MergeTreeData::DataPartPtr data_part;
    size_t part_index_in_query;
    MarkRanges ranges;

    RangesInDataPart() = default;

    RangesInDataPart(const MergeTreeData::DataPartPtr & data_part, const size_t part_index_in_query,
                     const MarkRanges & ranges = MarkRanges{})
        : data_part{data_part}, part_index_in_query{part_index_in_query}, ranges{ranges}
    {
    }

    size_t getMarksCount() const
    {
        size_t total = 0;
        for (const auto & range : ranges)
            total += range.end - range.begin;

        return total;
    }

    size_t getRowsCount() const
    {
        return data_part->index_granularity.getRowsCountInRanges(ranges);
    }
};

using RangesInDataParts = std::vector<RangesInDataPart>;

}
