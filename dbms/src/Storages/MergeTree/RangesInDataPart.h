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

    size_t getAvgGranularityForRanges() const
    {
        size_t total_rows = 0;
        size_t total_marks = 0;
        for (const auto & range : ranges)
        {
            total_rows += data_part->index_granularity.getRowsCountInRange(range);
            total_marks += (range.end - range.begin);
        }
        return total_rows / total_marks;
    }

    size_t getRowsCount() const
    {
        return data_part->index_granularity.getRowsCountInRanges(ranges);
    }

};

using RangesInDataParts = std::vector<RangesInDataPart>;

inline size_t getAvgGranularityForAllPartsRanges(const RangesInDataParts & parts_ranges)
{
    size_t sum_of_averages = 0;
    for (const auto & part : parts_ranges)
        sum_of_averages += part.getAvgGranularityForRanges();

    return sum_of_averages / parts_ranges.size();
}

}
