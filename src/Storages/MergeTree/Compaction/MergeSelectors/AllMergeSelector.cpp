#include <Storages/MergeTree/Compaction/MergeSelectors/AllMergeSelector.h>

namespace DB
{

PartsRanges AllMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeConstraints & merge_constraints,
    const RangeFilter & range_filter) const
{
    chassert(merge_constraints.size() == 1, "Multi Select is not supported for AllMergeSelector");
    const size_t max_total_size_to_merge = merge_constraints[0].max_size_bytes;
    const size_t max_rows_in_part = merge_constraints[0].max_size_rows;

    size_t min_partition_size = 0;
    size_t min_partition_rows = 0;
    PartsRanges::const_iterator best_partition;

    for (auto it = parts_ranges.begin(); it != parts_ranges.end(); ++it)
    {
        if (it->size() <= 1)
            continue;

        if (range_filter && !range_filter(*it))
            continue;

        size_t sum_size = 0;
        size_t sum_rows = 0;

        for (const auto & part : *it)
        {
            sum_size += part.size;
            sum_rows += part.rows;
        }

        if (!min_partition_size || sum_size < min_partition_size)
        {
            min_partition_size = sum_size;
            min_partition_rows = sum_rows;
            best_partition = it;
        }
    }

    if (min_partition_size
        && min_partition_rows
        && min_partition_size <= max_total_size_to_merge
        && min_partition_rows <= max_rows_in_part)
        return {*best_partition};

    return {};
}

}
