#include <Storages/MergeTree/AllMergeSelector.h>

#include <cmath>


namespace DB
{

AllMergeSelector::PartsRange AllMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t /*max_total_size_to_merge*/)
{
    size_t min_partition_size = 0;
    PartsRanges::const_iterator best_partition;

    for (auto it = parts_ranges.begin(); it != parts_ranges.end(); ++it)
    {
        if (it->size() <= 1)
            continue;

        size_t sum_size = 0;
        for (const auto & part : *it)
            sum_size += part.size;

        if (!min_partition_size || sum_size < min_partition_size)
        {
            min_partition_size = sum_size;
            best_partition = it;
        }
    }

    if (min_partition_size)
        return *best_partition;
    else
        return {};
}

}
