#include <Storages/MergeTree/Compaction/MergeSelectors/AllMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>

namespace DB
{

void registerAllMergeSelector(MergeSelectorFactory & factory)
{
    factory.registerPrivateSelector("All", [](const std::any &)
    {
        return std::make_shared<AllMergeSelector>();
    });
}

PartsRanges AllMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeSizes & max_merge_sizes,
    const RangeFilter & range_filter) const
{
    chassert(max_merge_sizes.size() == 1, "Multi Select is not supported for AllMergeSelector");
    const size_t max_total_size_to_merge = max_merge_sizes[0];

    size_t min_partition_size = 0;
    PartsRanges::const_iterator best_partition;

    for (auto it = parts_ranges.begin(); it != parts_ranges.end(); ++it)
    {
        if (it->size() <= 1)
            continue;

        if (range_filter && !range_filter(*it))
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

    if (min_partition_size && min_partition_size <= max_total_size_to_merge)
        return {*best_partition};

    return {};
}

}
