#include <Storages/MergeTree/Compaction/MergeSelectors/TrivialMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>

#include <Common/thread_local_rng.h>

#include <algorithm>

namespace DB
{

void registerTrivialMergeSelector(MergeSelectorFactory & factory)
{
    factory.registerPublicSelector("Trivial", MergeSelectorAlgorithm::TRIVIAL, [](const std::any &)
    {
        return std::make_shared<TrivialMergeSelector>();
    });
}

PartsRanges TrivialMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeSizes & max_merge_sizes,
    const RangeFilter & range_filter) const
{
    chassert(max_merge_sizes.size() == 1, "Multi Select is not supported for TrivialMergeSelector");
    const size_t max_total_size_to_merge = max_merge_sizes[0];

    size_t num_partitions = parts_ranges.size();
    if (num_partitions == 0)
        return {};

    /// Sort partitions from the largest to smallest in the number of parts.
    std::vector<size_t> sorted_partition_indices;
    sorted_partition_indices.reserve(num_partitions);
    for (size_t i = 0; i < num_partitions; ++i)
        if (parts_ranges[i].size() >= settings.num_parts_to_merge)
            sorted_partition_indices.emplace_back(i);

    if (sorted_partition_indices.empty())
        return {};

    std::sort(sorted_partition_indices.begin(), sorted_partition_indices.end(),
        [&](size_t i, size_t j){ return parts_ranges[i].size() > parts_ranges[j].size(); });

    size_t partition_idx = 0;
    size_t left = 0;
    size_t right = 0;

    std::vector<PartsRange> candidates;
    while (candidates.size() < settings.num_ranges_to_choose)
    {
        const PartsRange & partition = parts_ranges[partition_idx];

        if (1 + right - left == settings.num_parts_to_merge)
        {
            ++right;

            size_t total_size = 0;
            for (size_t i = left; i < right; ++i)
                total_size += partition[i].size;

            const auto range_begin = partition.begin() + left;
            const auto range_end = partition.begin() + right;

            if (total_size <= max_total_size_to_merge && (!range_filter || range_filter({range_begin, range_end})))
            {
                candidates.emplace_back(range_begin, range_end);
                if (candidates.size() == settings.num_ranges_to_choose)
                    break;
            }

            left = right;
        }

        if (partition.size() - left < settings.num_parts_to_merge)
        {
            ++partition_idx;
            if (partition_idx == sorted_partition_indices.size())
                break;

            left = 0;
            right = 0;
        }

        ++right;

        if (right < partition.size() && partition[right].info.level < partition[left].info.level)
            left = right;
    }

    if (candidates.empty())
        return {};

    if (candidates.size() == 1)
        return {candidates[0]};

    return {candidates[thread_local_rng() % candidates.size()]};
}

}
