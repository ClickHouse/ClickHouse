#include <Storages/MergeTree/MergeSelectors/TrivialMergeSelector.h>
#include <Storages/MergeTree/MergeSelectors/MergeSelectorFactory.h>

#include <algorithm>
#include <numeric>

#include <Common/thread_local_rng.h>


namespace DB
{

void registerTrivialMergeSelector(MergeSelectorFactory & factory)
{
    factory.registerPublicSelector("Trivial", MergeSelectorAlgorithm::TRIVIAL, [](const std::any &)
    {
        return std::make_shared<TrivialMergeSelector>();
    });
}

TrivialMergeSelector::PartsRange TrivialMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t max_total_size_to_merge)
{
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

            if (!max_total_size_to_merge || total_size <= max_total_size_to_merge)
            {
                candidates.emplace_back(partition.data() + left, partition.data() + right);
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

        if (right < partition.size() && partition[right].level < partition[left].level)
            left = right;
    }

    if (candidates.empty())
        return {};

    if (candidates.size() == 1)
        return candidates[0];

    return candidates[thread_local_rng() % candidates.size()];
}

}
