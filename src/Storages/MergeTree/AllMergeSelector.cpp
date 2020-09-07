#include <Storages/MergeTree/AllMergeSelector.h>

#include <cmath>


namespace DB
{

AllMergeSelector::PartsInPartition AllMergeSelector::select(
    const Partitions & partitions,
    const size_t /*max_total_size_to_merge*/)
{
    size_t min_partition_size = 0;
    Partitions::const_iterator best_partition;

    for (auto it = partitions.begin(); it != partitions.end(); ++it)
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
