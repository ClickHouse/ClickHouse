#include <Storages/MergeTree/TTLMergeSelector.h>

#include <cmath>
#include <algorithm>


namespace DB
{

IMergeSelector::PartsInPartition TTLMergeSelector::select(
    const Partitions & partitions,
    const size_t max_total_size_to_merge)
{
    ssize_t partition_to_merge_index = -1;
    time_t partition_to_merge_ttl = 0;
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        time_t min_ttl = 0;
        for (const auto & part : partitions[i])
            if (part.min_ttl && (!min_ttl || part.min_ttl < min_ttl))
                min_ttl = part.min_ttl;

        if (partition_to_merge_index == -1 || min_ttl < partition_to_merge_ttl)
        {
            partition_to_merge_ttl = min_ttl;
            partition_to_merge_index = i;
        }
    }
    time_t current_time = time(nullptr);
    if (partition_to_merge_index == -1 || partition_to_merge_ttl > current_time)
        return {};

    size_t total_size = 0;
    PartsInPartition parts_to_merge;
    for (const auto & part : partitions[partition_to_merge_index])
    {
        if (part.min_ttl && part.min_ttl < current_time)
        {
            parts_to_merge.emplace_back(part);
            total_size += part.size;
        }

        if (max_total_size_to_merge && total_size > max_total_size_to_merge)
            break;
    }

    return parts_to_merge;
}

}
