#include <Storages/MergeTree/TTLMergeSelector.h>

#include <cmath>
#include <algorithm>


namespace DB
{

IMergeSelector::PartsInPartition TTLMergeSelector::select(
    const Partitions & partitions,
    const size_t max_total_size_to_merge)
{
    using Iterator = IMergeSelector::PartsInPartition::const_iterator;
    Iterator best_begin;
    ssize_t partition_to_merge_index = -1;
    time_t partition_to_merge_min_ttl = 0;

    for (size_t i = 0; i < partitions.size(); ++i)
    {
        for (auto it = partitions[i].begin(); it != partitions[i].end(); ++it)
        {
            time_t ttl = only_drop_parts ? it->max_ttl : it->min_ttl;

            if (ttl && (partition_to_merge_index == -1 || ttl < partition_to_merge_min_ttl))
            {
                partition_to_merge_min_ttl = ttl;
                partition_to_merge_index = i;
                best_begin = it;
            }
        }
    }

    if (partition_to_merge_index == -1 || partition_to_merge_min_ttl > current_time)
        return {};

    const auto & best_partition = partitions[partition_to_merge_index];
    Iterator best_end = best_begin + 1;
    size_t total_size = 0;

    while (true)
    {
        time_t ttl = only_drop_parts ? best_begin->max_ttl : best_begin->min_ttl;

        if (!ttl || ttl > current_time
            || (max_total_size_to_merge && total_size > max_total_size_to_merge))
        {
            ++best_begin;
            break;
        }

        total_size += best_begin->size;
        if (best_begin == best_partition.begin())
            break;

        --best_begin;
    }

    while (best_end != best_partition.end())
    {
        time_t ttl = only_drop_parts ? best_end->max_ttl : best_end->min_ttl;

        if (!ttl || ttl > current_time
            || (max_total_size_to_merge && total_size > max_total_size_to_merge))
            break;

        total_size += best_end->size;
        ++best_end;
    }

    return PartsInPartition(best_begin, best_end);
}

}
