#include <Storages/MergeTree/Compaction/MergeSelectors/PartitionStatistics.h>

namespace DB
{

PartitionsStatistics calculateStatisticsForPartitions(const PartsRanges & ranges)
{
    PartitionsStatistics stats;

    for (const auto & range : ranges)
    {
        chassert(!range.empty());
        PartitionStatistics & partition_stats = stats[range.front().info.getPartitionId()];

        partition_stats.part_count += range.size();

        for (const auto & part : range)
        {
            partition_stats.min_age = std::min(partition_stats.min_age, part.age);
            partition_stats.total_size += part.size;
        }
    }

    return stats;
}

}
