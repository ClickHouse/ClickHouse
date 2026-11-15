#include <Storages/MergeTree/Compaction/PartitionStatistics.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

PartitionsStatistics calculateStatisticsForParts(const std::vector<MergeTreeDataPartPtr> & parts, time_t current_time)
{
    PartitionsStatistics stats;

    for (const auto & part : parts)
    {
        PartitionStatistics & partition_stats = stats[part->info.getPartitionId()];

        partition_stats.part_count += 1;
        partition_stats.min_age = std::min(partition_stats.min_age, current_time - part->modification_time);
        partition_stats.total_size += part->getExistingBytesOnDisk();
    }

    return stats;
}

}
