#pragma once

#include <Storages/MergeTree/Compaction/PartProperties.h>

#include <unordered_map>
#include <string>

namespace DB
{

struct PartitionStatistics
{
    time_t min_age = std::numeric_limits<time_t>::max();
    size_t part_count = 0;
    size_t total_size = 0;

    /// The latest TTL merge times among all parts of the partition, zero if there is none.
    time_t last_delete_ttl_merge_time = 0;
    time_t last_recompression_ttl_merge_time = 0;
};
using PartitionsStatistics = std::unordered_map<std::string, PartitionStatistics>;

PartitionsStatistics calculateStatisticsForParts(const std::vector<MergeTreeDataPartPtr> & parts, time_t current_time);

}
