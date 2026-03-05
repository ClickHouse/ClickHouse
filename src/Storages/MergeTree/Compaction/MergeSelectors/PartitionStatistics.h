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
};
using PartitionsStatistics = std::unordered_map<std::string, PartitionStatistics>;

PartitionsStatistics calculateStatisticsForPartitions(const PartsRanges & ranges);

}
