#pragma once

#include <Core/Streaming/Settings.h>

#include <set>
#include <string>

namespace DB
{

class ReadState;

struct ClassifiedPartitions
{
    std::set<std::string> changed_partitions;
    std::set<std::string> unchanged_partitions;
    std::set<std::string> idle_partitions;
};

ClassifiedPartitions classifyPartitions(const ReadState & state, const std::map<String, Int64> & safe_block_numbers, const StreamSettings & stream_settings);

}
