#pragma once
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

#include <unordered_map>

namespace DB
{

/// Minimal and maximal ttl for column or table
struct MergeTreeDataPartTTLInfo
{
    time_t min = 0;
    time_t max = 0;

    void update(time_t time)
    {
        if (time && (!min || time < min))
            min = time;

        max = std::max(time, max);
    }

    void update(const MergeTreeDataPartTTLInfo & other_info)
    {
        if (other_info.min && (!min || other_info.min < min))
            min = other_info.min;

        max = std::max(other_info.max, max);
    }
};

/// PartTTLInfo for all columns and table with minimal ttl for whole part
struct MergeTreeDataPartTTLInfos
{
    std::unordered_map<String, MergeTreeDataPartTTLInfo> columns_ttl;
    MergeTreeDataPartTTLInfo table_ttl;
    time_t part_min_ttl = 0;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
    void update(const MergeTreeDataPartTTLInfos & other_infos);

    void updatePartMinTTL(time_t time)
    {
        if (time && (!part_min_ttl || time < part_min_ttl))
            part_min_ttl = time;
    }
};

}
