#pragma once
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

#include <map>

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
    /// Order is important as it would be serialized and hashed for checksums
    std::map<String, MergeTreeDataPartTTLInfo> columns_ttl;
    MergeTreeDataPartTTLInfo table_ttl;

    /// `part_min_ttl` and `part_max_ttl` are TTLs which are used for selecting parts
    /// to merge in order to remove expired rows.    
    time_t part_min_ttl = 0;
    time_t part_max_ttl = 0;

    /// Order is important as it would be serialized and hashed for checksums
    std::map<String, MergeTreeDataPartTTLInfo> moves_ttl;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
    void update(const MergeTreeDataPartTTLInfos & other_infos);

    void updatePartMinMaxTTL(time_t time_min, time_t time_max)
    {
        if (time_min && (!part_min_ttl || time_min < part_min_ttl))
            part_min_ttl = time_min;

        if (time_max && (!part_max_ttl || time_max > part_max_ttl))
            part_max_ttl = time_max;
    }

    bool empty()
    {
        return !part_min_ttl && moves_ttl.empty();
    }
};

}
