#pragma once
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <Storages/TTLDescription.h>

#include <map>
#include <optional>

namespace DB
{

/// Minimal and maximal ttl for column or table
struct MergeTreeDataPartTTLInfo
{
    time_t min = 0;
    time_t max = 0;

    /// This TTL was computed on completely expired part. It doesn't make sense
    /// to select such parts for TTL again. But make sense to recalcuate TTL
    /// again for merge with multiple parts.
    std::optional<bool> ttl_finished;
    bool finished() const { return ttl_finished.value_or(false); }

    void update(time_t time);
    void update(const MergeTreeDataPartTTLInfo & other_info);
};

/// Order is important as it would be serialized and hashed for checksums
using TTLInfoMap = std::map<String, MergeTreeDataPartTTLInfo>;

/// PartTTLInfo for all columns and table with minimal ttl for whole part
struct MergeTreeDataPartTTLInfos
{
    TTLInfoMap columns_ttl;
    MergeTreeDataPartTTLInfo table_ttl;

    /// `part_min_ttl` and `part_max_ttl` are TTLs which are used for selecting parts
    /// to merge in order to remove expired rows.
    time_t part_min_ttl = 0;
    time_t part_max_ttl = 0;

    TTLInfoMap rows_where_ttl;

    TTLInfoMap moves_ttl;

    TTLInfoMap recompression_ttl;

    TTLInfoMap group_by_ttl;

    /// Return the smallest max recompression TTL value
    time_t getMinimalMaxRecompressionTTL() const;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
    void update(const MergeTreeDataPartTTLInfos & other_infos);

    /// Has any TTLs which are not calculated on completely expired parts.
    bool hasAnyNonFinishedTTLs() const;

    void updatePartMinMaxTTL(time_t time_min, time_t time_max)
    {
        if (time_min && (!part_min_ttl || time_min < part_min_ttl))
            part_min_ttl = time_min;

        if (time_max && (!part_max_ttl || time_max > part_max_ttl))
            part_max_ttl = time_max;
    }

    bool empty() const
    {
        /// part_min_ttl in minimum of rows, rows_where and group_by TTLs
        return !part_min_ttl && moves_ttl.empty() && recompression_ttl.empty() && columns_ttl.empty();
    }
};

/// Selects the most appropriate TTLDescription using TTL info and current time.
std::optional<TTLDescription> selectTTLDescriptionForTTLInfos(const TTLDescriptions & descriptions, const TTLInfoMap & ttl_info_map, time_t current_time, bool use_max);

}
