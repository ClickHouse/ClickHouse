#pragma once

#include <Core/Types.h>
#include <common/DateLUT.h>

namespace DB
{

/// Information about partition and the range of blocks contained in the part.
/// Allows determining if parts are disjoint or one part fully contains the other.
struct MergeTreePartInfo
{
    String partition_id;
    Int64 min_block;
    Int64 max_block;
    UInt32 level;

    bool operator<(const MergeTreePartInfo & rhs) const
    {
        return std::forward_as_tuple(partition_id, min_block, max_block, level)
            < std::forward_as_tuple(rhs.partition_id, rhs.min_block, rhs.max_block, rhs.level);
    }

    /// Contains another part (obtained after merging another part with some other)
    bool contains(const MergeTreePartInfo & rhs) const
    {
        return partition_id == rhs.partition_id        /// Parts for different partitions are not merged
            && min_block <= rhs.min_block
            && max_block >= rhs.max_block
            && level >= rhs.level;
    }

    static MergeTreePartInfo fromPartName(const String & part_name);

    static bool tryParsePartName(const String & dir_name, MergeTreePartInfo * part_info);

    static void parseMinMaxDatesFromPartName(const String & dir_name, DayNum_t & min_date, DayNum_t & max_date);

    static bool contains(const String & outer_part_name, const String & inner_part_name);

    static String getPartName(DayNum_t left_date, DayNum_t right_date, Int64 left_id, Int64 right_id, UInt64 level);
};

}
