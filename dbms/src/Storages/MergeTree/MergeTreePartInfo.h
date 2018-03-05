#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <common/DateLUT.h>


namespace DB
{

/// Information about partition and the range of blocks contained in the part.
/// Allows determining if parts are disjoint or one part fully contains the other.
struct MergeTreePartInfo
{
    String partition_id;
    Int64 min_block = 0;
    Int64 max_block = 0;
    UInt32 level = 0;

    MergeTreePartInfo() = default;
    MergeTreePartInfo(String partition_id_, Int64 min_block_, Int64 max_block_, UInt32 level_)
        : partition_id(std::move(partition_id_)), min_block(min_block_), max_block(max_block_), level(level_)
    {
    }

    bool operator<(const MergeTreePartInfo & rhs) const
    {
        return std::forward_as_tuple(partition_id, min_block, max_block, level)
            < std::forward_as_tuple(rhs.partition_id, rhs.min_block, rhs.max_block, rhs.level);
    }

    bool operator==(const MergeTreePartInfo & rhs) const
    {
        return !(*this < rhs || rhs < *this);
    }

    /// Contains another part (obtained after merging another part with some other)
    bool contains(const MergeTreePartInfo & rhs) const
    {
        return partition_id == rhs.partition_id        /// Parts for different partitions are not merged
            && min_block <= rhs.min_block
            && max_block >= rhs.max_block
            && level >= rhs.level;
    }

    /// True if parts do not intersect in any way.
    bool isDisjoint(const MergeTreePartInfo & rhs) const
    {
        return partition_id != rhs.partition_id
            || min_block > rhs.max_block
            || max_block < rhs.min_block;
    }

    String getPartName() const;
    String getPartNameV0(DayNum_t left_date, DayNum_t right_date) const;
    UInt64 getBlocksCount() const
    {
        return static_cast<UInt64>(max_block - min_block + 1);
    }

    static MergeTreePartInfo fromPartName(const String & part_name, MergeTreeDataFormatVersion format_version);

    static bool tryParsePartName(const String & dir_name, MergeTreePartInfo * part_info, MergeTreeDataFormatVersion format_version);

    static void parseMinMaxDatesFromPartName(const String & part_name, DayNum_t & min_date, DayNum_t & max_date);

    static bool contains(const String & outer_part_name, const String & inner_part_name, MergeTreeDataFormatVersion format_version);
};

}
