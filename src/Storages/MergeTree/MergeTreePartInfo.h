#pragma once

#include <tuple>
#include <common/types.h>
#include <common/DayNum.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>


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
    Int64 mutation = 0;   /// If the part has been mutated or contains mutated parts, is equal to mutation version number.

    MergeTreePartInfo() = default;

    MergeTreePartInfo(String partition_id_, Int64 min_block_, Int64 max_block_, UInt32 level_)
        : partition_id(std::move(partition_id_)), min_block(min_block_), max_block(max_block_), level(level_)
    {
    }

    MergeTreePartInfo(String partition_id_, Int64 min_block_, Int64 max_block_, UInt32 level_, Int64 mutation_)
        : partition_id(std::move(partition_id_)), min_block(min_block_), max_block(max_block_), level(level_), mutation(mutation_)
    {
    }

    bool operator<(const MergeTreePartInfo & rhs) const
    {
        return std::forward_as_tuple(partition_id, min_block, max_block, level, mutation)
            < std::forward_as_tuple(rhs.partition_id, rhs.min_block, rhs.max_block, rhs.level, rhs.mutation);
    }

    bool operator==(const MergeTreePartInfo & rhs) const
    {
        return !(*this != rhs);
    }

    bool operator!=(const MergeTreePartInfo & rhs) const
    {
        return *this < rhs || rhs < *this;
    }

    /// Get block number that can be used to determine which mutations we still need to apply to this part
    /// (all mutations with version greater than this block number).
    Int64 getDataVersion() const { return mutation ? mutation : min_block; }

    /// True if contains rhs (this part is obtained by merging rhs with some other parts or mutating rhs)
    bool contains(const MergeTreePartInfo & rhs) const
    {
        return partition_id == rhs.partition_id        /// Parts for different partitions are not merged
            && min_block <= rhs.min_block
            && max_block >= rhs.max_block
            && level >= rhs.level
            && mutation >= rhs.mutation;
    }

    /// True if parts do not intersect in any way.
    bool isDisjoint(const MergeTreePartInfo & rhs) const
    {
        return partition_id != rhs.partition_id
            || min_block > rhs.max_block
            || max_block < rhs.min_block;
    }

    String getPartName() const;
    String getPartNameV0(DayNum left_date, DayNum right_date) const;
    UInt64 getBlocksCount() const
    {
        return static_cast<UInt64>(max_block - min_block + 1);
    }

    static MergeTreePartInfo fromPartName(const String & part_name, MergeTreeDataFormatVersion format_version);

    static bool tryParsePartName(const String & part_name, MergeTreePartInfo * part_info, MergeTreeDataFormatVersion format_version);

    static void parseMinMaxDatesFromPartName(const String & part_name, DayNum & min_date, DayNum & max_date);

    static bool contains(const String & outer_part_name, const String & inner_part_name, MergeTreeDataFormatVersion format_version);

    static constexpr UInt32 MAX_LEVEL = 999999999;
    static constexpr UInt32 MAX_BLOCK_NUMBER = 999999999;
};

/// Information about detached part, which includes its prefix in
/// addition to the above fields.
struct DetachedPartInfo : public MergeTreePartInfo
{
    String dir_name;
    String prefix;

    String disk;

    /// If false, MergeTreePartInfo is in invalid state (directory name was not successfully parsed).
    bool valid_name;

    static bool tryParseDetachedPartName(const String & dir_name, DetachedPartInfo & part_info, MergeTreeDataFormatVersion format_version);
};

using DetachedPartsInfo = std::vector<DetachedPartInfo>;

}
