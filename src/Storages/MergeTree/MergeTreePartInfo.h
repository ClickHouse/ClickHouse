#pragma once

#include <limits>
#include <optional>
#include <tuple>
#include <vector>
#include <array>
#include <base/types.h>
#include <base/DayNum.h>
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

    bool use_leagcy_max_level = false;  /// For compatibility. TODO remove it

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
        /// Containing part may have equal level iff block numbers are equal (unless level is MAX_LEVEL)
        /// (e.g. all_0_5_2 does not contain all_0_4_2, but all_0_5_3 or all_0_4_2_9 do)
        bool strictly_contains_block_range = (min_block == rhs.min_block && max_block == rhs.max_block) || level > rhs.level
            || level == MAX_LEVEL || level == LEGACY_MAX_LEVEL;
        return partition_id == rhs.partition_id        /// Parts for different partitions are not merged
            && min_block <= rhs.min_block
            && max_block >= rhs.max_block
            && level >= rhs.level
            && mutation >= rhs.mutation
            && strictly_contains_block_range;
    }

    /// Part was created with mutation of parent_candidate part
    bool isMutationChildOf(const MergeTreePartInfo & parent_candidate) const
    {
        return partition_id == parent_candidate.partition_id
            && min_block == parent_candidate.min_block
            && max_block == parent_candidate.max_block
            && level == parent_candidate.level
            && mutation >= parent_candidate.mutation;
    }

    /// Return part mutation version, if part wasn't mutated return zero
    Int64 getMutationVersion() const
    {
        return mutation;
    }

    /// True if parts do not intersect in any way.
    bool isDisjoint(const MergeTreePartInfo & rhs) const
    {
        return partition_id != rhs.partition_id
            || min_block > rhs.max_block
            || max_block < rhs.min_block;
    }

    bool isFakeDropRangePart() const
    {
        /// Another max level was previously used for REPLACE/MOVE PARTITION
        auto another_max_level = std::numeric_limits<decltype(level)>::max();
        return level == MergeTreePartInfo::MAX_LEVEL || level == another_max_level;
    }

    String getPartName() const;
    String getPartNameV0(DayNum left_date, DayNum right_date) const;
    UInt64 getBlocksCount() const
    {
        return static_cast<UInt64>(max_block - min_block + 1);
    }

    /// Simple sanity check for partition ID. Checking that it's not too long or too short, doesn't contain a lot of '_'.
    static void validatePartitionID(const String & partition_id, MergeTreeDataFormatVersion format_version);

    static MergeTreePartInfo fromPartName(const String & part_name, MergeTreeDataFormatVersion format_version);  // -V1071

    static std::optional<MergeTreePartInfo> tryParsePartName(
        std::string_view part_name, MergeTreeDataFormatVersion format_version);

    static void parseMinMaxDatesFromPartName(const String & part_name, DayNum & min_date, DayNum & max_date);

    static bool contains(const String & outer_part_name, const String & inner_part_name, MergeTreeDataFormatVersion format_version);

    static constexpr UInt32 MAX_LEVEL = 999999999;
    static constexpr UInt32 MAX_BLOCK_NUMBER = 999999999;

    static constexpr UInt32 LEGACY_MAX_LEVEL = std::numeric_limits<decltype(level)>::max();
};

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Information about detached part, which includes its prefix in
/// addition to the above fields.
struct DetachedPartInfo : public MergeTreePartInfo
{
    String dir_name;
    String prefix;

    DiskPtr disk;

    /// If false, MergeTreePartInfo is in invalid state (directory name was not successfully parsed).
    bool valid_name;

    static constexpr auto DETACH_REASONS = std::to_array<std::string_view>({
        "broken",
        "unexpected",
        "noquorum",
        "ignored",
        "broken-on-start",
        "clone",
        "attaching",
        "deleting",
        "tmp-fetch",
        "covered-by-broken",
    });

    /// NOTE: It may parse part info incorrectly.
    /// For example, if prefix contains '_' or if DETACH_REASONS doesn't contain prefix.
    // This method has different semantics with MergeTreePartInfo::tryParsePartName.
    // Detached parts are always parsed regardless of their validity.
    // DetachedPartInfo::valid_name field specifies whether parsing was successful or not.
    static DetachedPartInfo parseDetachedPartName(const DiskPtr & disk, std::string_view dir_name, MergeTreeDataFormatVersion format_version);

private:
    void addParsedPartInfo(const MergeTreePartInfo& part);
};

using DetachedPartsInfo = std::vector<DetachedPartInfo>;

}
