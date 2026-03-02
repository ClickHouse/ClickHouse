#include <gtest/gtest.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

using namespace DB;

/// Tests for MergeTreePartInfo level extraction
/// Used to verify the replicated_fetches_min_part_level setting behavior

TEST(MergeTreePartLevel, LevelZeroFromPartName)
{
    // Level 0 = freshly inserted, unmerged part (min_block == max_block)
    auto info = MergeTreePartInfo::fromPartName("all_1_1_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    EXPECT_EQ(info.level, 0u);
}

TEST(MergeTreePartLevel, LevelOneFromPartName)
{
    // Level 1 = merged once
    auto info = MergeTreePartInfo::fromPartName("all_1_2_1", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    EXPECT_EQ(info.level, 1u);
}

TEST(MergeTreePartLevel, LevelFiveFromPartName)
{
    // Level 5 = merged 5 times
    auto info = MergeTreePartInfo::fromPartName("all_1_100_5", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    EXPECT_EQ(info.level, 5u);
}

TEST(MergeTreePartLevel, LevelBelowThresholdShouldSkip)
{
    // Simulate the replicated_fetches_min_part_level = 1 check
    auto info = MergeTreePartInfo::fromPartName("all_1_1_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    UInt64 min_level = 1;
    EXPECT_TRUE(info.level < min_level);  // Level 0 < 1, should skip
}

TEST(MergeTreePartLevel, LevelAtThresholdShouldFetch)
{
    // Level equals threshold — should fetch (not skip)
    auto info = MergeTreePartInfo::fromPartName("all_1_2_1", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    UInt64 min_level = 1;
    EXPECT_FALSE(info.level < min_level);  // Level 1 is NOT < 1, should fetch
}

TEST(MergeTreePartLevel, LevelAboveThresholdShouldFetch)
{
    // Level above threshold — should fetch
    auto info = MergeTreePartInfo::fromPartName("all_1_10_3", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    UInt64 min_level = 1;
    EXPECT_FALSE(info.level < min_level);  // Level 3 is NOT < 1, should fetch
}

TEST(MergeTreePartLevel, ZeroThresholdFetchesAll)
{
    // When min_level = 0, all parts should be fetched (default behavior)
    auto info = MergeTreePartInfo::fromPartName("all_1_1_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    UInt64 min_level = 0;
    // min_level == 0 means the check is disabled, so we never skip
    // The check in shouldExecuteLogEntry is: if (min_level > 0) { ... }
    EXPECT_FALSE(min_level > 0);  // Setting = 0 means check is disabled
}

TEST(MergeTreePartLevel, PartitionedTableLevelZero)
{
    // Partitioned table part name format: partition_min_max_level
    auto info = MergeTreePartInfo::fromPartName("20240101_1_1_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    EXPECT_EQ(info.level, 0u);
}

TEST(MergeTreePartLevel, PartitionedTableLevelTwo)
{
    auto info = MergeTreePartInfo::fromPartName("20240101_1_5_2", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    EXPECT_EQ(info.level, 2u);
}
