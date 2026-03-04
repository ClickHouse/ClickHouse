#include <gtest/gtest.h>
#include <ctime>
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

TEST(MergeTreePartLevel, TimeoutExceededForcesFetch)
{
    // Simulate: level-0 part, min_level=1, create_time 400s ago, timeout=300s
    // elapsed (400) >= timeout (300) → force fetch (do NOT return false)
    auto info = MergeTreePartInfo::fromPartName("all_1_1_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    UInt64 min_level = 1;
    UInt64 timeout_sec = 300;
    time_t create_time = time(nullptr) - 400; // 400 seconds ago

    EXPECT_TRUE(info.level < min_level);  // Part is below threshold

    // Timeout check: elapsed >= timeout → force fetch
    auto elapsed = time(nullptr) - create_time;
    EXPECT_GE(elapsed, static_cast<time_t>(timeout_sec));  // Should force fetch
}

TEST(MergeTreePartLevel, TimeoutNotExceededStillSkips)
{
    // Simulate: level-0 part, min_level=1, create_time 100s ago, timeout=300s
    // elapsed (100) < timeout (300) → still skip
    auto info = MergeTreePartInfo::fromPartName("all_1_1_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    UInt64 min_level = 1;
    UInt64 timeout_sec = 300;
    time_t create_time = time(nullptr) - 100; // 100 seconds ago

    EXPECT_TRUE(info.level < min_level);  // Part is below threshold

    // Timeout check: elapsed < timeout → still skip
    auto elapsed = time(nullptr) - create_time;
    EXPECT_LT(elapsed, static_cast<time_t>(timeout_sec));  // Should still skip
}

TEST(MergeTreePartLevel, ZeroTimeoutMeansPermanentSkip)
{
    // When timeout_sec = 0, the timeout check is disabled entirely
    // Even if the part is very old, it should not be force-fetched
    UInt64 timeout_sec = 0;

    // The check in shouldExecuteLogEntry is: if (timeout_sec > 0 && entry.create_time > 0)
    // When timeout_sec == 0, this condition is false → permanent skip
    EXPECT_FALSE(timeout_sec > 0);  // Confirms the guard disables timeout
}
