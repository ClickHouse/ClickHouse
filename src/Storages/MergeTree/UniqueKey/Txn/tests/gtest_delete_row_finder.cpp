#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeyDeleteRowFinder.h>

#include <vector>

using namespace DB;
using namespace DB::UniqueKeyTxn;

namespace
{

UniqueKeyDeleteRowFinder::PartRowEntry entry(PartName p, UInt32 r)
{
    return UniqueKeyDeleteRowFinder::PartRowEntry{std::move(p), r};
}

std::vector<UInt64> rowsOf(const UniqueKeyDeleteRowsForPart & e)
{
    std::vector<UInt64> v;
    if (e.rows)
        v = e.rows->toVector();
    return v;
}

}

/// Single part, multiple rows: collapsed into one RoaringBitmap.
TEST(UniqueKeyDeleteRowFinder, SinglePartRoundTripsRoaringBitmap)
{
    std::vector<UniqueKeyDeleteRowFinder::PartRowEntry> pairs = {
        entry("all_1_1_0", 0),
        entry("all_1_1_0", 5),
        entry("all_1_1_0", 12),
        entry("all_1_1_0", 5),    /// dup — Roaring is idempotent, only one bit
    };
    auto r = UniqueKeyDeleteRowFinder::group(pairs, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    ASSERT_EQ(r.by_partition.size(), 1u);

    auto it = r.by_partition.find("all");
    ASSERT_NE(it, r.by_partition.end());
    ASSERT_EQ(it->second.size(), 1u);
    EXPECT_EQ(it->second[0].part_name, "all_1_1_0");
    EXPECT_EQ(rowsOf(it->second[0]), (std::vector<UInt64>{0, 5, 12}));

    /// `total_matched_rows` is the raw pair count, not unique-row count —
    /// the caller logs both shapes.
    EXPECT_EQ(r.stats.total_matched_rows, 4u);
    EXPECT_EQ(r.stats.parts_with_hits, 1u);
}

/// Multiple parts in the same partition: grouped together, sorted by part name.
TEST(UniqueKeyDeleteRowFinder, MultiplePartsSamePartitionSortedDeterministic)
{
    std::vector<UniqueKeyDeleteRowFinder::PartRowEntry> pairs = {
        entry("all_3_3_0", 7),
        entry("all_1_1_0", 1),
        entry("all_2_2_0", 4),
        entry("all_1_1_0", 2),
    };
    auto r = UniqueKeyDeleteRowFinder::group(pairs, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    ASSERT_EQ(r.by_partition.size(), 1u);

    const auto & parts = r.by_partition.at("all");
    ASSERT_EQ(parts.size(), 3u);
    EXPECT_EQ(parts[0].part_name, "all_1_1_0");
    EXPECT_EQ(parts[1].part_name, "all_2_2_0");
    EXPECT_EQ(parts[2].part_name, "all_3_3_0");

    EXPECT_EQ(rowsOf(parts[0]), (std::vector<UInt64>{1, 2}));
    EXPECT_EQ(rowsOf(parts[1]), (std::vector<UInt64>{4}));
    EXPECT_EQ(rowsOf(parts[2]), (std::vector<UInt64>{7}));

    EXPECT_EQ(r.stats.total_matched_rows, 4u);
    EXPECT_EQ(r.stats.parts_with_hits, 3u);
}

/// Parts in different partitions: grouped under separate partition_id keys.
/// Format version >= 1 with custom partitioning means the partition id is
/// the prefix before `_<min>_<max>_<level>`.
TEST(UniqueKeyDeleteRowFinder, MultiplePartitionsBucketedSeparately)
{
    std::vector<UniqueKeyDeleteRowFinder::PartRowEntry> pairs = {
        entry("p1_1_1_0", 10),
        entry("p1_2_2_0", 20),
        entry("p2_5_5_0", 30),
        entry("p2_5_5_0", 31),
    };
    auto r = UniqueKeyDeleteRowFinder::group(pairs, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    ASSERT_EQ(r.by_partition.size(), 2u);

    ASSERT_TRUE(r.by_partition.count("p1") == 1);
    ASSERT_TRUE(r.by_partition.count("p2") == 1);

    const auto & p1 = r.by_partition.at("p1");
    ASSERT_EQ(p1.size(), 2u);
    EXPECT_EQ(p1[0].part_name, "p1_1_1_0");
    EXPECT_EQ(p1[1].part_name, "p1_2_2_0");
    EXPECT_EQ(rowsOf(p1[0]), (std::vector<UInt64>{10}));
    EXPECT_EQ(rowsOf(p1[1]), (std::vector<UInt64>{20}));

    const auto & p2 = r.by_partition.at("p2");
    ASSERT_EQ(p2.size(), 1u);
    EXPECT_EQ(p2[0].part_name, "p2_5_5_0");
    EXPECT_EQ(rowsOf(p2[0]), (std::vector<UInt64>{30, 31}));

    EXPECT_EQ(r.stats.total_matched_rows, 4u);
    EXPECT_EQ(r.stats.parts_with_hits, 3u);
}

/// Malformed `_part` name (cannot parse) is dropped silently from group(),
/// the pure-function seam exercised here; the production driver `find()`
/// fail-closes (throws) on such input, so this leniency only serves the unit
/// test. Other entries are unaffected.
TEST(UniqueKeyDeleteRowFinder, MalformedPartNameDroppedSilently)
{
    std::vector<UniqueKeyDeleteRowFinder::PartRowEntry> pairs = {
        entry("not a valid part name", 1),
        entry("all_1_1_0", 2),
        entry("", 3),
    };
    auto r = UniqueKeyDeleteRowFinder::group(pairs, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);

    ASSERT_EQ(r.by_partition.size(), 1u);
    const auto & parts = r.by_partition.at("all");
    ASSERT_EQ(parts.size(), 1u);
    EXPECT_EQ(parts[0].part_name, "all_1_1_0");
    EXPECT_EQ(rowsOf(parts[0]), (std::vector<UInt64>{2}));

    /// total_matched_rows counts the pre-grouping input (matches what the
    /// internal SELECT returned); parts_with_hits is post-drop.
    EXPECT_EQ(r.stats.total_matched_rows, 3u);
    EXPECT_EQ(r.stats.parts_with_hits, 1u);
}

