#include <gtest/gtest.h>

#include <IO/ByteRangeSet.h>

using namespace DB;

TEST(ByteRangeSet, AddMergesOverlapAndAdjacency)
{
    ByteRangeSet s;
    s.add({0, 10});
    s.add({5, 10});   /// [5, 15) overlaps [0, 10) -> merges to [0, 15)
    EXPECT_EQ(s.totalBytes(), 15u);
    EXPECT_TRUE(s.subtract({0, 15}).empty());

    s.add({15, 5});   /// [15, 20) is adjacent -> merges to [0, 20)
    EXPECT_EQ(s.totalBytes(), 20u);
    EXPECT_TRUE(s.subtract({0, 20}).empty());

    s.add({30, 10});   /// [30, 40) is disjoint
    EXPECT_EQ(s.totalBytes(), 30u);
    const auto gaps = s.subtract({0, 40});   /// only [20, 30) is uncovered
    ASSERT_EQ(gaps.size(), 1u);
    EXPECT_EQ(gaps[0].offset, 20u);
    EXPECT_EQ(gaps[0].size, 10u);
}

TEST(ByteRangeSet, AddInAnyOrderKeepsSortedDisjointIntervals)
{
    ByteRangeSet s;
    s.add({50, 10});   /// [50, 60)
    s.add({10, 10});   /// [10, 20), before it
    s.add({30, 5});    /// [30, 35), between
    s.add({18, 14});   /// [18, 32) bridges [10, 20) and [30, 35) -> [10, 35)
    s.add({60, 1});    /// adjacent to [50, 60) -> [50, 61)

    const auto & ranges = s.ranges();
    ASSERT_EQ(ranges.size(), 2u);
    EXPECT_EQ(ranges[0].offset, 10u);
    EXPECT_EQ(ranges[0].size, 25u);
    EXPECT_EQ(ranges[1].offset, 50u);
    EXPECT_EQ(ranges[1].size, 11u);
}

TEST(ByteRangeSet, IntersectClipsAndShiftMoves)
{
    ByteRangeSet s;
    s.add({0, 10});
    s.add({90, 20});
    s.add({150, 5});

    auto inside = s.intersect({5, 95});   /// [5, 100)
    const auto & clipped = inside.ranges();
    ASSERT_EQ(clipped.size(), 2u);
    EXPECT_EQ(clipped[0].offset, 5u);
    EXPECT_EQ(clipped[0].size, 5u);
    EXPECT_EQ(clipped[1].offset, 90u);
    EXPECT_EQ(clipped[1].size, 10u);

    inside.shift(100);
    EXPECT_EQ(inside.ranges()[0].offset, 105u);
    EXPECT_EQ(inside.ranges()[1].offset, 190u);
    EXPECT_EQ(inside.totalBytes(), 15u);

    EXPECT_TRUE(s.intersect({20, 50}).empty());
}

TEST(ByteRangeSet, AddIgnoresEmptyRange)
{
    ByteRangeSet s;
    s.add({5, 0});
    EXPECT_EQ(s.totalBytes(), 0u);
}

TEST(ByteRangeSet, SubtractSplitsCoversAndPassesThrough)
{
    ByteRangeSet s;
    s.add({10, 10});   /// [10, 20)

    const auto split = s.subtract({0, 30});   /// [0, 30) minus [10, 20)
    ASSERT_EQ(split.size(), 2u);
    EXPECT_EQ(split[0].offset, 0u);
    EXPECT_EQ(split[0].size, 10u);
    EXPECT_EQ(split[1].offset, 20u);
    EXPECT_EQ(split[1].size, 10u);

    EXPECT_TRUE(s.subtract({10, 10}).empty());   /// fully covered

    const auto pass = s.subtract({100, 5});   /// disjoint -> returned whole
    ASSERT_EQ(pass.size(), 1u);
    EXPECT_EQ(pass[0].offset, 100u);
    EXPECT_EQ(pass[0].size, 5u);
}

TEST(ByteRangeSet, RemoveTrimsSplitsAndClears)
{
    ByteRangeSet s;
    s.add({0, 30});   /// [0, 30)

    s.remove({10, 10});   /// punch out [10, 20) -> [0, 10) + [20, 30)
    EXPECT_EQ(s.totalBytes(), 20u);
    const auto gaps = s.subtract({0, 30});
    ASSERT_EQ(gaps.size(), 1u);
    EXPECT_EQ(gaps[0].offset, 10u);
    EXPECT_EQ(gaps[0].size, 10u);

    s.remove({0, 5});   /// trim the front of [0, 10) -> [5, 10)
    EXPECT_EQ(s.totalBytes(), 15u);

    s.remove({0, 100});   /// remove everything
    EXPECT_EQ(s.totalBytes(), 0u);
}
