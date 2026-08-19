#include <gtest/gtest.h>

#include <IO/IntervalSet.h>

using namespace DB;

TEST(IntervalSet, AddMergesOverlapAndAdjacency)
{
    IntervalSet s;
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

TEST(IntervalSet, AddIgnoresEmptyRange)
{
    IntervalSet s;
    s.add({5, 0});
    EXPECT_EQ(s.totalBytes(), 0u);
}

TEST(IntervalSet, SubtractSplitsCoversAndPassesThrough)
{
    IntervalSet s;
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

TEST(IntervalSet, RemoveTrimsSplitsAndClears)
{
    IntervalSet s;
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
