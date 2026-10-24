#include <gtest/gtest.h>

#include <IO/ByteRangeSet.h>

#include <random>

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
    EXPECT_TRUE(s.intersect({5, 0}).empty());     /// empty, inside [0, 10)
    EXPECT_TRUE(s.intersect({10, 80}).empty());   /// touches [0, 10) and [90, 110) only
    EXPECT_EQ(s.intersect({0, 200}).totalBytes(), s.totalBytes());
}

TEST(ByteRangeSet, DescribeShowsAllRangesOrBothEndsOfLongSets)
{
    ByteRangeSet s;
    EXPECT_EQ(s.describe(), "0 ranges");

    s.add({0, 10});
    s.add({20, 5});
    EXPECT_EQ(s.describe(), "2 ranges: [0, 10), [20, 25)");

    ByteRangeSet many;
    for (size_t i = 0; i < 12; ++i)
        many.add({i * 10, 5});
    EXPECT_EQ(many.describe(),
        "12 ranges: [0, 5), [10, 15), [20, 25), [30, 35), ..., [80, 85), [90, 95), [100, 105), [110, 115)");

    ByteRangeSet nine;
    for (size_t i = 0; i < 9; ++i)
        nine.add({i * 10, 5});
    EXPECT_EQ(nine.describe().find("..."), String::npos);
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

TEST(ByteRangeSet, ContiguousEndBridgesNarrowGaps)
{
    ByteRangeSet s;
    s.add({10, 10});   /// [10, 20)
    s.add({25, 5});    /// [25, 30), 5 bytes after
    s.add({50, 10});   /// [50, 60), 20 bytes after

    /// A gap of at most 5 bytes joins [10, 20) and [25, 30); the gap of 20 bytes ends the run.
    EXPECT_EQ(s.contiguousEnd(10, 5), 30u);
    EXPECT_EQ(s.contiguousEnd(19, 5), 30u);
    EXPECT_EQ(s.contiguousEnd(22, 5), 30u);   /// in the narrow gap: the run that follows
    EXPECT_EQ(s.contiguousEnd(50, 5), 60u);
    EXPECT_EQ(s.contiguousEnd(10, 0), 20u);   /// no gap is narrow enough
    EXPECT_EQ(s.contiguousEnd(10, 20), 60u);  /// every gap is

    /// An offset in a wide gap, or past the last interval, is in no run.
    EXPECT_EQ(s.contiguousEnd(35, 5), std::nullopt);
    EXPECT_EQ(s.contiguousEnd(45, 5), 60u);   /// 5 bytes before [50, 60)
    EXPECT_EQ(s.contiguousEnd(60, 100), std::nullopt);
    EXPECT_EQ(s.contiguousEnd(0, 5), std::nullopt);
    EXPECT_EQ(s.contiguousEnd(5, 5), 30u);

    EXPECT_EQ(ByteRangeSet{}.contiguousEnd(0, 100), std::nullopt);
}

TEST(ByteRangeSet, CoalesceKeepsContiguousEnd)
{
    ByteRangeSet s;
    s.add({10, 10});   /// [10, 20)
    s.add({25, 5});    /// [25, 30), 5 bytes after
    s.add({50, 10});   /// [50, 60), 20 bytes after
    const ByteRangeSet runs = s.coalesce(5);
    ASSERT_EQ(runs.ranges().size(), 2u);
    EXPECT_EQ(runs.ranges()[0].offset, 10u);
    EXPECT_EQ(runs.ranges()[0].end(), 30u);
    EXPECT_EQ(runs.ranges()[1].offset, 50u);
    EXPECT_EQ(runs.ranges()[1].end(), 60u);
    EXPECT_TRUE(ByteRangeSet{}.coalesce(100).empty());

    /// `ReaderExecutor` looks run ends up in the coalesced set: every offset must get the answer of the walk.
    std::mt19937 rng(42); /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp) deterministic seed, so a failure is reproducible
    for (size_t round = 0; round < 200; ++round)
    {
        ByteRangeSet set;
        for (size_t i = 0; i < 20; ++i)
            set.add({rng() % 1000, 1 + rng() % 30});
        const size_t max_gap = rng() % 40;
        const ByteRangeSet coalesced = set.coalesce(max_gap);
        for (size_t offset = 0; offset < 1100; ++offset)
            ASSERT_EQ(coalesced.contiguousEnd(offset, max_gap), set.contiguousEnd(offset, max_gap))
                << "round " << round << ", offset " << offset << ", max_gap " << max_gap << ", set " << set.describe();
    }
}
