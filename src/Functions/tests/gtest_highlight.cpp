#include <gtest/gtest.h>
#include <Functions/HighlightImpl.h>

using namespace DB;
using Interval = HighlightImpl::Interval;

namespace DB
{
bool operator==(const HighlightImpl::Interval & a, const HighlightImpl::Interval & b)
{
    return a.begin == b.begin && a.end == b.end;
}
}

static std::vector<Interval> merge(std::vector<Interval> intervals)
{
    HighlightImpl::mergeIntervals(intervals);
    return intervals;
}

TEST(HighlightMergeIntervals, Empty)
{
    auto result = merge({});
    EXPECT_TRUE(result.empty());
}

TEST(HighlightMergeIntervals, Single)
{
    auto result = merge({{0, 5}});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (Interval{0, 5}));
}

TEST(HighlightMergeIntervals, NonOverlapping)
{
    auto result = merge({{0, 3}, {5, 8}});
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], (Interval{0, 3}));
    EXPECT_EQ(result[1], (Interval{5, 8}));
}

TEST(HighlightMergeIntervals, ClassicOverlap)
{
    auto result = merge({{0, 5}, {3, 8}});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (Interval{0, 8}));
}

TEST(HighlightMergeIntervals, AdjacentMerge)
{
    auto result = merge({{0, 5}, {5, 10}});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (Interval{0, 10}));
}

TEST(HighlightMergeIntervals, Containment)
{
    auto result = merge({{0, 10}, {2, 5}});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (Interval{0, 10}));
}

TEST(HighlightMergeIntervals, MultiOverlapChain)
{
    auto result = merge({{0, 3}, {2, 6}, {5, 9}});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (Interval{0, 9}));
}

TEST(HighlightMergeIntervals, UnsortedInput)
{
    auto result = merge({{5, 8}, {0, 3}});
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], (Interval{0, 3}));
    EXPECT_EQ(result[1], (Interval{5, 8}));
}

TEST(HighlightMergeIntervals, Identical)
{
    auto result = merge({{2, 5}, {2, 5}});
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], (Interval{2, 5}));
}

TEST(HighlightMergeIntervals, MixedGroups)
{
    auto result = merge({{0, 3}, {2, 6}, {10, 15}, {14, 20}});
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], (Interval{0, 6}));
    EXPECT_EQ(result[1], (Interval{10, 20}));
}
