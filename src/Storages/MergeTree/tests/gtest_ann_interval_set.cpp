#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/IntervalSet.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{
    using IV = IntervalSet<UInt64>::Interval;

    std::vector<IV> intervalsOf(const IntervalSet<UInt64> & s)
    {
        return {s.intervals().begin(), s.intervals().end()};
    }
}

TEST(IntervalSetTest, EmptyInitially)
{
    IntervalSet<UInt64> s;
    EXPECT_TRUE(s.empty());
    EXPECT_EQ(s.size(), 0u);
    EXPECT_FALSE(s.containsRange(0, 0));
}

TEST(IntervalSetTest, SingleInterval)
{
    IntervalSet<UInt64> s;
    s.addInterval(5, 10);
    EXPECT_EQ(s.size(), 1u);
    EXPECT_TRUE(s.containsRange(5, 5));
    EXPECT_TRUE(s.containsRange(5, 10));
    EXPECT_TRUE(s.containsRange(7, 9));
    EXPECT_FALSE(s.containsRange(4, 10));
    EXPECT_FALSE(s.containsRange(5, 11));
}

TEST(IntervalSetTest, TouchingMerges)
{
    IntervalSet<UInt64> s;
    s.addInterval(1, 3);
    s.addInterval(4, 6);
    ASSERT_EQ(s.size(), 1u);
    EXPECT_EQ(intervalsOf(s)[0], (IV{1, 6}));
}

TEST(IntervalSetTest, OverlappingMerges)
{
    IntervalSet<UInt64> s;
    s.addInterval(1, 5);
    s.addInterval(4, 8);
    ASSERT_EQ(s.size(), 1u);
    EXPECT_EQ(intervalsOf(s)[0], (IV{1, 8}));
}

TEST(IntervalSetTest, ContainmentNoOp)
{
    IntervalSet<UInt64> s;
    s.addInterval(1, 100);
    s.addInterval(20, 30);
    ASSERT_EQ(s.size(), 1u);
    EXPECT_EQ(intervalsOf(s)[0], (IV{1, 100}));
}

TEST(IntervalSetTest, NonAdjacentKept)
{
    IntervalSet<UInt64> s;
    s.addInterval(10, 20);
    s.addInterval(30, 40);
    s.addInterval(1, 5);
    ASSERT_EQ(s.size(), 3u);
    EXPECT_EQ(intervalsOf(s)[0], (IV{1, 5}));
    EXPECT_EQ(intervalsOf(s)[1], (IV{10, 20}));
    EXPECT_EQ(intervalsOf(s)[2], (IV{30, 40}));
}

TEST(IntervalSetTest, MergeSpansMultiple)
{
    IntervalSet<UInt64> s;
    s.addInterval(1, 3);
    s.addInterval(10, 12);
    s.addInterval(20, 22);
    s.addInterval(0, 100);
    ASSERT_EQ(s.size(), 1u);
    EXPECT_EQ(intervalsOf(s)[0], (IV{0, 100}));
}

TEST(IntervalSetTest, ContainsRangeExactBoundary)
{
    IntervalSet<UInt64> s;
    s.addInterval(3, 10);
    EXPECT_TRUE(s.containsRange(5, 8));
    EXPECT_TRUE(s.containsRange(3, 10));
    EXPECT_FALSE(s.containsRange(3, 11));
    EXPECT_FALSE(s.containsRange(2, 10));
    /// Range straddling a gap is not contained.
    s.addInterval(20, 30);
    EXPECT_FALSE(s.containsRange(8, 25));
}

TEST(IntervalSetTest, ReversedIntervalThrows)
{
    IntervalSet<UInt64> s;
    EXPECT_THROW(s.addInterval(10, 5), DB::Exception);
    EXPECT_THROW((void)s.containsRange(10, 5), DB::Exception);
}

TEST(IntervalSetTest, SerializeRoundTrip)
{
    IntervalSet<UInt64> s;
    s.addInterval(1, 3);
    s.addInterval(10, 15);
    s.addInterval(100, 100);

    String blob;
    {
        WriteBufferFromString out(blob);
        s.serialize(out);
    }

    IntervalSet<UInt64> s2;
    ReadBufferFromString in(blob);
    s2.deserialize(in);

    EXPECT_EQ(s.size(), s2.size());
    EXPECT_EQ(intervalsOf(s), intervalsOf(s2));
}

TEST(IntervalSetTest, DeserializeDetectsUnsorted)
{
    /// Hand-craft bytes: count=2, [5,10], [3,4] — not sorted.
    String blob;
    {
        WriteBufferFromString out(blob);
        UInt32 count = 2;
        out.write(reinterpret_cast<const char *>(&count), sizeof(count));
        UInt64 pair[4] = {5, 10, 3, 4};
        out.write(reinterpret_cast<const char *>(pair), sizeof(pair));
    }

    IntervalSet<UInt64> s;
    ReadBufferFromString in(blob);
    EXPECT_THROW(s.deserialize(in), DB::Exception);
}
