#include <gtest/gtest.h>
#include <Storages/MergeTree/MarkRangeChunking.h>

using namespace DB;

namespace
{
size_t totalMarks(const std::vector<MarkRanges> & chunks)
{
    size_t n = 0;
    for (const auto & c : chunks)
        n += c.getNumberOfMarks();
    return n;
}

/// Flatten chunks back into the sequence of individual marks they cover.
std::vector<size_t> flatten(const std::vector<MarkRanges> & chunks)
{
    std::vector<size_t> marks;
    for (const auto & c : chunks)
        for (const auto & r : c)
            for (size_t m = r.begin; m < r.end; ++m)
                marks.push_back(m);
    return marks;
}
}

TEST(MarkRangeChunking, EmptyInputProducesNoChunks)
{
    MarkRanges empty;
    EXPECT_TRUE(splitMarkRanges(empty, 10).empty());
}

TEST(MarkRangeChunking, ZeroTargetReturnsSingleChunkEqualToInput)
{
    MarkRanges in{{0, 100}};
    auto chunks = splitMarkRanges(in, 0);
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].getNumberOfMarks(), 100u);
}

TEST(MarkRangeChunking, TargetLargerThanTotalReturnsSingleChunk)
{
    MarkRanges in{{0, 50}};
    auto chunks = splitMarkRanges(in, 1000);
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].getNumberOfMarks(), 50u);
}

TEST(MarkRangeChunking, SplitsSingleRangeIntoEvenChunks)
{
    MarkRanges in{{0, 100}};
    auto chunks = splitMarkRanges(in, 25);
    ASSERT_EQ(chunks.size(), 4u);
    EXPECT_EQ(totalMarks(chunks), 100u);
}

TEST(MarkRangeChunking, SplitsAcrossMultipleRangesPreservingMarks)
{
    /// Two disjoint ranges: [0,30) and [100,140) -> 70 marks total.
    MarkRanges in{{0, 30}, {100, 140}};
    auto chunks = splitMarkRanges(in, 20);
    EXPECT_EQ(totalMarks(chunks), 70u);

    /// The exact same marks must be covered, in the same order, with no overlaps or gaps.
    std::vector<size_t> expected;
    for (size_t m = 0; m < 30; ++m) expected.push_back(m);
    for (size_t m = 100; m < 140; ++m) expected.push_back(m);
    EXPECT_EQ(flatten(chunks), expected);

    /// No chunk exceeds the target.
    for (const auto & c : chunks)
        EXPECT_LE(c.getNumberOfMarks(), 20u);
}

TEST(MarkRangeChunking, RemainderGoesIntoFinalChunk)
{
    MarkRanges in{{0, 10}};
    auto chunks = splitMarkRanges(in, 3);
    EXPECT_EQ(totalMarks(chunks), 10u);
    EXPECT_EQ(chunks.size(), 4u); /// 3+3+3+1
}

TEST(MarkRangeChunking, TargetOneMakesSingletonChunks)
{
    MarkRanges in{{5, 9}};
    auto chunks = splitMarkRanges(in, 1);
    ASSERT_EQ(chunks.size(), 4u);
    EXPECT_EQ(totalMarks(chunks), 4u);
    for (const auto & c : chunks)
        EXPECT_EQ(c.getNumberOfMarks(), 1u);
}
