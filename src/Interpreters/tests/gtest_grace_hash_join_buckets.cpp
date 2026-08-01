#include <gtest/gtest.h>

#include <Interpreters/GraceHashJoin.h>

#include <limits>

using namespace DB;


TEST(GraceHashJoinBuckets, PreservesExplicitValues)
{
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(1, 1024), 1);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(3, 1024), 4);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(3, 3), 2);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(2048, 1024), 1024);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(2, 1024, 1000000, 1), 2);
}

TEST(GraceHashJoinBuckets, UsesSizeEstimateInAutoMode)
{
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024), 1);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024, 999, 1000), 1);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024, 1000, 1000), 2);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024, 4000, 1000), 8);
}

TEST(GraceHashJoinBuckets, ClampsAutoValueWithoutOverflow)
{
    EXPECT_EQ(
        GraceHashJoin::getInitialNumBuckets(0, 8, std::numeric_limits<size_t>::max(), 1),
        8);
}
