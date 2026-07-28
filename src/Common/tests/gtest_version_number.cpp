#include <Common/VersionNumber.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(VersionNumber, VersionNumber)
{
    VersionNumber version(1, 2, 3);
    EXPECT_NE(VersionNumber(1, 1, 1), version);
    EXPECT_EQ(VersionNumber(1, 2, 3), version);
    EXPECT_GE(VersionNumber(1, 2, 3), version);
    EXPECT_GT(VersionNumber(1, 2, 4), version);
    EXPECT_LE(VersionNumber(1, 2, 3), version);
    EXPECT_LT(VersionNumber(1, 2, 2), version);
}

TEST(VersionNumber, fromString)
{
    EXPECT_EQ(VersionNumber("1.1.1"), VersionNumber(1, 1, 1));
    EXPECT_EQ(VersionNumber("5.5.13prefix"), VersionNumber(5, 5, 13));

    EXPECT_GT(VersionNumber("1.1.1.1"), VersionNumber(1, 1, 1));
    EXPECT_LT(VersionNumber("1.1"), VersionNumber(1, 1, 0));
    EXPECT_LT(VersionNumber("1"), VersionNumber(1, 0, 0));
    EXPECT_LT(VersionNumber(""), VersionNumber(0, 0, 0));
}
