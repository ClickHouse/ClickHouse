#include <IO/Rope.h>
#include <gtest/gtest.h>

using namespace DB;

TEST(Range, Basic)
{
    Range r{100, 50};
    EXPECT_EQ(r.offset, 100);
    EXPECT_EQ(r.size, 50);
    EXPECT_EQ(r.end(), 150);
}

TEST(Range, ZeroSize)
{
    Range r{0, 0};
    EXPECT_EQ(r.end(), 0);
}
