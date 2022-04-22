#include <Common/RangeGenerator.h>
#include <gtest/gtest.h>

using namespace DB;


TEST(RangeGenerator, Common)
{
    RangeGenerator g{25, 10};
    EXPECT_EQ(g.totalRanges(), 3);

    auto r = g.nextRange();
    EXPECT_TRUE(r);
    EXPECT_EQ(*r, RangeGenerator::Range(0, 10));

    r = g.nextRange();
    EXPECT_TRUE(r);
    EXPECT_EQ(*r, RangeGenerator::Range(10, 20));

    r = g.nextRange();
    EXPECT_TRUE(r);
    EXPECT_EQ(*r, RangeGenerator::Range(20, 25));

    r = g.nextRange();
    EXPECT_TRUE(!r);
}
