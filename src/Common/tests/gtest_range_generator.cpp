#include <Common/RangeGenerator.h>
#include <gtest/gtest.h>

using namespace DB;


TEST(RangeGenerator, Common)
{
    RangeGenerator g{25, 10};
    EXPECT_EQ(g.totalRanges(), 3);

    std::vector<RangeGenerator::Range> ranges{{0, 10}, {10, 20}, {20, 25}};
    for (size_t i = 0; i < 3; ++i)
    {
        auto r = g.nextRange();
        EXPECT_TRUE(r);
        EXPECT_EQ(r, ranges[i]);
    }

    auto r = g.nextRange();
    EXPECT_TRUE(!r);
}
