#include <gtest/gtest.h>

#include <Common/FairDivision.h>

TEST(FairDivision, SimpleTest)
{
    using namespace DB;

    FairDivision<std::string_view> fd;

    Int64 limit = 700;
    fd.addDemand("A", 300, 100); // fair allocation is 200
    fd.addDemand("B", 400, 200); // fair allocation is 400
    fd.addDemand("C",  50, 100); // fair allocation is 50
    fd.addDemand("D",  50, 100); // fair allocation is 50

    fd.computeFairShares(limit, [](std::string_view name, Int64 committed, Int64 soft_limit, Int64 fair_share)
    {
        if (name == "A") ASSERT_EQ(fair_share, 200);
        if (name == "B") ASSERT_EQ(fair_share, 400);
        if (name == "C") ASSERT_EQ(fair_share, 50);
        if (name == "D") ASSERT_EQ(fair_share, 50);
    });
}
