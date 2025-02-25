#include <base/defines.h>

#ifndef NDEBUG

#include <gtest/gtest.h>

#include <vector>

TEST(TestBadVector, Simple)
{
    std::vector<int> x;
    x.push_back(1);
    x.push_back(2);

    EXPECT_DEATH(
        {
            auto sum = x.back();
            x.pop_back();
            sum += x.back();
            x.pop_back();
            sum += x.back();
            ASSERT_NE(sum, 3);
        },
        ".*assert.*");
}

#endif
