#include <gtest/gtest.h>

#include <random>

#include <Common/PlainMultiSet.h>

using namespace DB;


TEST(PlainMultiSet, Simple)
{
    PlainMultiSet<int> set(10);

    ASSERT_TRUE(set.tryPush(1));
    ASSERT_TRUE(set.tryPush(1));
    ASSERT_TRUE(set.tryPush(2));
    ASSERT_TRUE(set.tryPush(3));

    ASSERT_TRUE(set.has(1));
    ASSERT_TRUE(set.has(2));
    ASSERT_TRUE(set.has(3));
}
