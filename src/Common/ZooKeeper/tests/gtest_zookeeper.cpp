#include <gtest/gtest.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>

TEST(ZooKeeperTest, TestMatchPath)
{
    using namespace Coordination;

    ASSERT_EQ(matchPath("/path/file", "/path"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/path/file", "/path/"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/path/file", "/"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/", "/"), PathMatchResult::EXACT);
    ASSERT_EQ(matchPath("/path", "/path/"), PathMatchResult::EXACT);
    ASSERT_EQ(matchPath("/path/", "/path"), PathMatchResult::EXACT);
}
