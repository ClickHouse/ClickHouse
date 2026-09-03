#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Common/ZooKeeper/ZooKeeperIO.h>

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

TEST(ZooKeeperTest, MultiRequestRejectsCloseSubrequest)
{
    using namespace Coordination;

    WriteBufferFromOwnString out;
    write(OpNum::Close, out);
    write(false, out);
    write(-1, out);
    write(OpNum::Error, out);
    write(true, out);
    write(-1, out);

    auto request = ZooKeeperRequestFactory::instance().get(OpNum::Multi);
    ReadBufferFromString in(out.str());
    EXPECT_THROW(request->readImpl(in), Coordination::Exception);
}
