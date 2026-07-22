#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <gtest/gtest.h>

using namespace Coordination;
using namespace DB;

TEST(ZooKeeperTest, TestMatchPath)
{
    ASSERT_EQ(matchPath("/path/file", "/path"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/path/file", "/path/"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/path/file", "/"), PathMatchResult::IS_CHILD);
    ASSERT_EQ(matchPath("/", "/"), PathMatchResult::EXACT);
    ASSERT_EQ(matchPath("/path", "/path/"), PathMatchResult::EXACT);
    ASSERT_EQ(matchPath("/path/", "/path"), PathMatchResult::EXACT);
}

TEST(ZooKeeperTest, ExtractZooKeeperPathAndCollapseTrailingSlashes)
{
    using zkutil::extractZooKeeperPathAndCollapseTrailingSlashes;

    /// Any number of trailing slashes collapses to the same canonical path, so that different spellings
    /// of the same keeper path compare equal (used by the SYSTEM DROP REPLICA self-protection guards).
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("/a/b", false), "/a/b");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("/a/b/", false), "/a/b");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("/a/b//", false), "/a/b");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("/a/b///", false), "/a/b");

    /// Auxiliary keeper prefix is stripped and the remaining path is canonicalized the same way.
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("aux:/a/b", false), "/a/b");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("aux:/a/b///", false), "/a/b");

    /// Root-only inputs collapse to "" or "/" (both are rejected by the "empty or root-only" check in the
    /// parser and never equal a real database/table path in the guards): normalizeZooKeeperPath first strips
    /// a single trailing slash, so "/" and "aux:/" become "", while "//"/"///" become "/".
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("/", false), "");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("aux:/", false), "");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("//", false), "/");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("///", false), "/");
    ASSERT_EQ(extractZooKeeperPathAndCollapseTrailingSlashes("aux://", false), "/");
}

TEST(ZooKeeperTest, ListRequestWireRoundTrip)
{
    auto roundtrip = [](OpNum expected_op_num, ListRequestType list_request_type, bool with_stat, bool with_data)
    {
        auto request_ptr = zkutil::makeListRequest("/round/trip", list_request_type, with_stat, with_data);
        auto & request = dynamic_cast<ZooKeeperListRequest &>(*request_ptr);
        EXPECT_EQ(request.getOpNum(), expected_op_num);

        WriteBufferFromOwnString out;
        request.writeImpl(out);

        auto decoded = ZooKeeperRequestFactory::instance().get(expected_op_num);
        auto & decoded_list = dynamic_cast<ZooKeeperListRequest &>(*decoded);
        ReadBufferFromString in(out.str());
        decoded_list.readImpl(in);

        EXPECT_TRUE(in.eof());
        EXPECT_EQ(decoded_list.getOpNum(), expected_op_num);
        EXPECT_EQ(decoded_list.path, request.path);
        EXPECT_EQ(decoded_list.list_request_type, request.list_request_type);
        EXPECT_EQ(decoded_list.with_stat, request.with_stat);
        EXPECT_EQ(decoded_list.with_data, request.with_data);
    };

    roundtrip(OpNum::List, ListRequestType::ALL, false, false);
    roundtrip(OpNum::FilteredList, ListRequestType::PERSISTENT_ONLY, false, false);
    roundtrip(OpNum::FilteredList, ListRequestType::EPHEMERAL_ONLY, false, false);
    roundtrip(OpNum::FilteredListWithStatsAndData, ListRequestType::ALL, true, true);
    roundtrip(OpNum::FilteredListWithStatsAndData, ListRequestType::EPHEMERAL_ONLY, true, false);
    roundtrip(OpNum::FilteredListWithStatsAndData, ListRequestType::ALL, false, true);
}

TEST(ZooKeeperTest, Create2ResponseWireRoundTrip)
{
    ZooKeeperCreate2Response original;
    original.path_created = "/created/node";
    original.zstat.czxid = 42;
    original.zstat.mzxid = 43;
    original.zstat.ctime = 1000;
    original.zstat.mtime = 2000;
    original.zstat.version = 3;
    original.zstat.cversion = 1;
    original.zstat.aversion = 0;
    original.zstat.ephemeralOwner = 0;
    original.zstat.dataLength = 13;
    original.zstat.numChildren = 0;
    original.zstat.pzxid = 44;

    WriteBufferFromOwnString out;
    original.writeImpl(out);

    ZooKeeperCreate2Response decoded;
    ReadBufferFromString in(out.str());
    decoded.readImpl(in);

    EXPECT_TRUE(in.eof());
    EXPECT_EQ(decoded.path_created, original.path_created);
    EXPECT_EQ(decoded.zstat, original.zstat);
    EXPECT_EQ(decoded.zstat.dataLength, 13);
}
