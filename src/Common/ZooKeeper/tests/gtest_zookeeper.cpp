#include <IO/ReadBufferFromString.h>

#include <Common/ZooKeeper/Types.h>
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
