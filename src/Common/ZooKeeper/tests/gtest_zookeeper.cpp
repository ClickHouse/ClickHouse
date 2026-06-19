#include <IO/ReadBufferFromString.h>

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
    auto roundtrip = [](OpNum expected_op_num, std::optional<ListRequestType> list_request_type,
                        std::optional<bool> with_stat, std::optional<bool> with_data)
    {
        ZooKeeperListRequest request;
        request.path = "/round/trip";
        request.has_watch = true;
        request.list_request_type = list_request_type;
        request.with_stat = with_stat;
        request.with_data = with_data;
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
        EXPECT_EQ(decoded_list.list_request_type, list_request_type);
        EXPECT_EQ(decoded_list.with_stat, with_stat);
        EXPECT_EQ(decoded_list.with_data, with_data);
    };

    roundtrip(OpNum::List, std::nullopt, std::nullopt, std::nullopt);
    roundtrip(OpNum::FilteredList, ListRequestType::ALL, std::nullopt, std::nullopt);
    roundtrip(OpNum::FilteredList, ListRequestType::PERSISTENT_ONLY, std::nullopt, std::nullopt);
    roundtrip(OpNum::FilteredListWithStatsAndData, ListRequestType::EPHEMERAL_ONLY, true, true);
    roundtrip(OpNum::FilteredListWithStatsAndData, ListRequestType::ALL, true, false);
}
