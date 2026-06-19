#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

#include <future>

using namespace Coordination;
using namespace DB;

namespace
{

Coordination::TestKeeper makeKeeper(int32_t operation_timeout_ms = DEFAULT_OPERATION_TIMEOUT_MS, std::string chroot = "")
{
    zkutil::ZooKeeperArgs args;
    args.operation_timeout_ms = operation_timeout_ms;
    args.chroot = chroot;

    return Coordination::TestKeeper(args);
}

void create(TestKeeper & keeper, const String & path, const String & data, bool is_ephemeral)
{
    std::promise<CreateResponse> sink;
    std::future<CreateResponse> future = sink.get_future();
    keeper.create(path, data, is_ephemeral, /* is_sequential */ false, {},
        [&](const auto & response) { sink.set_value(std::move(response)); });

    CreateResponse response = future.get();
    ASSERT_EQ(response.error, Error::ZOK);
}

bool exists(TestKeeper & keeper, const String & path)
{
    std::promise<ExistsResponse> sink;
    std::future<ExistsResponse> future = sink.get_future();
    keeper.exists(path, [&](const auto & response) { sink.set_value(std::move(response)); }, WatchCallbackPtrOrEventPtr());

    return future.get().error == Coordination::Error::ZOK;
}

ListResponse list(TestKeeper & keeper, const String & path, ListRequestType list_request_type, bool with_stat, bool with_data)
{
    std::promise<ListResponse> sink;
    std::future<ListResponse> future = sink.get_future();
    keeper.list(path, list_request_type,
        [&](const auto & response) { sink.set_value(std::move(response)); },
        WatchCallbackPtrOrEventPtr(), with_stat, with_data);

    return future.get();
}

}

TEST(TestKeeperTest, JustWorks)
{
    TestKeeper keeper = makeKeeper();

    ASSERT_TRUE(exists(keeper, "/"));
    ASSERT_FALSE(exists(keeper, "/A"));

    create(keeper, "/A", "hello", /*is_ephemeral=*/false);
    ASSERT_TRUE(exists(keeper, "/A"));
}

TEST(TestKeeperTest, FilteredListWithStatsAndDataIsAligned)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/parent", "", /* is_ephemeral */ false);
    create(keeper, "/parent/ephemeral", "ephemeral_data", /* is_ephemeral */ true);
    create(keeper, "/parent/persistent", "persistent_data", /* is_ephemeral */ false);

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::PERSISTENT_ONLY, /* with_stat */ true, /* with_data */ true);

        ASSERT_EQ(response.error, Error::ZOK);
        ASSERT_EQ(response.names, std::vector<std::string>({"persistent"}));

        ASSERT_EQ(response.data.size(), 1u);
        EXPECT_EQ(response.data[0], "persistent_data");

        ASSERT_EQ(response.stats.size(), 1u);
        EXPECT_EQ(response.stats[0].ephemeralOwner, 0);
    }

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::EPHEMERAL_ONLY, /* with_stat */ true, /* with_data */ true);

        ASSERT_EQ(response.error, Error::ZOK);
        EXPECT_EQ(response.names, std::vector<std::string>({"ephemeral"}));

        ASSERT_EQ(response.data.size(), 1u);
        EXPECT_EQ(response.data[0], "ephemeral_data");

        ASSERT_EQ(response.stats.size(), 1u);
        EXPECT_NE(response.stats[0].ephemeralOwner, 0);
    }

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::ALL, /* with_stat */ true, /* with_data */ true);

        ASSERT_EQ(response.error, Error::ZOK);
        ASSERT_EQ(response.names.size(), 2u);
        ASSERT_EQ(response.data.size(), 2u);
        ASSERT_EQ(response.stats.size(), 2u);
    }
}

TEST(TestKeeperTest, FilteredListWithoutStatsAndData)
{
    TestKeeper keeper = makeKeeper();

    create(keeper, "/parent", "", /* is_ephemeral */ false);
    create(keeper, "/parent/ephemeral", "ephemeral_data", /* is_ephemeral */ true);
    create(keeper, "/parent/persistent", "persistent_data", /* is_ephemeral */ false);

    {
        ListResponse response = list(keeper, "/parent", ListRequestType::PERSISTENT_ONLY, /* with_stat */ false, /* with_data */ false);

        ASSERT_EQ(response.error, Error::ZOK);
        ASSERT_EQ(response.names, std::vector<std::string>({"persistent"}));

        EXPECT_TRUE(response.data.empty());
        EXPECT_TRUE(response.stats.empty());
    }
}

TEST(TestKeeperTest, ListRequestWireRoundTrip)
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

        DB::WriteBufferFromOwnString out;
        request.writeImpl(out);

        auto decoded = ZooKeeperRequestFactory::instance().get(expected_op_num);
        auto & decoded_list = dynamic_cast<ZooKeeperListRequest &>(*decoded);
        DB::ReadBufferFromString in(out.str());
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
