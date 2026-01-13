#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <gtest/gtest.h>

#include <future>
#include <memory>

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

}

TEST(TestKeeperTest, JustWorks)
{
    TestKeeper keeper = makeKeeper();

    std::promise<ExistsResponse> sink;
    std::future<ExistsResponse> future = sink.get_future();
    keeper.exists("/", [&](const auto & response) { sink.set_value(std::move(response)); }, WatchCallbackPtrOrEventPtr());

    ExistsResponse response = future.get();
    ASSERT_EQ(response.error, Error::ZOK);
}

TEST(TestKeeperTest, SimpleMulti)
{
    TestKeeper keeper = makeKeeper();

    MultiResponse response = [&]() {
        std::promise<MultiResponse> multi_sink;
        Requests requests = {
            zkutil::makeCreateRequest("/A", "test keeper should work with normal keeper requests", zkutil::CreateMode::Persistent)
        };
        keeper.multi(requests, /*atomic=*/true, [&](const auto & resp) { multi_sink.set_value(std::move(resp)); });
        return multi_sink.get_future().get();
    }();

    ASSERT_EQ(response.error, Error::ZOK);
    ASSERT_EQ(response.responses.size(), 1);
    ASSERT_EQ(response.responses[0]->error, Error::ZOK);
}

TEST(TestKeeperTest, MultiMulti)
{
    TestKeeper keeper = makeKeeper();

    MultiResponse multi_response = [&]() {
        std::promise<MultiResponse> multi_sink;
        Requests requests = {
            zkutil::makeCreateRequest("/A", "1", zkutil::CreateMode::Persistent),
            zkutil::makeCheckRequest("/A", 0),
            zkutil::makeCreateRequest("/B", "2", zkutil::CreateMode::Persistent),
            zkutil::makeCheckRequest("/B", 0),
            zkutil::makeCreateRequest("/C", "3", zkutil::CreateMode::Persistent),
            zkutil::makeCheckRequest("/C", 0),
        };
        keeper.multi(requests, /*atomic=*/true, [&](const auto & resp) { multi_sink.set_value(std::move(resp)); });
        return multi_sink.get_future().get();
    }();

    ASSERT_EQ(multi_response.error, Error::ZOK);
    ASSERT_EQ(multi_response.responses.size(), 6);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[2]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[3]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[4]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[5]->error, Error::ZOK);

    GetResponse get_response = [&]()
    {
        std::promise<GetResponse> sink;
        keeper.get("/B", [&](const auto & resp) { sink.set_value(std::move(resp)); }, WatchCallbackPtrOrEventPtr());
        return sink.get_future().get();
    }();

    ASSERT_EQ(get_response.error, Error::ZOK);
    ASSERT_EQ(get_response.data, "2");
}

TEST(TestKeeperTest, NestedMulti)
{
    TestKeeper keeper = makeKeeper();

    MultiResponse multi_response = [&]() {
        std::promise<MultiResponse> multi_sink;
        Requests requests = {
            std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                zkutil::makeCreateRequest("/A", "1", zkutil::CreateMode::Persistent),
                std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                    zkutil::makeCreateRequest("/B", "2", zkutil::CreateMode::Persistent),
                    std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                        zkutil::makeCreateRequest("/C", "3", zkutil::CreateMode::Persistent),
                    }, ACLs{}),
                }, ACLs{}),
            }, ACLs{}),
        };
        keeper.multi(requests, /*atomic=*/true, [&](const auto & resp) { multi_sink.set_value(std::move(resp)); });
        return multi_sink.get_future().get();
    }();

    ASSERT_EQ(multi_response.error, Error::ZOK);
    ASSERT_EQ(multi_response.responses.size(), 1);

    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    ASSERT_EQ(std::dynamic_pointer_cast<MultiResponse>(multi_response.responses[0])->responses.size(), 2);

    GetResponse get_response = [&]()
    {
        std::promise<GetResponse> sink;
        keeper.get("/C", [&](const auto & resp) { sink.set_value(std::move(resp)); }, WatchCallbackPtrOrEventPtr());
        return sink.get_future().get();
    }();

    ASSERT_EQ(get_response.data, "3");
}

TEST(TestKeeperTest, NestedMultiError)
{
    TestKeeper keeper = makeKeeper();

    MultiResponse multi_response = [&]() {
        std::promise<MultiResponse> multi_sink;
        Requests requests = {
            std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                zkutil::makeCreateRequest("/A", "1", zkutil::CreateMode::Persistent),
                std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                    zkutil::makeCreateRequest("/B", "2", zkutil::CreateMode::Persistent),
                    zkutil::makeCheckRequest("/A", 42),
                    std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                        zkutil::makeCreateRequest("/C", "3", zkutil::CreateMode::Persistent),
                    }, ACLs{}),
                }, ACLs{}),
            }, ACLs{}),
        };
        keeper.multi(requests, /*atomic=*/true, [&](const auto & resp) { multi_sink.set_value(std::move(resp)); });
        return multi_sink.get_future().get();
    }();

    ASSERT_EQ(multi_response.error, Error::ZBADVERSION);
    ASSERT_EQ(multi_response.responses.size(), 1);

    for (std::string node : {"/A", "/B", "/C"})
    {
        GetResponse get_response = [&]()
        {
            std::promise<GetResponse> sink;
            keeper.get(node, [&](const auto & resp) { sink.set_value(std::move(resp)); }, WatchCallbackPtrOrEventPtr());
            return sink.get_future().get();
        }();

        ASSERT_EQ(get_response.error, Error::ZNONODE);
    }
}

TEST(TestKeeperTest, NonAtomicMulti)
{
    TestKeeper keeper = makeKeeper();

    MultiResponse multi_response = [&]() {
        std::promise<MultiResponse> multi_sink;
        Requests requests = {
            zkutil::makeCreateRequest("/A", "A", zkutil::CreateMode::Persistent),
            zkutil::makeCreateRequest("/B", "B", zkutil::CreateMode::Persistent),
            std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                zkutil::makeCreateRequest("/X1", "1", zkutil::CreateMode::Persistent),
                zkutil::makeCreateRequest("/A", "1", zkutil::CreateMode::Persistent),
                zkutil::makeCreateRequest("/X2", "1", zkutil::CreateMode::Persistent),
            }, ACLs{}),
            zkutil::makeCreateRequest("/C", "C", zkutil::CreateMode::Persistent),
            std::make_shared<ZooKeeperMultiRequest>(Coordination::Requests{
                zkutil::makeCreateRequest("/X3", "1", zkutil::CreateMode::Persistent),
                zkutil::makeCreateRequest("/A", "1", zkutil::CreateMode::Persistent),
            }, ACLs{}),
        };
        keeper.multi(requests, /*atomic=*/false, [&](const auto & resp) { multi_sink.set_value(std::move(resp)); });
        return multi_sink.get_future().get();
    }();

    ASSERT_EQ(multi_response.error, Error::ZNODEEXISTS);
    ASSERT_EQ(multi_response.responses.size(), 5);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[2]->error, Error::ZNODEEXISTS);
    ASSERT_EQ(multi_response.responses[3]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[4]->error, Error::ZNODEEXISTS);

    for (std::string node : {"/A", "/B", "/C"})
    {
        GetResponse get_response = [&]()
        {
            std::promise<GetResponse> sink;
            keeper.get(node, [&](const auto & resp) { sink.set_value(std::move(resp)); }, WatchCallbackPtrOrEventPtr());
            return sink.get_future().get();
        }();

        ASSERT_EQ(get_response.data[0], node[1]);
    }

    for (std::string node : {"/X1", "/X2", "/X3"})
    {
        GetResponse get_response = [&]()
        {
            std::promise<GetResponse> sink;
            keeper.get(node, [&](const auto & resp) { sink.set_value(std::move(resp)); }, WatchCallbackPtrOrEventPtr());
            return sink.get_future().get();
        }();

        ASSERT_EQ(get_response.error, Error::ZNONODE);
    }
}
