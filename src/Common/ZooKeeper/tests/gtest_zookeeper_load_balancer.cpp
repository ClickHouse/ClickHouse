#include <cstdint>
#include <memory>
#include <Common/ZooKeeper/ZooKeeperLoadBalancer.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/TestKeeper.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SOCKET_TIMEOUT;
}
}

namespace Coordination{
class MockKeeperFactory : public IKeeperFactory {
public:
    MOCK_METHOD(std::unique_ptr<Coordination::IKeeper>, create, 
        (const Coordination::ZooKeeper::Node &node,
            zkutil::ZooKeeperArgs &args,
            std::shared_ptr<ZooKeeperLog> zk_log_), (override));
};


TEST(ZooKeeperLoadBalancer, Basics)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::IN_ORDER;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    auto test_keeper = std::make_unique<Coordination::TestKeeper>(args);
    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 0 && node.address.port() == 2181;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Return(testing::ByMove(std::move(test_keeper))));

    auto client = load_balancer->createClient();
    EXPECT_FALSE(client->isClientSessionDeadlineSet());
}

TEST(ZooKeeperLoadBalancer, Suboptimal)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::IN_ORDER;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 0 && node.address.port() == 2181;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    auto test_keeper = std::make_unique<Coordination::TestKeeper>(args);
    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 1 && node.address.port() == 2182;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Return(testing::ByMove(std::move(test_keeper))));

    auto client = load_balancer->createClient();
    EXPECT_TRUE(client->isClientSessionDeadlineSet());
}

TEST(ZooKeeperLoadBalancer, AllFailure)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::IN_ORDER;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 0 && node.address.port() == 2181;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 1 && node.address.port() == 2182;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 2 && node.address.port() == 2183;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    ASSERT_THROW(load_balancer->createClient(),DB::Exception);
}


TEST(ZooKeeperLoadBalancer, StatusAreResetBetweenCalls)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::IN_ORDER;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 0 && node.address.port() == 2181;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 1 && node.address.port() == 2182;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Return(testing::ByMove(std::make_unique<Coordination::TestKeeper>(args))));

    auto client = load_balancer->createClient();
    EXPECT_TRUE(client->isClientSessionDeadlineSet());

    // Even the first rounds 2181 fails, and 2182 succeeds, this time load balancer starts from scratch without memorizing the status.
    EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
        return node.original_index == 0 && node.address.port() == 2181;
    }), ::testing::_, ::testing::_))
        .WillOnce(::testing::Return(testing::ByMove(std::make_unique<Coordination::TestKeeper>(args))));

    client = load_balancer->createClient();
    EXPECT_FALSE(client->isClientSessionDeadlineSet());
}

}
