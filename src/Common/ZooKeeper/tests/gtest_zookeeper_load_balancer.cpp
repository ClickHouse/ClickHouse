#include <cstdint>
#include <memory>
#include <base/getFQDNOrHostName.h>
#include <Common/ZooKeeper/ZooKeeperLoadBalancer.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/TestKeeper.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

using ::testing::_;
using ::testing::Ne;
using ::testing::Return;

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
        (const std::string & address, size_t original_index, bool secure, 
        const zkutil::ZooKeeperArgs & args, std::shared_ptr<ZooKeeperLog> zk_log_), (override));
};

std::string getCurrentTimeWithMilliseconds() {
    // Get current time as time_point
    auto now = std::chrono::system_clock::now();

    // Convert to a time_t, which represents seconds since the Epoch
    auto nowAsTimeT = std::chrono::system_clock::to_time_t(now);

    // Convert to local time
    auto localTime = std::localtime(&nowAsTimeT);

    // Extract milliseconds
    auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    // Use stringstream to format the string
    std::stringstream ss;
    
    // Format the date and time up to seconds
    ss << std::put_time(localTime, "%Y-%m-%d %H:%M:%S");
    
    // Append milliseconds
    ss << '.' << std::setfill('0') << std::setw(3) << milliseconds.count();

    return ss.str();
}

TEST(ZooKeeperLoadBalancer, Basics)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    LOG_INFO(getLogger("debug"), "start debugging, time {} ", getCurrentTimeWithMilliseconds());
    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::IN_ORDER;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    // Destructor of TestKeeper make the test take long time.
    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .Times(1)
        .WillOnce(Return(std::make_unique<Coordination::TestKeeper>(args)));
    auto client = load_balancer->createClient();
    EXPECT_FALSE(client->isClientSessionDeadlineSet());
    LOG_INFO(getLogger("debug"), "finish debugging, time {} ", getCurrentTimeWithMilliseconds());
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

    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .Times(1)
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create("localhost:2182", 1, _, _, _))
        .Times(1)
        .WillOnce(::testing::Return(std::make_unique<Coordination::TestKeeper>(args)));

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

    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create("localhost:2182", 1, _, _, _))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create("localhost:2183", 2, _, _, _))
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

    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create("localhost:2182", 1, _, _, _))
        .WillOnce(::testing::Return(testing::ByMove(std::make_unique<Coordination::TestKeeper>(args))));

    auto client = load_balancer->createClient();
    EXPECT_TRUE(client->isClientSessionDeadlineSet());

    // Even the first rounds 2181 fails, and 2182 succeeds, this time load balancer starts from scratch without memorizing the status.
    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .WillOnce(::testing::Return(testing::ByMove(std::make_unique<Coordination::TestKeeper>(args))));

    client = load_balancer->createClient();
    EXPECT_FALSE(client->isClientSessionDeadlineSet());
}

TEST(ZooKeeperLoadBalancer, RoundRobin)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::ROUND_ROBIN;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    // First keeper fails and second one succeeds as it's round robin.
    auto test_keeper = std::make_unique<Coordination::TestKeeper>(args);
    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    EXPECT_CALL(*mock_factory, create("localhost:2182", 1, _, _, _))
        .WillOnce(::testing::Return(testing::ByMove(std::make_unique<Coordination::TestKeeper>(args))));

    auto client = load_balancer->createClient();

    // For round robin we always treat the current connected one as optimal keeper host.
    EXPECT_FALSE(client->isClientSessionDeadlineSet());

    // Now new connection should choose the third and then first keeper again.
    EXPECT_CALL(*mock_factory, create("localhost:2183", 2, _, _, _))
        .WillOnce(::testing::Return(testing::ByMove(std::make_unique<Coordination::TestKeeper>(args))));
    client = load_balancer->createClient();
    EXPECT_FALSE(client->isClientSessionDeadlineSet());

    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .WillOnce(::testing::Return(testing::ByMove(std::make_unique<Coordination::TestKeeper>(args))));
    client = load_balancer->createClient();
    EXPECT_FALSE(client->isClientSessionDeadlineSet());
}

TEST(ZooKeeperLoadBalancer, FirstOrRandomUseFirst)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::FIRST_OR_RANDOM;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    auto test_keeper = std::make_unique<Coordination::TestKeeper>(args);
    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .WillOnce(::testing::Return(testing::ByMove(std::move(test_keeper))));

    auto client = load_balancer->createClient();
    EXPECT_FALSE(client->isClientSessionDeadlineSet());
}

TEST(ZooKeeperLoadBalancer, FirstOrRandomUseOthers)
{
    using namespace Coordination;
    auto mock_factory = std::make_shared<MockKeeperFactory>();

    auto args = zkutil::ZooKeeperArgs();
    args.get_priority_load_balancing.load_balancing = LoadBalancing::FIRST_OR_RANDOM;
    args.hosts = Strings{"localhost:2181", "localhost:2182", "localhost:2183"};

    auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
    load_balancer->init(args, nullptr);

    auto test_keeper = std::make_unique<Coordination::TestKeeper>(args);
    EXPECT_CALL(*mock_factory, create("localhost:2181", 0, _, _, _))
        .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

    // Criteria here: any Keeper host as long as it's not the first one.
    EXPECT_CALL(*mock_factory, create(Ne("localhost:2181"), Ne(0), _, _, _))
        .WillOnce(::testing::Return(testing::ByMove(std::move(test_keeper))));

    auto client = load_balancer->createClient();

    // FirstOrRandom we only view the first one as the optimal host.
    EXPECT_TRUE(client->isClientSessionDeadlineSet());
}

// TEST(ZooKeeperLoadBalancer, NearestHost)
// {
//     using namespace Coordination;
//     auto mock_factory = std::make_shared<MockKeeperFactory>();

//     auto current_host = getFQDNOrHostName();
//     ASSERT_TRUE(current_host.length() >= 3 && "If the current host is too short, test can't proceed, please fix the test!");

//     // We need to construct the host dynamically because we don't know the value of `getFQDNOrHostName` under different environment.
//     auto hostWithDistance = [](const std::string& host, size_t distance) -> std::string {
//         distance = std::min(distance, host.length());
//         std::string new_host = host;
//         for (size_t i = 0; i < distance; ++i)
//             new_host[i]++;
//         return new_host;
//     };

//     auto args = zkutil::ZooKeeperArgs();
//     args.get_priority_load_balancing.load_balancing = LoadBalancing::NEAREST_HOSTNAME;
//     args.hosts = Strings{
//         hostWithDistance(current_host, 2) + ":2181",
//         hostWithDistance(current_host, 1)+":2182", 
//         hostWithDistance(current_host, 0) + ":2183"
//     };

//     auto load_balancer = std::make_unique<ZooKeeperLoadBalancer>("test", mock_factory);
//     load_balancer->init(args, nullptr);
//     load_balancer->disableDNSCheckForTest();

//     auto test_keeper = std::make_unique<Coordination::TestKeeper>(args);
//     EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
//         return node.original_index == 2 && node.address.port() == 2183;
//     }), ::testing::_, ::testing::_))
//         .WillOnce(::testing::Return(testing::ByMove(std::move(test_keeper))));

//     auto client = load_balancer->createClient();
//     EXPECT_FALSE(client->isClientSessionDeadlineSet());

//     EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
//         return node.original_index == 2 && node.address.port() == 2183;
//     }), ::testing::_, ::testing::_))
//         .WillOnce(::testing::Throw(DB::Exception(ErrorCodes::SOCKET_TIMEOUT, "socket timeout.")));

//     test_keeper = std::make_unique<Coordination::TestKeeper>(args);

//     // Tricky part, SocketAddress throws exception of host not found.
//     // Solution: make the current host address as dependency injection again, construct IP based socket address.
//     // First try the IP based socket address working or not.
//     EXPECT_CALL(*mock_factory, create(::testing::Truly([&](const Coordination::ZooKeeper::Node &node) {
//         return node.address.port() == 2182;
//     }), ::testing::_, ::testing::_))
//         .WillOnce(::testing::Return(testing::ByMove(std::move(test_keeper))));
//     client = load_balancer->createClient();
//     EXPECT_TRUE(client->isClientSessionDeadlineSet());
// }

}
