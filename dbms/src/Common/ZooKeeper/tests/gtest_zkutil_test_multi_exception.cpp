#include <Common/typeid_cast.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/StringUtils/StringUtils.h>
#include <iostream>
#include <chrono>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#include <Common/ShellCommand.h>


#pragma GCC diagnostic pop

using namespace DB;

TEST(zkutil, zookeeper_connected)
{
    auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");
    try
    {
        zookeeper->exists("/");
    }
    catch (...)
    {
        std::cerr << "No zookeeper. skip tests." << std::endl;
        exit(0);
    }
}

TEST(zkutil, multi_nice_exception_msg)
{
    auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");

    zkutil::Requests ops;

    ASSERT_NO_THROW(
        zookeeper->tryRemoveRecursive("/clickhouse_test/zkutil_multi");

        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi", "_", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/a", "_", zkutil::CreateMode::Persistent));
        zookeeper->multi(ops);
    );

    try
    {
        ops.clear();
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/c", "_", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeRemoveRequest("/clickhouse_test/zkutil_multi/c", -1));
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/a", "BadBoy", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/b", "_", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/a", "_", zkutil::CreateMode::Persistent));

        zookeeper->multi(ops);
        FAIL();
    }
    catch (...)
    {
        zookeeper->tryRemoveRecursive("/clickhouse_test/zkutil_multi");

        String msg = getCurrentExceptionMessage(false);

        bool msg_has_reqired_patterns = msg.find("/clickhouse_test/zkutil_multi/a") != std::string::npos && msg.find("#2") != std::string::npos;
        EXPECT_TRUE(msg_has_reqired_patterns) << msg;
    }
}


TEST(zkutil, multi_async)
{
    auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");
    zkutil::Requests ops;

    zookeeper->tryRemoveRecursive("/clickhouse_test/zkutil_multi");

    {
        ops.clear();
        auto fut = zookeeper->asyncMulti(ops);
    }

    {
        ops.clear();
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi", "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/a", "", zkutil::CreateMode::Persistent));

        auto fut = zookeeper->tryAsyncMulti(ops);
        ops.clear();

        auto res = fut.get();
        ASSERT_TRUE(res.error == ZooKeeperImpl::ZooKeeper::ZOK);
        ASSERT_EQ(res.responses.size(), 2);
    }

    EXPECT_ANY_THROW
    (
        std::vector<std::future<ZooKeeperImpl::ZooKeeper::MultiResponse>> futures;

        for (size_t i = 0; i < 10000; ++i)
        {
            ops.clear();
            ops.emplace_back(zkutil::makeRemoveRequest("/clickhouse_test/zkutil_multi", -1));
            ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi", "_", zkutil::CreateMode::Persistent));
            ops.emplace_back(zkutil::makeCheckRequest("/clickhouse_test/zkutil_multi", -1));
            ops.emplace_back(zkutil::makeSetRequest("/clickhouse_test/zkutil_multi", "xxx", 42));
            ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/a", "_", zkutil::CreateMode::Persistent));

            futures.emplace_back(zookeeper->asyncMulti(ops));
        }

        futures[0].get();
    );

    /// Check there are no segfaults for remaining 999 futures
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);

    {
        ops.clear();
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi", "_", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest("/clickhouse_test/zkutil_multi/a", "_", zkutil::CreateMode::Persistent));

        auto fut = zookeeper->tryAsyncMulti(ops);
        ops.clear();

        auto res = fut.get();
        ASSERT_TRUE(res.error == ZooKeeperImpl::ZooKeeper::ZNODEEXISTS);
        ASSERT_EQ(res.responses.size(), 2);
    }
}

/// Run this test under sudo
TEST(zkutil, multi_async_libzookeeper_segfault)
{
    auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181", "", 1000);
    zkutil::Requests ops;

    ops.emplace_back(zkutil::makeCheckRequest("/clickhouse_test/zkutil_multi", 0));

    /// Uncomment to test
    //auto cmd = ShellCommand::execute("sudo service zookeeper restart");
    //cmd->wait();

    auto future = zookeeper->asyncMulti(ops);
    auto res = future.get();

    EXPECT_TRUE(zkutil::isHardwareError(res.error));
}


TEST(zkutil, multi_create_sequential)
{
    try
    {
        /// Create chroot node firstly
        auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");
        zookeeper->createAncestors("/clickhouse_test/");

        zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181", "", zkutil::DEFAULT_SESSION_TIMEOUT, "/clickhouse_test");
        zkutil::Requests ops;

        String base_path = "/zkutil/multi_create_sequential";
        zookeeper->tryRemoveRecursive(base_path);
        zookeeper->createAncestors(base_path + "/");

        String sequential_node_prefix = base_path + "/queue-";
        ops.emplace_back(zkutil::makeCreateRequest(sequential_node_prefix, "", zkutil::CreateMode::EphemeralSequential));
        auto results = zookeeper->multi(ops);
        const auto & sequential_node_result_op = typeid_cast<const zkutil::CreateResponse &>(*results.at(0));

        EXPECT_FALSE(sequential_node_result_op.path_created.empty());
        EXPECT_GT(sequential_node_result_op.path_created.length(), sequential_node_prefix.length());
        EXPECT_EQ(sequential_node_result_op.path_created.substr(0, sequential_node_prefix.length()), sequential_node_prefix);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false);
        throw;
    }
}


