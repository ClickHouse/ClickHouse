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

    auto acl = zookeeper->getDefaultACL();
    zkutil::Ops ops;

    ASSERT_NO_THROW(
        zookeeper->tryRemoveRecursive("/clickhouse_test/zkutil_multi");

        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));
        zookeeper->multi(ops);
    );

    try
    {
        ops.clear();
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/c", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Remove("/clickhouse_test/zkutil_multi/c", -1));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/a", "BadBoy", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/b", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));

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
    auto acl = zookeeper->getDefaultACL();
    zkutil::Ops ops;

    zookeeper->tryRemoveRecursive("/clickhouse_test/zkutil_multi");

    {
        ops.clear();
        auto fut = zookeeper->asyncMulti(ops);
    }

    {
        ops.clear();
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi", "", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/a", "", acl, zkutil::CreateMode::Persistent));

        auto fut = zookeeper->tryAsyncMulti(ops);
        ops.clear();

        auto res = fut.get();
        ASSERT_TRUE(res.code == ZOK);
        ASSERT_EQ(res.results->size(), 2);
        ASSERT_EQ(res.ops_ptr->size(), 2);
    }

    EXPECT_ANY_THROW
    (
        std::vector<zkutil::ZooKeeper::MultiFuture> futures;

        for (size_t i = 0; i < 10000; ++i)
        {
            ops.clear();
            ops.emplace_back(new zkutil::Op::Remove("/clickhouse_test/zkutil_multi", -1));
            ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi", "_", acl, zkutil::CreateMode::Persistent));
            ops.emplace_back(new zkutil::Op::Check("/clickhouse_test/zkutil_multi", -1));
            ops.emplace_back(new zkutil::Op::SetData("/clickhouse_test/zkutil_multi", "xxx", 42));
            ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));

            futures.emplace_back(zookeeper->asyncMulti(ops));
        }

        futures[0].get();
    );

    /// Check there are no segfaults for remaining 999 futures
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);

    {
        ops.clear();
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test/zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));

        auto fut = zookeeper->tryAsyncMulti(ops);
        ops.clear();

        auto res = fut.get();
        ASSERT_TRUE(res.code == ZNODEEXISTS);
        ASSERT_EQ(res.results->size(), 2);
        ASSERT_EQ(res.ops_ptr->size(), 2);
    }
}

/// Run this test under sudo
TEST(zkutil, multi_async_libzookeeper_segfault)
{
    auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181", "", 1000);
    zkutil::Ops ops;

    ops.emplace_back(new zkutil::Op::Check("/clickhouse_test/zkutil_multi", 0));

    /// Uncomment to test
    //auto cmd = ShellCommand::execute("sudo service zookeeper restart");
    //cmd->wait();

    auto future = zookeeper->asyncMulti(ops);
    auto res = future.get();

    EXPECT_TRUE(zkutil::isUnrecoverableErrorCode(res.code));
}


TEST(zkutil, multi_create_sequential)
{
    try
    {
        /// Create chroot node firstly
        auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");
        zookeeper->createAncestors("/clickhouse_test/");

        zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181", "", zkutil::DEFAULT_SESSION_TIMEOUT, "/clickhouse_test");
        auto acl = zookeeper->getDefaultACL();
        zkutil::Ops ops;

        String base_path = "/zkutil/multi_create_sequential";
        zookeeper->tryRemoveRecursive(base_path);
        zookeeper->createAncestors(base_path + "/");

        String sequential_node_prefix = base_path + "/queue-";
        ops.emplace_back(new zkutil::Op::Create(sequential_node_prefix, "", acl, zkutil::CreateMode::EphemeralSequential));
        zkutil::OpResultsPtr results = zookeeper->multi(ops);
        zkutil::OpResult & sequential_node_result_op = results->at(0);

        EXPECT_FALSE(sequential_node_result_op.value.empty());
        EXPECT_GT(sequential_node_result_op.value.length(), sequential_node_prefix.length());
        EXPECT_EQ(sequential_node_result_op.value.substr(0, sequential_node_prefix.length()), sequential_node_prefix);
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false);
        throw;
    }
}


