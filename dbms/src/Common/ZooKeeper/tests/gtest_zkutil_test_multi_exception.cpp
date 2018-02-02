#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/StringUtils/StringUtils.h>
#include <iostream>
#include <chrono>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
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
        zookeeper->tryRemoveRecursive("/clickhouse_test_zkutil_multi");

        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));
        zookeeper->multi(ops);
    );

    try
    {
        ops.clear();
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/c", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Remove("/clickhouse_test_zkutil_multi/c", -1));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "BadBoy", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/b", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));

        zookeeper->multi(ops);
        FAIL();
    }
    catch (...)
    {
        zookeeper->tryRemoveRecursive("/clickhouse_test_zkutil_multi");

        String msg = getCurrentExceptionMessage(false);

        bool msg_has_reqired_patterns = msg.find("/clickhouse_test_zkutil_multi/a") != std::string::npos && msg.find("#2") != std::string::npos;
        EXPECT_TRUE(msg_has_reqired_patterns) << msg;
    }
}


TEST(zkutil, multi_async)
{
    auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");
    auto acl = zookeeper->getDefaultACL();
    zkutil::Ops ops;

    zookeeper->tryRemoveRecursive("/clickhouse_test_zkutil_multi");

    {
        ops.clear();
        auto fut = zookeeper->asyncMulti(ops);
    }

    {
        ops.clear();
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi", "", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "", acl, zkutil::CreateMode::Persistent));

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
            ops.emplace_back(new zkutil::Op::Remove("/clickhouse_test_zkutil_multi", -1));
            ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi", "_", acl, zkutil::CreateMode::Persistent));
            ops.emplace_back(new zkutil::Op::Check("/clickhouse_test_zkutil_multi", -1));
            ops.emplace_back(new zkutil::Op::SetData("/clickhouse_test_zkutil_multi", "xxx", 42));
            ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));

            futures.emplace_back(zookeeper->asyncMulti(ops));
        }

        futures[0].get();
    );

    /// Check there are no segfaults for remaining 999 futures
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1s);

    {
        ops.clear();
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));

        auto fut = zookeeper->tryAsyncMulti(ops);
        ops.clear();

        auto res = fut.get();
        ASSERT_TRUE(res.code == ZNODEEXISTS);
        ASSERT_EQ(res.results->size(), 2);
        ASSERT_EQ(res.ops_ptr->size(), 2);
    }
}


TEST(zkutil, multi_create_sequential)
{
    try
    {
        auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");
        auto acl = zookeeper->getDefaultACL();
        zkutil::Ops ops;

        String base_path = "/clickhouse_test/zkutil/multi_create_sequential";
        zookeeper->tryRemoveRecursive(base_path);
        zookeeper->createAncestors(base_path + "/");

        String entry_path = base_path + "/queue-";
        ops.emplace_back(new zkutil::Op::Create(entry_path, "", acl, zkutil::CreateMode::EphemeralSequential));
        zkutil::OpResultsPtr results = zookeeper->multi(ops);
        zkutil::OpResult & result = results->at(0);

        EXPECT_TRUE(result.value != nullptr);
        EXPECT_TRUE(startsWith(result.value, entry_path));
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(false);
        throw;
    }

}
