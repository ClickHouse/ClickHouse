#include <gtest/gtest.h>
#include <Access/ReplicatedAccessStorage.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
}
}


TEST(ReplicatedAccessStorage, ShutdownWithoutStartup)
{
    auto get_zk = []()
    {
        return std::shared_ptr<zkutil::ZooKeeper>();
    };

    auto storage = ReplicatedAccessStorage("replicated", "/clickhouse/access", get_zk);
    storage.shutdown();
}


TEST(ReplicatedAccessStorage, ShutdownWithFailedStartup)
{
    auto get_zk = []()
    {
        return std::shared_ptr<zkutil::ZooKeeper>();
    };

    auto storage = ReplicatedAccessStorage("replicated", "/clickhouse/access", get_zk);
    try
    {
        storage.startup();
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::NO_ZOOKEEPER)
            throw;
    }
    storage.shutdown();
}

