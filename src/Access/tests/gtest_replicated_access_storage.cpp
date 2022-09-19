#include <gtest/gtest.h>
#include <Access/ReplicatedAccessStorage.h>
#include <Access/AccessChangesNotifier.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
}
}


TEST(ReplicatedAccessStorage, ShutdownWithFailedStartup)
{
    auto get_zk = []()
    {
        return std::shared_ptr<zkutil::ZooKeeper>();
    };

    AccessChangesNotifier changes_notifier;

    try
    {
        auto storage = ReplicatedAccessStorage("replicated", "/clickhouse/access", get_zk, changes_notifier);
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::NO_ZOOKEEPER)
            throw;
    }
}

