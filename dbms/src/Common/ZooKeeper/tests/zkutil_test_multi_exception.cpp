#include <iostream>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/Exception.h>

using namespace DB;

int main()
{
    auto zookeeper = std::make_unique<zkutil::ZooKeeper>("localhost:2181");

    try
    {
        auto acl = zookeeper->getDefaultACL();
        zkutil::Ops ops;

        zookeeper->tryRemoveRecursive("/clickhouse_test_zkutil_multi");

        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));
        zookeeper->multi(ops);

        ops.clear();
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/c", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Remove("/clickhouse_test_zkutil_multi/c", -1));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "BadBoy", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/b", "_", acl, zkutil::CreateMode::Persistent));
        ops.emplace_back(new zkutil::Op::Create("/clickhouse_test_zkutil_multi/a", "_", acl, zkutil::CreateMode::Persistent));

        zookeeper->multi(ops);
    }
    catch (...)
    {
        zookeeper->tryRemoveRecursive("/clickhouse_test_zkutil_multi");

        String msg = getCurrentExceptionMessage(false);

        if (msg.find("/clickhouse_test_zkutil_multi/a") == std::string::npos || msg.find("#2") == std::string::npos)
        {
            std::cerr << "Wrong: " << msg;
            return -1;
        }

        std::cout << "Ok: " << msg;
        return 0;
    }

    std::cerr << "Unexpected";
    return -1;
}
