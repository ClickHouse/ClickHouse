#pragma once

#include "Types.h"
#include <functional>
#include <unistd.h>
#include <mutex>
#include <random>
#include <memory>
#include <string>
#include <vector>

#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/thread_local_rng.h>
#include <Coordination/KeeperFeatureFlags.h>

#include <Poco/Logger.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>


namespace Coordination
{

class ZooKeeperLoadBalancer
{
public:
    using BetterKeeperHostUpdater = std::function<void (std::unique_ptr<Coordination::IKeeper>)>;

    static ZooKeeperLoadBalancer & instance();

    void init(zkutil::ZooKeeperArgs args_, std::shared_ptr<ZooKeeperLog> zk_log_);
    std::unique_ptr<Coordination::ZooKeeper> createClient();

    // When ZooKeeperLoadBalancer detects that a better Keeper hots is available, it will call this handler.
    void setBetterKeeperHostUpdater(BetterKeeperHostUpdater handler);

private:
    struct HostInfo
    {
        // address is the network address without "secure://" prefix.
        String address;
        UInt8 original_index;
        bool secure;
        Priority priority;
        UInt64 random = 0;

        Coordination::ZooKeeper::Node toZooKeeperNode() const
        {
            Coordination::ZooKeeper::Node node;
            node.address = Poco::Net::SocketAddress(address);
            node.secure = secure;
            node.original_index = original_index;
            return node;
        }

        void randomize()
        {
            random = thread_local_rng();
        }

        static bool compare(const HostInfo & lhs, const HostInfo & rhs)
        {
            return std::forward_as_tuple(lhs.priority, lhs.random)
                < std::forward_as_tuple(rhs.priority, rhs.random);
        }
    };

    void shuffleHosts();
    void recordKeeperHostError(UInt8 original_index);

    std::vector<HostInfo> host_info_list;
    zkutil::ZooKeeperArgs args;
    std::shared_ptr<ZooKeeperLog> zk_log;
    BetterKeeperHostUpdater better_keeper_host_updater;

    std::mutex mutex;

    Poco::Logger* log;
};

}
