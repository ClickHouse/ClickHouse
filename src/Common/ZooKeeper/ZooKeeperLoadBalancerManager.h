#pragma once

// TODO: trim them down.

#include "Interpreters/SystemLog.h"
#include "Types.h"
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <optional>
#include <unordered_set>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/thread_local_rng.h>
#include <Coordination/KeeperFeatureFlags.h>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>

#include <unistd.h>
#include <random>

namespace Coordination
{

// Changes to include 
// [x] Connect.
// [X] Shuffle
//    - Testing.
// [X] DNS
// - Disconnect reason and make the host keep track of it.
//   - Callback.
// 4. Availability zone initialization.
// 5. (optional) background thread check on the hosts.
class ZooKeeperLoadBalancerManager
{
public:
    // NOTE: we need to support reconfdigure, see test_keeper_nodes_add test cases.
    // How this is done before? Check
    // Add request not necessarily current session tear down.
    // but how about the new nodes should be considered to load.
    ZooKeeperLoadBalancerManager(zkutil::ZooKeeperArgs args_, std::shared_ptr<ZooKeeperLog> zk_log_);

    std::unique_ptr<Coordination::ZooKeeper> createClient();

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

    // The list of the hosts, as specified in the configuration file.
    // String hosts;

    std::vector<HostInfo> host_info_list;

    zkutil::ZooKeeperArgs args;
    // ZooKeeper just pass-in so totally okay to just let here own this.
    std::shared_ptr<ZooKeeperLog> zk_log;

    Poco::Logger* log;
};

}
