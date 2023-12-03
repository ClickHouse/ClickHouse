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
// - Shuffle
// - DNS
// 2. SetDeadline fallback logic.
// 3. Disconnect reason and make the host keep track of it.
// 4. (optional) availability zone initialization.
// 5. (optional) background thread check on the hosts.
class ZooKeeperLoadBalancerManager
{
public:
    // LBManager here filter out the unavailable host
    // sorting algo is defined from external logic. Or maybe we can put it here?

    // NOTE: we need to support reconfdigure, see test_keeper_nodes_add test cases.
    // How this is done before? Check
    // Add request not necessarily current session tear down.
    // but how about the new nodes should be considered to load.
    ZooKeeperLoadBalancerManager(zkutil::ZooKeeperArgs args_, std::shared_ptr<ZooKeeperLog> zk_log_);

    void initialize();

    std::unique_ptr<Coordination::ZooKeeper> createClient();

private:
    struct HostInfo
    {
        String host;
        Int8 original_index;
        bool secure;
        Coordination::ZooKeeper::Node toZooKeeperNode() const;
    };

    // The list of the hosts, as specified in the configuration file.
    // String hosts;

    std::vector<HostInfo> host_info_list;

    zkutil::ZooKeeperArgs args;
    // ZooKeeper just pass-in so totally okay to just let here own this.
    std::shared_ptr<ZooKeeperLog> zk_log;

    Poco::Logger* log;
};

}
