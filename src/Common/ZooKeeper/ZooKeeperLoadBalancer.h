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

class IClientsConnectionBalancer
{
public:
    struct ClientSettings
    {
        bool use_fallback_session_lifetime = false;
    };

    struct EndpointInfo
    {
        const String & address;
        bool secure = false;
        size_t id = 0;
        ClientSettings settings = {};
    };

    virtual void addEndpoint(EndpointInfo endpoint) = 0;

    virtual std::optional<EndpointInfo> getHostToConnect() = 0;

    // Returns true if there is a host that is better than one currently connected to.
    virtual bool hasBetterHostToConnect(size_t current_id) const = 0;

    virtual void markHostOffline(size_t id) = 0;
    virtual void markHostOnline(size_t id) = 0;

    virtual void resetAllOffline() = 0;
    virtual size_t getAvailableEndpointsCount() const = 0;
    virtual size_t getEndpointsCount() const = 0;

    virtual void logAllEndpoints(Poco::Logger *log) const = 0;

    virtual ~IClientsConnectionBalancer() = default;
};
using ClientsConnectionBalancerPtr = std::unique_ptr<IClientsConnectionBalancer>;

ClientsConnectionBalancerPtr getConnectionBalancer(LoadBalancing load_balancing_type);

class ZooKeeperLoadBalancer
{
public:
    // We supports different named ZooKeeper for example <zookeeper> and <auxiliary_zookeeper>.
    // Their ZK nodes, timeout, fault injestion configurations are all independent, so use different
    // load balancer instance for different config name.
    static ZooKeeperLoadBalancer & instance(const std::string & config_name);

    explicit ZooKeeperLoadBalancer(const std::string & config_name);

    void init(zkutil::ZooKeeperArgs args_, std::shared_ptr<ZooKeeperLog> zk_log_);
    std::unique_ptr<Coordination::ZooKeeper> createClient();

private:
    void recordKeeperHostError(UInt8 id);

    zkutil::ZooKeeperArgs args;

    ClientsConnectionBalancerPtr connection_balancer;

    std::shared_ptr<ZooKeeperLog> zk_log;
    Poco::Logger* log;
};

}
