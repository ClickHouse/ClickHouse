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
        // address is in format <host>:<port>
        const String address;
        // host is the same as address, but without port
        const String host;
        // secure is true if the connection should be encrypted.
        bool secure = false;
        // id is the index of the endpoint appeared in the list of hosts configuration.
        const size_t id;
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

    virtual void logAllEndpoints(LoggerPtr log) const = 0;

    virtual ~IClientsConnectionBalancer() = default;
};
using ClientsConnectionBalancerPtr = std::unique_ptr<IClientsConnectionBalancer>;

ClientsConnectionBalancerPtr getConnectionBalancer(LoadBalancing load_balancing_type);


template <typename KeeperClient>
class IKeeperFactory
{
public:
    virtual std::unique_ptr<KeeperClient> create(
        const std::string & address, size_t original_index, bool secure,
        const zkutil::ZooKeeperArgs & args, std::shared_ptr<ZooKeeperLog> zk_log_
    ) = 0;
    virtual ~IKeeperFactory() = default;
};


class ZooKeeperImplFactory : public IKeeperFactory<Coordination::IKeeper>
{
public:
    std::unique_ptr<Coordination::IKeeper> create(
        const std::string & address, size_t original_index, bool secure,
        const zkutil::ZooKeeperArgs & args, std::shared_ptr<ZooKeeperLog> zk_log_
    ) override;
};

// Using template type allows us to use a simple KeeperClient class for unit test. Even `TestKeeper` is too heavy and not necessary.
template <typename KeeperClient>
class ZooKeeperLoadBalancer
{
public:
    // We supports different named ZooKeeper for example <zookeeper> and <auxiliary_zookeeper>.
    // Their ZK nodes, timeout, fault injestion configurations are all independent, so use different
    // load balancer instance for different config name.
    static ZooKeeperLoadBalancer & instance(const std::string & config_name,
        std::shared_ptr<IKeeperFactory<KeeperClient>> factory = std::make_shared<ZooKeeperImplFactory>());

    explicit ZooKeeperLoadBalancer(const std::string & config_name,
        std::shared_ptr<IKeeperFactory<KeeperClient>> factory = std::make_shared<ZooKeeperImplFactory>());

    void init(zkutil::ZooKeeperArgs args_, std::shared_ptr<ZooKeeperLog> zk_log_);

    void setZooKeeperLog(std::shared_ptr<ZooKeeperLog> zk_log_)
    {
        zk_log = zk_log_;
    }

    std::unique_ptr<KeeperClient> createClient();

    // Used only for test.
    void disableDNSCheckForTest() {  check_dns_error = false; }

private:
    void recordKeeperHostError(UInt8 id);

    zkutil::ZooKeeperArgs args;

    ClientsConnectionBalancerPtr connection_balancer;

    std::shared_ptr<ZooKeeperLog> zk_log;
    LoggerPtr log;

    std::shared_ptr<IKeeperFactory<KeeperClient>> keeper_factory;

    bool check_dns_error = true;
};

struct FakeKeeperClient
{
public:
    UInt32 setClientSessionDeadline(UInt32, UInt32)
    {
        set_deadline = true;
        return 0;
    }

    bool isClientSessionDeadlineSet() const
    {
        return set_deadline;
    }

    bool set_deadline = false;
};

typedef ZooKeeperLoadBalancer<Coordination::IKeeper> DefaultZooKeeperLoadBalancer;

}
