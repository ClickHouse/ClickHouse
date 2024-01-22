#include <algorithm>
#include <memory>
#include <mutex>

#include <base/types.h>
#include <base/sort.h>
#include <base/getFQDNOrHostName.cpp>
#include "Common/Priority.h"
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeperLoadBalancer.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>

#include <Poco/Logger.h>
#include <Poco/Net/NetException.h>
#include <Poco/String.h>


enum Status
{
    UNDEF = 0,
    ONLINE,
    OFFLINE,
};


class EndpointRegistry
{
public:
    struct EndpointInternal
    {
        String address;
        bool secure = false;
        size_t id;
        Status status = UNDEF;
    };

    size_t addEndpoint(EndpointInternal endpoint)
    {
        size_t id = endpoints.size();
        endpoints.push_back(std::move(endpoint));
        return id;
    }

    const EndpointInternal & findEndpointById(size_t id) const
    {
        return endpoints[id];
    }

    size_t getEndpointsCount() const
    {
        return endpoints.size();
    }

    void markHostOffline(size_t id)
    {
        endpoints[id].status = OFFLINE;
    }

    void markHostOnline(size_t id)
    {
        endpoints[id].status = ONLINE;
    }

    void resetAllOffline()
    {
        for (auto & endpoint : endpoints)
            endpoint.status = UNDEF;
    }

    std::vector<size_t> getRangeByStatus(Status status) const
    {
        std::vector<size_t> ids;
        for (const auto & endpoint : endpoints)
        {
            if (endpoint.status == status)
                ids.push_back(endpoint.id);
        }
        return ids;
    }

    void logAllEndpoints(Poco::Logger* logger) const
    {
        LOG_INFO(logger, "Reporting Endpoint status information.");
        for (const auto & endpoint : endpoints)
            LOG_INFO(logger, "Endpoint ID {}, address {}, status {}", endpoint.id, endpoint.address, endpoint.status);
    }

private:
    std::vector<EndpointInternal> endpoints;
};

std::pair<std::string, bool> parseForSocketAddress(const std::string & raw_host)
{
    std::pair<std::string, bool> result;
    bool secure = startsWith(raw_host, "secure://");
    if (secure)
        result.first = raw_host.substr(strlen("secure://"));
    else
        result.first = raw_host;
    result.second = secure;
    return result;
}

Coordination::IClientsConnectionBalancer::EndpointInfo parseEndpoint(size_t id, const std::string & raw_host)
{
    bool secure = startsWith(raw_host, "secure://");
    std::string address;
    if (secure)
        address = raw_host.substr(strlen("secure://"));
    else
        address = raw_host;
    return Coordination::IClientsConnectionBalancer::EndpointInfo{
        .address = address,
        .host = address.substr(0, address.find_last_of(':')),
        .secure = secure,
        .id = id,
        .settings = Coordination::IClientsConnectionBalancer::ClientSettings{
            .use_fallback_session_lifetime = false,
        },
    };
}

class IBalancerWithEndpointStatuses: public Coordination::IClientsConnectionBalancer
{
public:
    using EndpointInfo = Coordination::IClientsConnectionBalancer::EndpointInfo;

    void addEndpoint(EndpointInfo endpoint) override
    {
        registry.addEndpoint(
            EndpointRegistry::EndpointInternal{
                .address = endpoint.address,
                .secure = endpoint.secure,
                .id = registry.getEndpointsCount(),
                .status = UNDEF,
            });
    }

    void markHostOffline(size_t id) override
    {
        registry.markHostOffline(id);
    }

    void markHostOnline(size_t id) override
    {
        registry.markHostOnline(id);
    }

    void resetAllOffline() override
    {
        registry.resetAllOffline();
    }

    size_t getEndpointsCount() const override
    {
        return registry.getEndpointsCount();
    }

    size_t getAvailableEndpointsCount() const override
    {
        return registry.getRangeByStatus(ONLINE).size() + registry.getRangeByStatus(UNDEF).size();
    }

    void logAllEndpoints(Poco::Logger *logger) const override
    {
        registry.logAllEndpoints(logger);
    }

protected:
    using EndpointInternal = EndpointRegistry::EndpointInternal;

    EndpointInfo asOptimalEndpoint(size_t id) const
    {
        const auto & endpoint = findEndpointById(id);
        return EndpointInfo{
            .address = endpoint.address,
            .secure = endpoint.secure,
            .id = id,
            .settings = ClientSettings{
                .use_fallback_session_lifetime = false,
            },
        };
    }

    EndpointInfo asTemporaryEndpoint(size_t id) const
    {
        const auto & endpoint = findEndpointById(id);
        return EndpointInfo {
            .address = endpoint.address,
            .secure = endpoint.secure,
            .id = id,
            .settings = ClientSettings{
                .use_fallback_session_lifetime = true,
            },
        };
    }

    const EndpointInternal & findEndpointById(size_t id) const
    {
        return registry.findEndpointById(id);
    }

    std::vector<size_t> getRangeByStatus(Status status) const
    {
        return registry.getRangeByStatus(status);
    }

    EndpointRegistry registry;
};

class Random : public IBalancerWithEndpointStatuses
{
public:
    EndpointInfo getHostFrom(const std::vector<size_t> & range)
    {
        chassert(!range.empty());

        std::uniform_int_distribution<int> distribution(0, static_cast<int>(range.size()-1));
        int chosen = distribution(thread_local_rng);

        return asOptimalEndpoint(range[chosen]);
    }

    std::optional<EndpointInfo> getHostToConnect() override
    {
        auto range = getRangeByStatus(ONLINE);
        if (!range.empty())
        {
            return getHostFrom(range);
        }

        range = getRangeByStatus(UNDEF);
        if (!range.empty())
        {
            return getHostFrom(range);
        }

        chassert(getAvailableEndpointsCount() == 0);
        return {};
    }

    bool hasBetterHostToConnect(size_t) const override
    {
        return false;
    }

};

class IBalancerWithPriorities : public IBalancerWithEndpointStatuses
{
public:
    void addEndpoint(EndpointInfo endpoint) override
    {
        IBalancerWithEndpointStatuses::addEndpoint(endpoint);
        priorities.push_back(getPriority(endpoint));
    }

    virtual size_t getPriority(const EndpointInfo & endpoint) const = 0;

    EndpointInfo getHostWithSetting(size_t id) const
    {
        if (isOptimalEndpoint(id))
            return asOptimalEndpoint(id);
        return asTemporaryEndpoint(id);
    }

    std::optional<size_t> getMostPriority(Status status) const
    {
        auto endpoints_index = getRangeByStatus(status);
        size_t min_priority = std::numeric_limits<size_t>::max();
        size_t min_id = getEndpointsCount();
        for (size_t id : endpoints_index)
        {
            size_t p = priorities[id];
            if (min_priority > p)
            {
                min_priority = p;
                min_id = id;
            }
        }
        if (min_id == getEndpointsCount())
            return {};
        return min_id;
    }

    std::optional<EndpointInfo> getHostToConnect() override
    {
        auto id = getMostPriority(ONLINE);
        if (id.has_value())
            return getHostWithSetting(id.value());

        id = getMostPriority(UNDEF);

        if (id.has_value())
            return getHostWithSetting(id.value());

        chassert(getAvailableEndpointsCount() == 0);
        return {};
    }

    bool hasBetterHostToConnect(size_t current_endpoint_id) const override
    {
        auto id = getMostPriority(ONLINE);
        if (id.has_value() && id.value() != current_endpoint_id)
            return true;
        return false;
    }
private:
    bool isOptimalEndpoint(size_t id) const
    {
        return priorities[id] == *std::min(priorities.begin(), priorities.end());
    }

    std::vector<size_t> priorities;
};

class RoundRobin: public IBalancerWithEndpointStatuses
{
public:
    bool hasBetterHostToConnect(size_t) const override
    {
        return false;
    }
private:
    std::optional<EndpointInfo> getHostToConnect() override
    {
        auto round_robin_status = registry.findEndpointById(round_robin_id).status;
        if (round_robin_status == ONLINE)
            return selectEndpoint(round_robin_id);

        auto online_endpoints = getRangeByStatus(ONLINE);
        if (!online_endpoints.empty())
            return selectEndpoint(online_endpoints[0]);

        if (round_robin_status == UNDEF)
            return asOptimalEndpoint(round_robin_id);

        auto undef_endpoints = getRangeByStatus(UNDEF);
        if (!undef_endpoints.empty())
            return selectEndpoint(undef_endpoints[0]);

        chassert(getAvailableEndpointsCount() == 0);
        return {};
    }

    EndpointInfo selectEndpoint(size_t id)
    {
        round_robin_id = (id + 1) % getEndpointsCount();
        return asOptimalEndpoint(id);
    }

    size_t round_robin_id = 0;
};

class FirstOrRandom : public IBalancerWithEndpointStatuses
{
public:
    EndpointInfo getHostFrom(const std::vector<size_t> & range)
    {
        chassert(!range.empty());
        std::uniform_int_distribution<int> distribution(0, static_cast<int>(range.size()-1));
        size_t chosen = distribution(thread_local_rng);
        return asTemporaryEndpoint(range[chosen]);
    }

    std::optional<EndpointInfo> getHostToConnect() override
    {
        auto first_status = registry.findEndpointById(0).status;

        if (first_status == ONLINE)
            return asOptimalEndpoint(0);

        auto range = getRangeByStatus(ONLINE);
        if (!range.empty())
            return getHostFrom(range);

        if (first_status == UNDEF)
            return asOptimalEndpoint(0);

        range = getRangeByStatus(UNDEF);
        if (!range.empty())
            return getHostFrom(range);

        chassert(getAvailableEndpointsCount() == 0);
        return {};
    }

    bool hasBetterHostToConnect(size_t current_endpoint_id) const override
    {
        auto first_status = registry.findEndpointById(0).status;
        return first_status == ONLINE && current_endpoint_id != 0;
    }
};

class NearestHostname : public IBalancerWithPriorities
{
public:
    size_t getPriority(const EndpointInfo & endpoint) const override
    {
        return Coordination::getHostNamePrefixDistance(getFQDNOrHostName(), endpoint.host);
    }
};

class Levenshtein : public IBalancerWithPriorities
{
public:
    size_t getPriority(const EndpointInfo & endpoint) const override
    {
        return Coordination::getHostNameLevenshteinDistance(getFQDNOrHostName(), endpoint.host);
    }
};

class InOrder : public IBalancerWithPriorities
{
public:
    size_t getPriority(const EndpointInfo & endpoint) const override
    {
        return endpoint.id;
    }
};

namespace Coordination
{

ClientsConnectionBalancerPtr getConnectionBalancer(LoadBalancing load_balancing_type)
{
    switch (load_balancing_type)
    {
        case LoadBalancing::RANDOM:
            return std::make_unique<Random>();
        case LoadBalancing::NEAREST_HOSTNAME:
            return std::make_unique<NearestHostname>();
        case LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE:
            return std::make_unique<Levenshtein>();
        case LoadBalancing::IN_ORDER:
            return std::make_unique<InOrder>();
        case LoadBalancing::FIRST_OR_RANDOM:
            return std::make_unique<FirstOrRandom>();
        case LoadBalancing::ROUND_ROBIN:
            return std::make_unique<RoundRobin>();
    }

    return nullptr;
}

bool isKeeperHostDNSAvailable(Poco::Logger* log, const std::string & address, bool & dns_error_occurred)
{
    /// We want to resolve all hosts without DNS cache for keeper connection.
    Coordination::DNSResolver::instance().removeHostFromCache(address);
    try
    {
        const Poco::Net::SocketAddress host_socket_addr{address};
    }
    catch (const Poco::Net::HostNotFoundException & e)
    {
        /// Most likely it's misconfiguration and wrong hostname was specified
        LOG_ERROR(log, "Cannot use ZooKeeper host {}, reason: {}", address, e.displayText());
        return false;
    }
    catch (const Poco::Net::DNSException & e)
    {
        /// Most likely DNS is not available now
        dns_error_occurred = true;
        LOG_ERROR(log, "Cannot use ZooKeeper host {} due to DNS error: {}", address, e.displayText());
        return false;
    }
    return true;
}

ZooKeeperLoadBalancer & ZooKeeperLoadBalancer::instance(const std::string & config_name)
{
    static std::unordered_map<std::string, ZooKeeperLoadBalancer> load_balancer_by_name;
    static std::mutex mutex;

    std::lock_guard<std::mutex> lock{mutex};
    if (load_balancer_by_name.contains(config_name))
        return load_balancer_by_name.at(config_name);
    return load_balancer_by_name.emplace(config_name, ZooKeeperLoadBalancer(config_name)).first->second;
}

ZooKeeperLoadBalancer::ZooKeeperLoadBalancer(const std::string & config_name)
    : log(&Poco::Logger::get("ZooKeeperLoadBalancer/" + config_name))
{
}

void ZooKeeperLoadBalancer::init(zkutil::ZooKeeperArgs args_, std::shared_ptr<ZooKeeperLog> zk_log_)
{
    if (args_.hosts.empty())
        throw zkutil::KeeperException::fromMessage(Coordination::Error::ZBADARGUMENTS, "No hosts specified in ZooKeeperArgs.");

    args = args_;
    zk_log = std::move(zk_log_);

    connection_balancer = getConnectionBalancer(args.get_priority_load_balancing.load_balancing);
    for (size_t i = 0; i < args_.hosts.size(); ++i)
        connection_balancer->addEndpoint(parseEndpoint(i, args_.hosts[i]));
}


[[noreturn]] void throwWhenNoHostAvailable(bool dns_error_occurred)
{
    if (dns_error_occurred)
        throw zkutil::KeeperException::fromMessage(Coordination::Error::ZCONNECTIONLOSS, "Cannot resolve any of provided ZooKeeper hosts due to DNS error");
    else
        throw zkutil::KeeperException::fromMessage(Coordination::Error::ZCONNECTIONLOSS, "Cannot use any of provided ZooKeeper nodes");
}

std::unique_ptr<Coordination::ZooKeeper> ZooKeeperLoadBalancer::createClient()
{
    // We want to retry a better host later if it becomes offline temporarily. But currently we don't have background thread to check endpoint status,
    // so instead we reset all offline status when creating a new client.
    // TODO: add background thread to refresh the host status instead.
    connection_balancer->resetAllOffline();

    bool dns_error_occurred = false;
    size_t attempts = 0;
    while (true)
    {
        ++attempts;
        connection_balancer->logAllEndpoints(log);
        auto endpoint_or = connection_balancer->getHostToConnect();
        if (!endpoint_or.has_value())
        {
            LOG_ERROR(log, "No available endpoints left, all of them are in offline status.");
            throwWhenNoHostAvailable(dns_error_occurred);
        }

        const auto endpoint = endpoint_or.value();
        LOG_INFO(log, "Connecting to ZooKeeper host {},"
                      " number of attempted hosts {}/{}",
                 endpoint.address, attempts, connection_balancer->getEndpointsCount());
        if (!isKeeperHostDNSAvailable(log, endpoint.address, dns_error_occurred))
        {
            LOG_ERROR(log, "Skip the host {} due to DNS error.", endpoint.address);
            connection_balancer->markHostOffline(endpoint.id);
            continue;
        }

        try
        {
            auto zknode = Coordination::ZooKeeper::Node {
                .address = Poco::Net::SocketAddress(endpoint.address),
                .original_index = UInt8(endpoint.id),
                .secure = endpoint.secure,
            };
            auto client  = std::make_unique<Coordination::ZooKeeper>(zknode, args, zk_log);

            if (endpoint.settings.use_fallback_session_lifetime)
            {
                auto session_timeout_seconds = client->setClientSessionDeadline(
                    args.fallback_session_lifetime.min_sec, args.fallback_session_lifetime.max_sec);
                LOG_INFO(log, "Connecting to a sub-optimal ZooKeeper with session timeout {} seconds", session_timeout_seconds);
            }
            connection_balancer->markHostOnline(endpoint.id);

            if (connection_balancer->hasBetterHostToConnect(endpoint.id))
            {
                LOG_INFO(log, "Hosts better than {} exist, would try more.", endpoint.address);
                continue;
            }
            else
            {
                LOG_INFO(log, "No more better host exists for now, will return with host {}.", endpoint.address);
                return client;
            }
        }
        catch (DB::Exception & ex)
        {
            connection_balancer->markHostOffline(endpoint.id);
            LOG_ERROR(log, "Failed to connect to ZooKeeper host {}, error {}", endpoint.address, ex.what());
        }
    }
}
}
