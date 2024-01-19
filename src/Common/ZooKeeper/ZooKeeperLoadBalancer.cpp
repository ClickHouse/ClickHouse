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

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/member.hpp>


namespace DB::ErrorCodes
{
extern const int ALL_CONNECTION_TRIES_FAILED;
}

enum Status
{
    UNDEF = 0,
    ONLINE,
    OFFLINE,
};


class EndpointRegistry
{
public:
    EndpointRegistry()
        : endpoints_by_id(endpoints_indexes.get<TagById>()),
         endpoints_by_status(endpoints_indexes.get<TagByStatus>())
    {}

    struct Endpoint
    {
        String address;
        bool secure = false;
        size_t id;
    };

    size_t addEndpoint(Endpoint endpoint)
    {
        size_t id = endpoints.size();
        endpoints.push_back(std::move(endpoint));
        putToIndexWithStatus(id, UNDEF);
        return id;
    }

    const Endpoint & findEndpointById(size_t id) const
    {
        return endpoints[id];
    }

    size_t getEndpointsCount() const
    {
        return endpoints.size();
    }
    
    void putToIndexWithStatus(size_t id, Status status)
    {
        popFromIndex(id);
        endpoints_indexes.insert(IdxInfo{
            .address = endpoints[id].address,
            .current_status = status,
            .id = id,
        });
    }

    void popFromIndex(size_t id)
    {
        auto it = endpoints_by_id.find(id);
        if (it != endpoints_by_id.end())
            endpoints_indexes.erase(it);
    }

    Status getStatus(size_t id) const
    {
        auto info_it = endpoints_by_id.find(id);
        chassert(info_it != endpoints_by_id.end());

        return info_it->current_status;
    }

    void atHostIsOffline(size_t id)
    {
        putToIndexWithStatus(id, OFFLINE);
    }

    void atHostIsOnline(size_t id)
    {
        putToIndexWithStatus(id, ONLINE);
    }

    void resetOfflineStatuses()
    {
        std::vector<size_t> offline_ids = getRangeByStatus(OFFLINE);
        for (auto id : offline_ids)
            putToIndexWithStatus(id, UNDEF);
    }

    std::vector<size_t> getRangeByStatus(Status status) const
    {
        auto start = endpoints_by_status.lower_bound(boost::make_tuple(status));
        auto end = endpoints_by_status.upper_bound(boost::make_tuple(status));

        std::vector<size_t> ids;
        for (auto it =start; it != end; ++it)
        {
            ids.push_back(it->id);
        }

        return ids;
    }

    size_t getRangeSizeByStatus(Status status) const
    {
        auto start = endpoints_by_status.lower_bound(boost::make_tuple(status));
        auto end = endpoints_by_status.upper_bound(boost::make_tuple(status));
        return std::distance(start, end);
    }

private:
    struct IdxInfo
    {
        std::string_view address = {};
        Status current_status = UNDEF;
        size_t id = 0;
    };

    struct TagById{};
    struct TagByStatus{};

    using EndpointsIndex = boost::multi_index_container<
        IdxInfo,
        boost::multi_index::indexed_by<
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagById>,
                boost::multi_index::member<IdxInfo, size_t, &IdxInfo::id>
                >,
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagByStatus>,
                boost::multi_index::composite_key<
                    IdxInfo,
                    boost::multi_index::member<IdxInfo, Status, &IdxInfo::current_status>,
                    boost::multi_index::member<IdxInfo, size_t, &IdxInfo::id>
                    >
                >
            >
        >;

    std::vector<Endpoint> endpoints;

    EndpointsIndex endpoints_indexes;
    EndpointsIndex::index<TagById>::type & endpoints_by_id;
    EndpointsIndex::index<TagByStatus>::type & endpoints_by_status;
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

class IBalancerWithEndpointStatuses: public Coordination::IClientsConnectionBalancer
{
public:
    using EndpointInfo = Coordination::IClientsConnectionBalancer::EndpointInfo;

    explicit IBalancerWithEndpointStatuses(std::vector<std::string> hosts)
    {
        for (size_t i = 0; i < hosts.size(); i++)
        {
            auto [address, secure] = parseForSocketAddress(hosts[i]);
            registry.addEndpoint(
                EndpointRegistry::Endpoint{
                    .address = address,
                    .secure = secure,
                    .id = i,
                });
        }
    }

    void atHostIsOffline(size_t id) override
    {
        registry.atHostIsOffline(id);
    }

    void atHostIsOnline(size_t id) override
    {
        registry.atHostIsOnline(id);
    }

    void resetOfflineStatuses() override
    {
        registry.resetOfflineStatuses();
    }

    size_t getEndpointsCount() const override
    {
        return registry.getEndpointsCount();
    }

    size_t getAvailableEndpointsCount() const override
    {
        return getRangeSizeByStatus(ONLINE) + getRangeSizeByStatus(UNDEF);
    }

protected:
    using RegisterEndpoint = EndpointRegistry::Endpoint;

    EndpointInfo asOptimalEndpoint(size_t id)
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

    EndpointInfo asTemporaryEndpoint(size_t id)
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

    const RegisterEndpoint & findEndpointById(size_t id) const
    {
        return registry.findEndpointById(id);
    }

    Status getStatus(size_t id) const
    {
        return registry.getStatus(id);
    }

    std::vector<size_t> getRangeByStatus(Status status) const
    {
        return registry.getRangeByStatus(status);
    }

    size_t getRangeSizeByStatus(Status status) const
    {
        return registry.getRangeSizeByStatus(status);
    }

    EndpointRegistry registry;
};

class Random : public IBalancerWithEndpointStatuses
{
public:
    explicit Random(std::vector<std::string> hosts)
        : IBalancerWithEndpointStatuses(std::move(hosts)) {}

    EndpointInfo getHostFrom(const std::vector<size_t> & range)
    {
        chassert(!range.empty());

        std::uniform_int_distribution<int> distribution(0, static_cast<int>(range.size()-1));
        int chosen = distribution(thread_local_rng);

        return asOptimalEndpoint(range[chosen]);
    }

    EndpointInfo getHostToConnect() override
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
        resetOfflineStatuses();
        throw DB::Exception(
            DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
            "No available endpoints left."
            " All offline endpoints are reset in undefined status."
            " Endpoints count is {}",
            getEndpointsCount());
    }
};

// Other cls init in the constructor.
class IBalancerWithPriorities : public IBalancerWithEndpointStatuses
{
public:
    explicit IBalancerWithPriorities(std::vector<std::string> hosts)
        : IBalancerWithEndpointStatuses(std::move(hosts))
    {
    }

    EndpointInfo getHostWithSetting(size_t id)
    {
        if (isOptimalEndpoint(id))
            return asOptimalEndpoint(id);
        return asTemporaryEndpoint(id);
    }

    // TODO: use optional to make more readable.
    size_t getMostPriority(Status status) const
    {
        auto endpoints_index = getRangeByStatus(status);
        size_t min_priority = std::numeric_limits<size_t>::max();
        size_t min_id = getEndpointsCount();
        for (size_t id : endpoints_index)
        {
            LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "getMostPriority id {}, size of priorrities {} ", id, priorities.size());
            size_t p = priorities[id];
            if (min_priority > p)
            {
                min_priority = p;
                min_id = id;
            }
        }
        return min_id;
    }

    EndpointInfo getHostToConnect() override
    {
        LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "Begin of reporting");
        for (const auto id : getRangeByStatus(ONLINE))
        {
            LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "getHostToConnect id ONLINE {}, status {} ", id, getStatus(id));
        }
        for (const auto id : getRangeByStatus(UNDEF))
        {
            LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "getHostToConnect id UNDEF {}, status {} ", id, getStatus(id));
        }
        for (const auto id : getRangeByStatus(OFFLINE))
        {
            LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "getHostToConnect id OFFLINE {}, status {} ", id, getStatus(id));
        }
        LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "End of reporting");

        auto id = getMostPriority(ONLINE);

        if (id != getEndpointsCount())
        {
            LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "getHostToConnect id online {}, status {} ", id, getStatus(id));
            return getHostWithSetting(id);
        }

        id = getMostPriority(UNDEF);

        if (id != getEndpointsCount())
        {
            LOG_INFO(&Poco::Logger::get("ZooKeeperLoadBalancerEndpoint"), "getHostToConnect id undef {}, status {} ", id, getStatus(id));
            return getHostWithSetting(id);
        }

        chassert(getAvailableEndpointsCount() == 0);
        resetOfflineStatuses();
        throw DB::Exception(
            DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
            "No available endpoints left."
            " All offline endpoints are reset in undefined status."
            " Endpoints count is {}",
            getEndpointsCount());
    }

protected:
    virtual bool isOptimalEndpoint(size_t id) = 0;
    // Not idea to check the fields to initialize, but let's do it for now.
    std::vector<size_t> priorities;
};

class RoundRobin: public IBalancerWithEndpointStatuses
{
public:
    explicit RoundRobin(std::vector<std::string> hosts)
        : IBalancerWithEndpointStatuses(std::move(hosts)) {}
private:
    EndpointInfo getHostToConnect() override
    {
        auto round_robin_status = getStatus(round_robin_id);
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

        // TODO: consider not reset here. instead before the createClient. because it's already reset anyway.
        resetOfflineStatuses();
        throw DB::Exception(
            DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
            "No available endpoints left."
            " All offline endpoints are reset in undefined status."
            " Endpoints count is {}",
            getEndpointsCount());
    }

    EndpointInfo selectEndpoint(size_t id)
    {
        round_robin_id = (id + 1 ) % getEndpointsCount();
        return asOptimalEndpoint(id);
    }

    size_t round_robin_id = 0;
};

class FirstOrRandom : public IBalancerWithEndpointStatuses
{
public:
    explicit FirstOrRandom(std::vector<std::string> hosts)
        : IBalancerWithEndpointStatuses(std::move(hosts)) {}

    EndpointInfo getHostFrom(const std::vector<size_t> & range)
    {
        chassert(!range.empty());
        std::uniform_int_distribution<int> distribution(0, static_cast<int>(range.size()-1));
        size_t chosen = distribution(thread_local_rng);
        return asTemporaryEndpoint(range[chosen]);
    }

    EndpointInfo getHostToConnect() override
    {
        auto first_status = getStatus(0);

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
        resetOfflineStatuses();
        throw DB::Exception(
            DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED,
            "No available endpoints left."
            " All offline endpoints are reset in undefined status."
            " Endpoints count is {}",
            getEndpointsCount());

    }
};

class IBalancerWithConstPriorities : public IBalancerWithPriorities
{
public:
    using PriorityCalculator = std::function<size_t(const RegisterEndpoint)>;

    explicit IBalancerWithConstPriorities(std::vector<std::string> hosts)
        : IBalancerWithPriorities(std::move(hosts))
    {}

    IBalancerWithConstPriorities(std::vector<std::string> hosts, PriorityCalculator priority_calculator)
        : IBalancerWithPriorities(std::move(hosts))
    {
        // TODO: test this change.
        // priorities.reserve(getEndpointsCount()
        for (size_t i = 0; i < getEndpointsCount(); ++i)
            priorities.push_back(priority_calculator(registry.findEndpointById(i)));
    }

    static size_t priorityAsNearestHostname(const RegisterEndpoint endpoint)
    {
        return Coordination::getHostNamePrefixDistance(getFQDNOrHostName(), endpoint.address);
    }

    static size_t priorityAsInOrer(const RegisterEndpoint endpoint)
    {
        return endpoint.id;
    }

    static size_t priorityAsLevenshtein(const RegisterEndpoint endpoint)
    {
        return Coordination::getHostNameLevenshteinDistance(getFQDNOrHostName(), endpoint.address);
    }

private:
    bool isOptimalEndpoint(size_t id) override
    {
        return priorities[id] == *std::min(priorities.begin(), priorities.end());
    }
};

namespace Coordination
{

ClientsConnectionBalancerPtr getConnectionBalancer(LoadBalancing load_balancing_type, std::vector<std::string> hosts)
{
    switch (load_balancing_type)
    {
        case LoadBalancing::RANDOM:
            return std::make_unique<Random>(hosts);
        case LoadBalancing::NEAREST_HOSTNAME:
            return std::make_unique<IBalancerWithConstPriorities>(hosts,  IBalancerWithConstPriorities::priorityAsNearestHostname);
        case LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE:
            return std::make_unique<IBalancerWithConstPriorities>(hosts, IBalancerWithConstPriorities::priorityAsLevenshtein);
        case LoadBalancing::IN_ORDER:
            return std::make_unique<IBalancerWithConstPriorities>(hosts, IBalancerWithConstPriorities::priorityAsInOrer);
        case LoadBalancing::FIRST_OR_RANDOM:
            return std::make_unique<FirstOrRandom>(hosts);
        case LoadBalancing::ROUND_ROBIN:
            return std::make_unique<RoundRobin>(hosts);
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

    std::vector<std::string> hosts;
    for (const auto & host : args_.hosts)
        hosts.push_back(host);
    connection_balancer = getConnectionBalancer(args.get_priority_load_balancing.load_balancing, hosts);
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
    bool dns_error_occurred = false;
    size_t attempts = 0;
    while (true)
    {
        ++attempts;
        auto endpoint = connection_balancer->getHostToConnect();

        if (!isKeeperHostDNSAvailable(log, endpoint.address, dns_error_occurred))
            connection_balancer->atHostIsOffline(endpoint.id);

        LOG_INFO(log, "Connecting to ZooKeeper host {},"
                      " number of attempted hosts {}/{}",
                 endpoint.address, attempts, connection_balancer->getEndpointsCount());

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

                LOG_INFO(log, "Connecting to a different az ZooKeeper with session timeout {} seconds", session_timeout_seconds);
            }

            // This is the client we expect to be actually used, therefore we set the callback to monitor the potential send/receive errors.
            client->setSendRecvErrorCallback(
                [logger=log, address=endpoint.address, id=endpoint.id]()
                {
                    LOG_DEBUG(logger, "Load Balancer records a failure on host {}, original index {}", address, id);
                });

            connection_balancer->atHostIsOnline(endpoint.id);
            return client;
        }
        catch (DB::Exception& ex)
        {
            connection_balancer->atHostIsOffline(endpoint.id);
            LOG_ERROR(log, "Failed to connect to ZooKeeper host {}, error {}", endpoint.address, ex.what());
        }
    }
    // now get host returned the error not sure if dns error make sense or not.
    // throwWhenNoHostAvailable(dns_error_occurred);
}

}
