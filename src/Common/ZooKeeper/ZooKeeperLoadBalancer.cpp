#include <memory>
#include <mutex>

#include <base/types.h>
#include <base/sort.h>
#include <base/getFQDNOrHostName.cpp>
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
        : endpoints_by_id(endpoints_indexes.get<TagById>())
        , endpoints_by_address(endpoints_indexes.get<TagByAddress>())
        , endpoints_by_status(endpoints_indexes.get<TagByStatus>())
    {}

    struct Endpoint
    {
        String address;
        bool secure = false;
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

    size_t getIdByAddress(std::string_view address)
    {
        auto infoIt = endpoints_by_address.find(address);
        chassert(infoIt != endpoints_by_address.end());
        return infoIt->id;
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
        auto infoIt = endpoints_by_id.find(id);
        chassert(infoIt != endpoints_by_id.end());

        return infoIt->current_status;
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
            ids.push_back(it->id);

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
    struct TagByAddress{};
    struct TagByStatus{};

    using EndpointsIndex = boost::multi_index_container<
        IdxInfo,
        boost::multi_index::indexed_by<
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagById>,
                boost::multi_index::member<IdxInfo, size_t, &IdxInfo::id>
                >,
            boost::multi_index::hashed_unique<
                boost::multi_index::tag<TagByAddress>,
                boost::multi_index::member<IdxInfo, std::string_view, &IdxInfo::address>
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
    EndpointsIndex::index<TagByAddress>::type & endpoints_by_address;
    EndpointsIndex::index<TagByStatus>::type & endpoints_by_status;
};

class IBalancerWithEndpointStatuses: public Coordination::IClientsConnectionBalancer
{
public:
    using EndpointInfo = Coordination::IClientsConnectionBalancer::EndpointInfo;

    void atHostIsOffline(size_t id) override
    {
        registry.atHostIsOnline(id);
    }

    void atHostIsOnline(size_t id) override
    {
        registry.atHostIsOnline(id);
    }

    size_t addEndpoint(const String & address, bool secure) override
    {
        return registry.addEndpoint(
            EndpointRegistry::Endpoint{
            .address = address,
            .secure = secure,
        });
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
        auto & endpoint = findEndpointById(id);
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
        auto & endpoint = findEndpointById(id);
        return EndpointInfo {
            .address = endpoint.address,
            .secure = endpoint.secure,
            .id = id,
            .settings = ClientSettings{
                .use_fallback_session_lifetime = true,
            },
        };
    }

    size_t findIdByAddress(std::string_view address)
    {
        return registry.getIdByAddress(address);
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

private:
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

class IBalancerWithPriorities : public IBalancerWithEndpointStatuses
{
public:
    IBalancerWithPriorities()
        : priority_by_id(priority_indexes.get<TagById>())
        , priority_by_status(priority_indexes.get<TagByStatus>())
    {}

    void atHostIsOffline(size_t id) override
    {
        IBalancerWithEndpointStatuses::atHostIsOffline(id);
        reindex(id);
    }

    void atHostIsOnline(size_t id) override
    {
        IBalancerWithEndpointStatuses::atHostIsOnline(id);
        reindex(id);
    }

    void resetOfflineStatuses() override
    {
        IBalancerWithEndpointStatuses::resetOfflineStatuses();
        resetIndexes();
    }

    EndpointInfo getHostWithSetting(size_t id)
    {
        if (isOptimalEndpoint(id))
            return asOptimalEndpoint(id);
        return asTemporaryEndpoint(id);
    }

    size_t getMostPriority(Status status) const
    {
        auto it = priority_by_status.lower_bound(boost::make_tuple(status));

        if (it == priority_by_status.end())
            return getEndpointsCount();

        if (it->status != status)
            return getEndpointsCount();

        return it->id;
    }

    EndpointInfo getHostToConnect() override
    {
        auto id = getMostPriority(ONLINE);

        if (id != getEndpointsCount())
            return getHostWithSetting(id);

        id = getMostPriority(UNDEF);

        if (id != getEndpointsCount())
            return getHostWithSetting(id);

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
    struct TagById{};
    struct TagByStatus{};

    struct IdxPriority
    {
        size_t id = 0;
        Status status = UNDEF;
        size_t priority = 0;
    };

    using PriorityIndex = boost::multi_index_container<
        IdxPriority,
        boost::multi_index::indexed_by<
            /// Index by address, it is a map
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagById>,
                boost::multi_index::member<IdxPriority, size_t, &IdxPriority::id>
                >,
            boost::multi_index::ordered_unique<
                boost::multi_index::tag<TagByStatus>,
                boost::multi_index::composite_key<
                    IdxPriority,
                    boost::multi_index::member<IdxPriority, Status, &IdxPriority::status>,
                    boost::multi_index::member<IdxPriority, size_t, &IdxPriority::priority>,
                    boost::multi_index::member<IdxPriority, size_t, &IdxPriority::id>
                    >
                >
            >
        >;

    PriorityIndex priority_indexes;
    PriorityIndex::index<TagById>::type & priority_by_id;
    PriorityIndex::index<TagByStatus>::type & priority_by_status;

    void reindex(size_t id)
    {
        auto it = priority_by_id.find(id);
        chassert(it != priority_by_id.end());
        priority_indexes.erase(it);
        priority_indexes.insert(
            IdxPriority{
                .id = id,
                .status = getStatus(id),
                .priority = getPriority(id),
            }
        );
    }

    void resetIndexes()
    {
        priority_indexes.clear();

        for (size_t id = 0; id < getEndpointsCount(); ++id)
        {
            priority_indexes.insert(
                IdxPriority{
                    .id = id,
                    .status = getStatus(id),
                    .priority = getPriority(id),
                }
            );
        }
    }

    virtual bool isOptimalEndpoint(size_t id) = 0;
    virtual size_t getPriority(size_t id) = 0;
};

class RoundRobin: public IBalancerWithPriorities
{
private:
    EndpointInfo getHostToConnect() override
    {
        auto round_robin_status = getStatus(round_robin_id);

        if (round_robin_status == ONLINE)
            return asOptimalEndpoint(round_robin_id++);

        auto id = getMostPriority(ONLINE);

        if (id != getEndpointsCount())
        {
            round_robin_id = id;
            return getHostWithSetting(round_robin_id++);
        }

        if (round_robin_status == UNDEF)
            return asOptimalEndpoint(round_robin_id++);

        id = getMostPriority(UNDEF);

        if (id != getEndpointsCount())
        {
            round_robin_id = id;
            return getHostWithSetting(round_robin_id++);
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

    size_t getPriority(size_t) override
    {
        return 0;
    }

    bool isOptimalEndpoint(size_t) override
    {
        return true;
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
private:
    void initPriorities()
    {
        if (priorities.size() == getEndpointsCount())
            return;

        priorities = getPriorities();
        optimal_priority = *std::min_element(priorities.begin(), priorities.end());

        resetIndexes();
    }

    size_t getPriority(size_t id) override
    {
        initPriorities();
        return priorities[id];
    }

    bool isOptimalEndpoint(size_t id) override
    {
        return optimal_priority == priorities[id];
    }

    virtual std::vector<size_t> getPriorities() = 0;

    std::vector<size_t> priorities;
    size_t optimal_priority = 0;
};

class NearestHostname: public IBalancerWithConstPriorities
{
public:
    explicit NearestHostname(const String client_hostname_)
        : client_hostname(client_hostname_)
    { }

private:
    std::vector<size_t> getPriorities() override
    {
        std::vector<size_t> priorities;
        priorities.resize(getEndpointsCount());
        for (size_t i = 0; i < getEndpointsCount(); ++i)
        {
            priorities.push_back(
                Coordination::getHostNamePrefixDistance(client_hostname, findEndpointById(i).address));
        }
        return priorities;
    }

    const String client_hostname;
};

class Levenshtein: public IBalancerWithConstPriorities
{
public:
    explicit Levenshtein(const String client_hostname_)
        : client_hostname(client_hostname_)
    { }

private:
    std::vector<size_t> getPriorities() override
    {
        std::vector<size_t> priorities;
        priorities.resize(getEndpointsCount());
        for (size_t i = 0; i < getEndpointsCount(); ++i)
        {
            priorities.push_back(
                Coordination::getHostNameLevenshteinDistance(client_hostname, findEndpointById(i).address));
        }
        return priorities;
    }

    const String client_hostname;
};

class InOrder: public IBalancerWithConstPriorities
{
private:
    std::vector<size_t> getPriorities() override
    {
        std::vector<size_t> priorities;
        priorities.reserve(getEndpointsCount());
        for (size_t i = 0; i < getEndpointsCount(); ++i)
        {
            priorities.push_back(i);
        }
        return priorities;
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
            return std::make_unique<NearestHostname>(getFQDNOrHostName());
        case LoadBalancing::HOSTNAME_LEVENSHTEIN_DISTANCE:
            return std::make_unique<Levenshtein>(getFQDNOrHostName());
        case LoadBalancing::IN_ORDER:
            return std::make_unique<InOrder>();
        case LoadBalancing::FIRST_OR_RANDOM:
            return std::make_unique<FirstOrRandom>();
        case LoadBalancing::ROUND_ROBIN:
            return std::make_unique<RoundRobin>();
    }

    return nullptr;
}

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

    connection_balancer = getConnectionBalancer(args.get_priority_load_balancing.load_balancing);

    for (size_t i = 0; i < args_.hosts.size(); ++i)
    {
        auto [address, secure] =  parseForSocketAddress(args_.hosts[i]);
        [[maybe_unused]] auto id = connection_balancer->addEndpoint(address, secure);
        chassert(id == i);
    }
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

    /// Since we do not have background checker aliveness of endpoint
    /// we are giving all offline endpoint second chance
    /// offline->undef, thy will be offered after all online
    connection_balancer->resetOfflineStatuses();

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
                [log_=log, address=endpoint.address, id=endpoint.id]()
                {
                    LOG_DEBUG(log_, "Load Balancer records a failure on host {}, original index {}", address, id);
                });

            return client;
        }
        catch (DB::Exception& ex)
        {
            LOG_ERROR(log, "Failed to connect to ZooKeeper host {}, error {}", endpoint.address, ex.what());

            if (ex.code() == DB::ErrorCodes::ALL_CONNECTION_TRIES_FAILED)
            {
                throw;
            }
        }
        throwWhenNoHostAvailable(dns_error_occurred);
    }
}

}
