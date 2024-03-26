#include <Common/HTTPConnectionPool.h>
#include <Common/HostResolvePool.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/ProxyConfiguration.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/SipHash.h>

#include <Poco/Net/HTTPChunkedStream.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Timespan.h>

#include <queue>

#include "config.h"

#if USE_SSL
#include <Poco/Net/HTTPSClientSession.h>
#endif


namespace ProfileEvents
{
    extern const Event StorageConnectionsCreated;
    extern const Event StorageConnectionsReused;
    extern const Event StorageConnectionsReset;
    extern const Event StorageConnectionsPreserved;
    extern const Event StorageConnectionsExpired;
    extern const Event StorageConnectionsErrors;
    extern const Event StorageConnectionsElapsedMicroseconds;

    extern const Event DiskConnectionsCreated;
    extern const Event DiskConnectionsReused;
    extern const Event DiskConnectionsReset;
    extern const Event DiskConnectionsPreserved;
    extern const Event DiskConnectionsExpired;
    extern const Event DiskConnectionsErrors;
    extern const Event DiskConnectionsElapsedMicroseconds;

    extern const Event HTTPConnectionsCreated;
    extern const Event HTTPConnectionsReused;
    extern const Event HTTPConnectionsReset;
    extern const Event HTTPConnectionsPreserved;
    extern const Event HTTPConnectionsExpired;
    extern const Event HTTPConnectionsErrors;
    extern const Event HTTPConnectionsElapsedMicroseconds;
}


namespace CurrentMetrics
{
    extern const Metric StorageConnectionsStored;
    extern const Metric StorageConnectionsTotal;

    extern const Metric DiskConnectionsStored;
    extern const Metric DiskConnectionsTotal;

    extern const Metric HTTPConnectionsStored;
    extern const Metric HTTPConnectionsTotal;
}

namespace
{
    Poco::Net::HTTPClientSession::ProxyConfig proxyConfigurationToPocoProxyConfig(const DB::ProxyConfiguration & proxy_configuration)
    {
        Poco::Net::HTTPClientSession::ProxyConfig poco_proxy_config;

        poco_proxy_config.host = proxy_configuration.host;
        poco_proxy_config.port = proxy_configuration.port;
        poco_proxy_config.protocol = DB::ProxyConfiguration::protocolToString(proxy_configuration.protocol);
        poco_proxy_config.tunnel = proxy_configuration.tunneling;
        poco_proxy_config.originalRequestProtocol = DB::ProxyConfiguration::protocolToString(proxy_configuration.original_request_protocol);

        return poco_proxy_config;
    }


    size_t roundUp(size_t x, size_t rounding)
    {
        chassert(rounding > 0);
        return (x + (rounding - 1)) / rounding * rounding;
    }


    Poco::Timespan divide(const Poco::Timespan span, int divisor)
    {
        return Poco::Timespan(Poco::Timestamp::TimeDiff(span.totalMicroseconds() / divisor));
    }
}

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNSUPPORTED_URI_SCHEME;
}


IHTTPConnectionPoolForEndpoint::Metrics getMetricsForStorageConnectionPool()
{
    return IHTTPConnectionPoolForEndpoint::Metrics{
        .created = ProfileEvents::StorageConnectionsCreated,
        .reused = ProfileEvents::StorageConnectionsReused,
        .reset = ProfileEvents::StorageConnectionsReset,
        .preserved = ProfileEvents::StorageConnectionsPreserved,
        .expired = ProfileEvents::StorageConnectionsExpired,
        .errors = ProfileEvents::StorageConnectionsErrors,
        .elapsed_microseconds = ProfileEvents::StorageConnectionsElapsedMicroseconds,
        .stored_count = CurrentMetrics::StorageConnectionsStored,
        .active_count = CurrentMetrics::StorageConnectionsTotal,
    };
}


IHTTPConnectionPoolForEndpoint::Metrics getMetricsForDiskConnectionPool()
{
    return IHTTPConnectionPoolForEndpoint::Metrics{
        .created = ProfileEvents::DiskConnectionsCreated,
        .reused = ProfileEvents::DiskConnectionsReused,
        .reset = ProfileEvents::DiskConnectionsReset,
        .preserved = ProfileEvents::DiskConnectionsPreserved,
        .expired = ProfileEvents::DiskConnectionsExpired,
        .errors = ProfileEvents::DiskConnectionsErrors,
        .elapsed_microseconds = ProfileEvents::DiskConnectionsElapsedMicroseconds,
        .stored_count = CurrentMetrics::DiskConnectionsStored,
        .active_count = CurrentMetrics::DiskConnectionsTotal,
    };
}


IHTTPConnectionPoolForEndpoint::Metrics getMetricsForHTTPConnectionPool()
{
    return IHTTPConnectionPoolForEndpoint::Metrics{
        .created = ProfileEvents::HTTPConnectionsCreated,
        .reused = ProfileEvents::HTTPConnectionsReused,
        .reset = ProfileEvents::HTTPConnectionsReset,
        .preserved = ProfileEvents::HTTPConnectionsPreserved,
        .expired = ProfileEvents::HTTPConnectionsExpired,
        .errors = ProfileEvents::HTTPConnectionsErrors,
        .elapsed_microseconds = ProfileEvents::HTTPConnectionsElapsedMicroseconds,
        .stored_count = CurrentMetrics::HTTPConnectionsStored,
        .active_count = CurrentMetrics::HTTPConnectionsTotal,
    };
}


IHTTPConnectionPoolForEndpoint::Metrics getConnectionPoolMetrics(HTTPConnectionGroupType type)
{
    switch (type)
    {
        case HTTPConnectionGroupType::STORAGE:
            return getMetricsForStorageConnectionPool();
        case HTTPConnectionGroupType::DISK:
            return getMetricsForDiskConnectionPool();
        case HTTPConnectionGroupType::HTTP:
            return getMetricsForHTTPConnectionPool();
    }
}


class ConnectionGroup
{
public:
    using Ptr = std::shared_ptr<ConnectionGroup>;

    explicit ConnectionGroup(HTTPConnectionGroupType type_) : type(type_), metrics(getConnectionPoolMetrics(type_)) { }

    void setLimits(HTTPConnectionPools::Limits limits_)
    {
        std::lock_guard lock(mutex);
        limits = std::move(limits_);
        mute_warning_until = 0;
    }

    bool isSoftLimitReached() const
    {
        std::lock_guard lock(mutex);
        return total_connections_in_group >= limits.soft_limit;
    }

    bool isStoreLimitReached() const
    {
        std::lock_guard lock(mutex);
        return total_connections_in_group >= limits.store_limit;
    }

    void atConnectionCreate()
    {
        std::lock_guard lock(mutex);

        ++total_connections_in_group;

        if (total_connections_in_group >= limits.warning_limit && total_connections_in_group >= mute_warning_until)
        {
            LOG_WARNING(log, "Too many active sessions in group {}, count {}, warning limit {}", type, total_connections_in_group, limits.warning_limit);
            mute_warning_until = roundUp(total_connections_in_group, limits.warning_step);
        }
    }

    void atConnectionDestroy()
    {
        std::lock_guard lock(mutex);

        --total_connections_in_group;

        const size_t reduced_warning_limit = limits.warning_limit > 10 ? limits.warning_limit - 10 : 1;
        if (mute_warning_until > 0 && total_connections_in_group < reduced_warning_limit)
        {
            LOG_WARNING(log, "Sessions count is OK in the group {}, count {}", type, total_connections_in_group);
            mute_warning_until = 0;
        }
    }

    HTTPConnectionGroupType getType() const { return type; }

    const IHTTPConnectionPoolForEndpoint::Metrics & getMetrics() const { return metrics; }

private:
    const HTTPConnectionGroupType type;
    const IHTTPConnectionPoolForEndpoint::Metrics metrics;

    LoggerPtr log = getLogger("ConnectionGroup");

    mutable std::mutex mutex;
    HTTPConnectionPools::Limits limits TSA_GUARDED_BY(mutex) = HTTPConnectionPools::Limits();
    size_t total_connections_in_group TSA_GUARDED_BY(mutex) = 0;
    size_t mute_warning_until TSA_GUARDED_BY(mutex) = 0;
};


class IExtendedPool : public IHTTPConnectionPoolForEndpoint
{
public:
    using Ptr = std::shared_ptr<IExtendedPool>;

    virtual HTTPConnectionGroupType getGroupType() const = 0;
    virtual size_t wipeExpired() = 0;
};


// EndpointConnectionPool manage connections to the endpoint
// Features:
// - it uses HostResolver for address selecting. See Common/HostResolver.h for more info.
// - it minimizes number of `Session::connect()`/`Session::reconnect()` calls
//   - stores only connected and ready to use sessions
//   - connection could be reused even when limits are reached
// - soft limit, warn limit, store limit
// - `Session::reconnect()` uses the pool as well
// - comprehensive sensors
// - session is reused according its inner state, automatically


template <class Session>
class EndpointConnectionPool : public std::enable_shared_from_this<EndpointConnectionPool<Session>>, public IExtendedPool
{
private:
    friend class HTTPConnectionPools;

    using WeakPtr = std::weak_ptr<EndpointConnectionPool<Session>>;

    class PooledConnection : public Session
    {
    public:
        using Ptr = std::shared_ptr<PooledConnection>;

        void reconnect() override
        {
            ProfileEvents::increment(metrics.reset);
            Session::close();

            if (auto lock = pool.lock())
            {
                auto timeouts = getTimeouts(*this);
                auto new_connection = lock->getConnection(timeouts);
                Session::assign(*new_connection);
            }
            else
            {
                auto timer = CurrentThread::getProfileEvents().timer(metrics.elapsed_microseconds);
                Session::reconnect();
                ProfileEvents::increment(metrics.created);
            }
        }

        String getTarget() const
        {
            if (!Session::getProxyConfig().host.empty())
                return fmt::format("{} over proxy {}", Session::getHost(), Session::getProxyConfig().host);
            return Session::getHost();
        }

        void flushRequest() override
        {
            if (bool(request_stream))
            {
                request_stream->flush();

                if (auto * fixed_steam = dynamic_cast<Poco::Net::HTTPFixedLengthOutputStream *>(request_stream))
                {
                    request_stream_completed = fixed_steam->isComplete();
                }
                else if (auto * chunked_steam = dynamic_cast<Poco::Net::HTTPChunkedOutputStream *>(request_stream))
                {
                    chunked_steam->rdbuf()->close();
                    request_stream_completed = chunked_steam->isComplete();
                }
                else if (auto * http_stream = dynamic_cast<Poco::Net::HTTPOutputStream *>(request_stream))
                {
                    request_stream_completed = http_stream->isComplete();
                }
                else
                {
                    request_stream_completed = false;
                }
            }
            request_stream = nullptr;

            Session::flushRequest();
        }

        std::ostream & sendRequest(Poco::Net::HTTPRequest & request) override
        {
            std::ostream & result = Session::sendRequest(request);
            result.exceptions(std::ios::badbit);

            request_stream = &result;
            request_stream_completed = false;

            response_stream = nullptr;
            response_stream_completed = false;

            return result;
        }

        std::istream & receiveResponse(Poco::Net::HTTPResponse & response) override
        {
            std::istream & result = Session::receiveResponse(response);
            result.exceptions(std::ios::badbit);

            response_stream = &result;
            response_stream_completed = false;

            return result;
        }

        void reset() override
        {
            request_stream = nullptr;
            request_stream_completed = false;

            response_stream = nullptr;
            response_stream_completed = false;

            Session::reset();
        }

        ~PooledConnection() override
        {
            if (bool(response_stream))
            {
                if (auto * fixed_steam = dynamic_cast<Poco::Net::HTTPFixedLengthInputStream *>(response_stream))
                {
                    response_stream_completed = fixed_steam->isComplete();
                }
                else if (auto * chunked_steam = dynamic_cast<Poco::Net::HTTPChunkedInputStream *>(response_stream))
                {
                    response_stream_completed = chunked_steam->isComplete();
                }
                else if (auto * http_stream = dynamic_cast<Poco::Net::HTTPInputStream *>(response_stream))
                {
                    response_stream_completed = http_stream->isComplete();
                }
                else
                {
                    response_stream_completed = false;
                }
            }
            response_stream = nullptr;

            if (auto lock = pool.lock())
                lock->atConnectionDestroy(*this);
            else
                ProfileEvents::increment(metrics.reset);

            CurrentMetrics::sub(metrics.active_count);
        }

    private:
        friend class EndpointConnectionPool;

        template <class... Args>
        explicit PooledConnection(EndpointConnectionPool::WeakPtr pool_, IHTTPConnectionPoolForEndpoint::Metrics metrics_, Args &&... args)
            : Session(args...), pool(std::move(pool_)), metrics(std::move(metrics_))
        {
            CurrentMetrics::add(metrics.active_count);
        }

        template <class... Args>
        static Ptr create(Args &&... args)
        {
            /// Pool is global, we shouldn't attribute this memory to query/user.
            MemoryTrackerSwitcher switcher{&total_memory_tracker};

            struct make_shared_enabler : public PooledConnection
            {
                explicit make_shared_enabler(Args &&... args) : PooledConnection(std::forward<Args>(args)...) { }
            };
            return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
        }

        void doConnect()
        {
            Session::reconnect();
        }

        bool isCompleted() const
        {
            return request_stream_completed && response_stream_completed;
        }

        WeakPtr pool;
        IHTTPConnectionPoolForEndpoint::Metrics metrics;

        Poco::Logger * log = &Poco::Logger::get("PooledConnection");

        std::ostream * request_stream = nullptr;
        std::istream * response_stream = nullptr;

        bool request_stream_completed = true;
        bool response_stream_completed = true;
    };

    using Connection = PooledConnection;
    using ConnectionPtr = PooledConnection::Ptr;

    struct GreaterByLastRequest
    {
        static bool operator()(const ConnectionPtr & l, const ConnectionPtr & r)
        {
            return l->getLastRequest() + l->getKeepAliveTimeout() > r->getLastRequest() + r->getKeepAliveTimeout();
        }
    };

    using ConnectionsMinHeap = std::priority_queue<ConnectionPtr, std::vector<ConnectionPtr>, GreaterByLastRequest>;

public:
    template <class... Args>
    static Ptr create(Args &&... args)
    {
        struct make_shared_enabler : public EndpointConnectionPool<Session>
        {
            explicit make_shared_enabler(Args &&... args) : EndpointConnectionPool<Session>(std::forward<Args>(args)...) { }
        };
        return std::make_shared<make_shared_enabler>(std::forward<Args>(args)...);
    }

    ~EndpointConnectionPool() override
    {
        CurrentMetrics::sub(group->getMetrics().stored_count, stored_connections.size());
    }

    String getTarget() const
    {
        if (!proxy_configuration.isEmpty())
            return fmt::format("{} over proxy {}", host, proxy_configuration.host);
        return host;
    }

    IHTTPConnectionPoolForEndpoint::ConnectionPtr getConnection(const ConnectionTimeouts & timeouts) override
    {
        Poco::Timestamp now;
        std::vector<ConnectionPtr> expired_connections;

        SCOPE_EXIT({
            MemoryTrackerSwitcher switcher{&total_memory_tracker};
            expired_connections.clear();
        });

        {
            std::lock_guard lock(mutex);

            wipeExpiredImpl(expired_connections, now);

            if (!stored_connections.empty())
            {
                auto it = stored_connections.top();
                stored_connections.pop();

                setTimeouts(*it, timeouts);

                ProfileEvents::increment(getMetrics().reused, 1);
                CurrentMetrics::sub(getMetrics().stored_count, 1);

                return it;
            }
        }

        return prepareNewConnection(timeouts);
    }

    const IHTTPConnectionPoolForEndpoint::Metrics & getMetrics() const override
    {
        return group->getMetrics();
    }

    HTTPConnectionGroupType getGroupType() const override
    {
        return group->getType();
    }

    size_t wipeExpired() override
    {
        Poco::Timestamp now;
        std::vector<ConnectionPtr> expired_connections;

        SCOPE_EXIT({
            MemoryTrackerSwitcher switcher{&total_memory_tracker};
            expired_connections.clear();
        });

        std::lock_guard lock(mutex);
        return wipeExpiredImpl(expired_connections, now);
    }

    size_t wipeExpiredImpl(std::vector<ConnectionPtr> & expired_connections, Poco::Timestamp now) TSA_REQUIRES(mutex)
    {
        while (!stored_connections.empty())
        {
            auto connection = stored_connections.top();

            if (!isExpired(now, connection))
                return stored_connections.size();

            stored_connections.pop();
            expired_connections.push_back(connection);
        }

        CurrentMetrics::sub(getMetrics().stored_count, expired_connections.size());
        ProfileEvents::increment(getMetrics().expired, expired_connections.size());

        return stored_connections.size();
    }

private:
    EndpointConnectionPool(ConnectionGroup::Ptr group_, String host_, UInt16 port_, bool https_, ProxyConfiguration proxy_configuration_)
        : host(std::move(host_))
        , port(port_)
        , https(https_)
        , proxy_configuration(std::move(proxy_configuration_))
        , group(group_)
    {
    }

    WeakPtr getWeakFromThis() { return EndpointConnectionPool::weak_from_this(); }

    bool isExpired(Poco::Timestamp & now, ConnectionPtr connection)
    {
        if (group->isSoftLimitReached())
            return now > (connection->getLastRequest() + divide(connection->getKeepAliveTimeout(), 10));
        return now > connection->getLastRequest() + connection->getKeepAliveTimeout();
    }

    ConnectionPtr allocateNewConnection()
    {
        ConnectionPtr connection = PooledConnection::create(this->getWeakFromThis(), getMetrics(), host, port);
        connection->setKeepAlive(true);

        if (!proxy_configuration.isEmpty())
        {
            connection->setProxyConfig(proxyConfigurationToPocoProxyConfig(proxy_configuration));
        }

        group->atConnectionCreate();

        return connection;
    }

    ConnectionPtr prepareNewConnection(const ConnectionTimeouts & timeouts)
    {
        auto address = HostResolversPool::instance().getResolver(host)->resolve();

        auto session = allocateNewConnection();

        setTimeouts(*session, timeouts);
        session->setResolvedHost(*address);

        try
        {
            auto timer = CurrentThread::getProfileEvents().timer(getMetrics().elapsed_microseconds);
            session->doConnect();
        }
        catch (...)
        {
            address.setFail();
            ProfileEvents::increment(getMetrics().errors);
            session->reset();
            throw;
        }

        ProfileEvents::increment(getMetrics().created);
        return session;
    }

    void atConnectionDestroy(PooledConnection & connection)
    {
        group->atConnectionDestroy();

        if (!connection.connected() || connection.mustReconnect() || !connection.isCompleted() || connection.buffered()
            || group->isStoreLimitReached())
        {
            ProfileEvents::increment(getMetrics().reset, 1);
            return;
        }

        auto connection_to_store = allocateNewConnection();
        connection_to_store->assign(connection);

        CurrentMetrics::add(getMetrics().stored_count, 1);
        ProfileEvents::increment(getMetrics().preserved, 1);

        {
            MemoryTrackerSwitcher switcher{&total_memory_tracker};
            std::lock_guard lock(mutex);
            stored_connections.push(connection_to_store);
        }
    }


    const std::string host;
    const UInt16 port;
    const bool https;
    const ProxyConfiguration proxy_configuration;
    const ConnectionGroup::Ptr group;

    std::mutex mutex;
    ConnectionsMinHeap stored_connections TSA_GUARDED_BY(mutex);
};

struct EndpointPoolKey
{
    HTTPConnectionGroupType connection_group;
    String target_host;
    UInt16 target_port;
    bool is_target_https;
    ProxyConfiguration proxy_config;

    bool operator==(const EndpointPoolKey & rhs) const
    {
        return std::tie(
                   connection_group,
                   target_host,
                   target_port,
                   is_target_https,
                   proxy_config.host,
                   proxy_config.port,
                   proxy_config.protocol,
                   proxy_config.tunneling,
                   proxy_config.original_request_protocol)
            == std::tie(
                   rhs.connection_group,
                   rhs.target_host,
                   rhs.target_port,
                   rhs.is_target_https,
                   rhs.proxy_config.host,
                   rhs.proxy_config.port,
                   rhs.proxy_config.protocol,
                   rhs.proxy_config.tunneling,
                   rhs.proxy_config.original_request_protocol);
    }
};

struct Hasher
{
    size_t operator()(const EndpointPoolKey & k) const
    {
        SipHash s;
        s.update(k.connection_group);
        s.update(k.target_host);
        s.update(k.target_port);
        s.update(k.is_target_https);
        s.update(k.proxy_config.host);
        s.update(k.proxy_config.port);
        s.update(k.proxy_config.protocol);
        s.update(k.proxy_config.tunneling);
        s.update(k.proxy_config.original_request_protocol);
        return s.get64();
    }
};

IExtendedPool::Ptr
createConnectionPool(ConnectionGroup::Ptr group, std::string host, UInt16 port, bool secure, ProxyConfiguration proxy_configuration)
{
    if (secure)
    {
#if USE_SSL
        return EndpointConnectionPool<Poco::Net::HTTPSClientSession>::create(
            group, std::move(host), port, secure, std::move(proxy_configuration));
#else
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED, "Inter-server secret support is disabled, because ClickHouse was built without SSL library");
#endif
    }
    else
    {
        return EndpointConnectionPool<Poco::Net::HTTPClientSession>::create(
            group, std::move(host), port, secure, std::move(proxy_configuration));
    }
}

class HTTPConnectionPools::Impl
{
private:
    const size_t DEFAULT_WIPE_TIMEOUT_SECONDS = 5 * 60;
    const Poco::Timespan wipe_timeout = Poco::Timespan(DEFAULT_WIPE_TIMEOUT_SECONDS, 0);

    ConnectionGroup::Ptr disk_group = std::make_shared<ConnectionGroup>(HTTPConnectionGroupType::DISK);
    ConnectionGroup::Ptr storage_group = std::make_shared<ConnectionGroup>(HTTPConnectionGroupType::STORAGE);
    ConnectionGroup::Ptr http_group = std::make_shared<ConnectionGroup>(HTTPConnectionGroupType::HTTP);


    /// If multiple mutexes are held simultaneously,
    /// they should be locked in this order:
    /// HTTPConnectionPools::mutex, then EndpointConnectionPool::mutex, then ConnectionGroup::mutex.
    std::mutex mutex;

    std::unordered_map<EndpointPoolKey, IExtendedPool::Ptr, Hasher> endpoints_pool TSA_GUARDED_BY(mutex);
    Poco::Timestamp last_wipe_time TSA_GUARDED_BY(mutex);

public:
    IHTTPConnectionPoolForEndpoint::Ptr getPool(HTTPConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
    {
        Poco::Timestamp now;

        std::lock_guard lock(mutex);

        if (now - last_wipe_time > wipe_timeout)
        {
            wipeExpired();
            last_wipe_time = now;
        }

        return getPoolImpl(type, uri, proxy_configuration);
    }

    void setLimits(HTTPConnectionPools::Limits disk, HTTPConnectionPools::Limits storage, HTTPConnectionPools::Limits http)
    {
        disk_group->setLimits(std::move(disk));
        storage_group->setLimits(std::move(storage));
        http_group->setLimits(std::move(http));
    }

    void dropCache()
    {
        std::lock_guard lock(mutex);
        endpoints_pool.clear();
    }

protected:
    ConnectionGroup::Ptr & getGroup(HTTPConnectionGroupType type)
    {
        switch (type)
        {
            case HTTPConnectionGroupType::DISK:
                return disk_group;
            case HTTPConnectionGroupType::STORAGE:
                return storage_group;
            case HTTPConnectionGroupType::HTTP:
                return http_group;
        }
    }

    IExtendedPool::Ptr getPoolImpl(HTTPConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
        TSA_REQUIRES(mutex)
    {
        auto [host, port, secure] = getHostPortSecure(uri, proxy_configuration);
        auto key = EndpointPoolKey{type, host, port, secure, proxy_configuration};

        auto it = endpoints_pool.find(key);
        if (it != endpoints_pool.end())
            return it->second;

        it = endpoints_pool.emplace(key, createConnectionPool(getGroup(type), std::move(host), port, secure, proxy_configuration)).first;

        return it->second;
    }

    void wipeExpired() TSA_REQUIRES(mutex)
    {
        std::vector<EndpointPoolKey> keys_to_drop;

        for (auto & [key, pool] : endpoints_pool)
        {
            auto left_connections = pool->wipeExpired();
            if (left_connections == 0 && pool->getGroupType() != HTTPConnectionGroupType::DISK)
                keys_to_drop.push_back(key);
        }

        for (const auto & key : keys_to_drop)
            endpoints_pool.erase(key);
    }

    static bool useSecureConnection(const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
    {
        if (uri.getScheme() == "http")
            return false;

        if (uri.getScheme() != "https")
            throw Exception(ErrorCodes::UNSUPPORTED_URI_SCHEME, "Unsupported scheme in URI '{}'", uri.toString());

        if (!proxy_configuration.isEmpty())
        {
            if (ProxyConfiguration::Protocol::HTTP == proxy_configuration.protocol && !proxy_configuration.tunneling)
            {
                // If it is an HTTPS request, proxy server is HTTP and user opted for tunneling off, we must not create an HTTPS request.
                // The desired flow is: HTTP request to the proxy server, then proxy server will initiate an HTTPS request to the target server.
                // There is a weak link in the security, but that's what the user opted for.
                return false;
            }
        }

        return true;
    }

    static std::tuple<std::string, UInt16, bool> getHostPortSecure(const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
    {
        return std::make_tuple(uri.getHost(), uri.getPort(), useSecureConnection(uri, proxy_configuration));
    }
};

HTTPConnectionPools::HTTPConnectionPools()
    : impl(std::make_unique<HTTPConnectionPools::Impl>())
{
}

HTTPConnectionPools & HTTPConnectionPools::instance()
{
    static HTTPConnectionPools instance;
    return instance;
}

void HTTPConnectionPools::setLimits(HTTPConnectionPools::Limits disk, HTTPConnectionPools::Limits storage, HTTPConnectionPools::Limits http)
{
    impl->setLimits(std::move(disk), std::move(storage), std::move(http));
}

void HTTPConnectionPools::dropCache()
{
    impl->dropCache();
}

IHTTPConnectionPoolForEndpoint::Ptr
HTTPConnectionPools::getPool(HTTPConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration)
{
    return impl->getPool(type, uri, proxy_configuration);
}
}
