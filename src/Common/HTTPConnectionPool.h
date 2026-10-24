#pragma once

#include <IO/ConnectionTimeouts.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProxyConfiguration.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <Poco/Timespan.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>

#include <cstdint>
#include <mutex>
#include <memory>
#include <vector>

namespace DB
{

class IHTTPConnectionPoolForEndpoint
{
public:
    struct Metrics
    {
        const ProfileEvents::Event created = ProfileEvents::end();
        const ProfileEvents::Event reused = ProfileEvents::end();
        const ProfileEvents::Event reset = ProfileEvents::end();
        const ProfileEvents::Event preserved = ProfileEvents::end();
        const ProfileEvents::Event expired = ProfileEvents::end();
        const ProfileEvents::Event errors = ProfileEvents::end();
        const ProfileEvents::Event elapsed_microseconds = ProfileEvents::end();

        const CurrentMetrics::Metric stored_count = CurrentMetrics::end();
        const CurrentMetrics::Metric active_count = CurrentMetrics::end();
    };

    using Ptr =  std::shared_ptr<IHTTPConnectionPoolForEndpoint>;
    using Connection = Poco::Net::HTTPClientSession;
    using ConnectionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

    /// can throw Poco::Net::Exception, DB::NetException, DB::Exception
    virtual ConnectionPtr getConnection(const ConnectionTimeouts & timeouts, UInt64 * connect_time) = 0;
    virtual const Metrics & getMetrics() const = 0;
    virtual ~IHTTPConnectionPoolForEndpoint() = default;

    IHTTPConnectionPoolForEndpoint(const IHTTPConnectionPoolForEndpoint &) = delete;
    IHTTPConnectionPoolForEndpoint & operator=(const IHTTPConnectionPoolForEndpoint &) = delete;

protected:
    IHTTPConnectionPoolForEndpoint() = default;

};

enum class HTTPConnectionGroupType : uint8_t
{
    DISK,
    STORAGE,
    HTTP,
};

class HTTPConnectionPools
{
public:
    struct Limits
    {
        size_t soft_limit = 100;
        size_t warning_limit = 1000;
        size_t store_limit = 10000;
        size_t hard_limit = 25000;

        static constexpr size_t warning_step = 100;
    };

    struct SocketBufferSizes
    {
        size_t rcvbuf = 0;
        size_t sndbuf = 0;
    };

    /// Socket inodes of live connections, grouped by connection pool type.
    struct PoolSocketInodes
    {
        std::vector<uint64_t> disk;
        std::vector<uint64_t> storage;
        std::vector<uint64_t> http;

        bool empty() const { return disk.empty() && storage.empty() && http.empty(); }
    };

    HTTPConnectionPools(const HTTPConnectionPools &) = delete;
    HTTPConnectionPools & operator=(const HTTPConnectionPools &) = delete;

private:
    HTTPConnectionPools();

public:
    static HTTPConnectionPools & instance();

    void setLimits(Limits disk, Limits storage, Limits http);
    /// Whether the group holds at least `soft_limit` connections, in use and idle together. Above
    /// the soft limit the pool economizes connections: idle ones expire sooner, and readers that
    /// consult this stop keeping a connection open between reads.
    bool isSoftLimitReached(HTTPConnectionGroupType type) const;
    void setSocketBufferSizes(SocketBufferSizes disk, SocketBufferSizes storage, SocketBufferSizes http);
    SocketBufferSizes getSocketBufferSizes(HTTPConnectionGroupType type) const;
    void dropCache();

    IHTTPConnectionPoolForEndpoint::Ptr getPool(HTTPConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration);

    /// Collect socket inodes of all tracked HTTP connections, grouped by pool type.
    /// Inodes are cached snapshots updated on connect, reconnect, and keep-alive store.
    PoolSocketInodes getSocketInodes();

    /// A session that is not created by `HTTPConnectionPools` (the native GCS transport keeps its
    /// own sessions), accounted in one of its connection groups exactly as the group's own
    /// connections are: it counts towards the group's `hard_limit` for as long as this object lives,
    /// its idle periods count towards the group's `store_limit`, and it gets the group's socket
    /// buffer sizes. Destroying the object unregisters the session, so it has to be destroyed exactly
    /// once per registered session, before the session itself.
    class ExternalConnection
    {
    public:
        virtual ~ExternalConnection() = default;

        /// To be called once the session is connected (it may have reconnected since the previous
        /// call): applies the group's `rcvbuf` / `sndbuf` and records the socket inode. Can throw.
        virtual void onConnected(Poco::Net::HTTPClientSession & session) = 0;

        /// Reserve a slot in the group's `store_limit` for keeping the session idle for reuse.
        /// Returns false when the limit is reached; the session must then not be kept.
        virtual bool tryReserveStored() = 0;

        /// Release the slot reserved by a successful `tryReserveStored`. Destroying the object releases
        /// it as well.
        virtual void releaseStored() noexcept = 0;
    };

    /// Registers `session` in the group `type`. Must be called before the session connects. Throws
    /// `HTTP_CONNECTION_LIMIT_REACHED` when the group's `hard_limit` is reached.
    std::unique_ptr<ExternalConnection> registerExternalConnection(HTTPConnectionGroupType type, Poco::Net::HTTPClientSession & session);

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

/// Installs the IO scheduling hooks of the current thread (`CurrentThread::getReadResourceLink` and
/// `CurrentThread::getWriteResourceLink`) on a session for `request`, the same ones the connections of
/// `HTTPConnectionPools` install, or removes the hooks of a previous request when the thread has none.
/// For sessions that are not created by `HTTPConnectionPools` (the native GCS transport).
void setIOSchedulingSessionHooks(Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request);

}
