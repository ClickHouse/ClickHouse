#pragma once

#include <IO/ConnectionTimeouts.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProxyConfiguration.h>
#include <Common/logger_useful.h>

#include <base/defines.h>

#include <Poco/Timespan.h>
#include <Poco/Net/HTTPClientSession.h>

#include <mutex>
#include <memory>

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
    virtual ConnectionPtr getConnection(const ConnectionTimeouts & timeouts) = 0;
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

        static constexpr size_t warning_step = 100;
    };

    HTTPConnectionPools(const HTTPConnectionPools &) = delete;
    HTTPConnectionPools & operator=(const HTTPConnectionPools &) = delete;

private:
    HTTPConnectionPools();

public:
    static HTTPConnectionPools & instance();

    void setLimits(Limits disk, Limits storage, Limits http);
    void dropCache();

    IHTTPConnectionPoolForEndpoint::Ptr getPool(HTTPConnectionGroupType type, const Poco::URI & uri, const ProxyConfiguration & proxy_configuration);

private:
    class Impl;
    std::unique_ptr<Impl> impl;
};

}
