#pragma once
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <memory>
#include <base/types.h>
#include <Core/Names.h>
#include <boost/noncopyable.hpp>
#include <Common/logger_useful.h>


namespace DB
{

/// A singleton implementing DNS names resolving with optional DNS cache
/// The cache is being updated asynchronous in separate thread (see DNSCacheUpdater)
/// or it could be updated manually via drop() method.
class DNSResolver : private boost::noncopyable
{
public:
    using IPAddresses = std::vector<Poco::Net::IPAddress>;

    static DNSResolver & instance();

    DNSResolver(const DNSResolver &) = delete;

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolves its IP
    Poco::Net::IPAddress resolveHost(const std::string & host);

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolves all its IPs
    IPAddresses resolveHostAll(const std::string & host);

    /// Accepts host names like 'example.com:port' or '127.0.0.1:port' or '[::1]:port' and resolves its IP and port
    Poco::Net::SocketAddress resolveAddress(const std::string & host_and_port);

    Poco::Net::SocketAddress resolveAddress(const std::string & host, UInt16 port);

    std::vector<Poco::Net::SocketAddress> resolveAddressList(const std::string & host, UInt16 port);

    /// Accepts host IP and resolves its host names
    Strings reverseResolve(const Poco::Net::IPAddress & address);

    /// Get this server host name
    String getHostName();

    /// Prefer IPv6 addresses for DNS resolution
    void setPreferIPv6(bool prefer_ipv6 = true);

    /// Disables caching
    void setDisableCacheFlag(bool is_disabled = true);

    /// Drops all caches
    void dropCache();

    /// Updates all known hosts in cache.
    /// Returns true if IP of any host has been changed or an element was dropped (too many failures)
    bool updateCache(UInt32 max_consecutive_failures);

    ~DNSResolver();

private:
    template <typename UpdateF, typename ElemsT>

    bool updateCacheImpl(
        UpdateF && update_func,
        ElemsT && elems,
        UInt32 max_consecutive_failures,
        const String & notfound_log_msg,
        const String & dropped_log_msg);

    DNSResolver();

    struct Impl;
    std::unique_ptr<Impl> impl;
    Poco::Logger * log;

    /// Updates cached value and returns true it has been changed.
    bool updateHost(const String & host);
    bool updateAddress(const Poco::Net::IPAddress & address);

    void addToNewHosts(const String & host);
    void addToNewAddresses(const Poco::Net::IPAddress & address);
};

}
