#pragma once
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <memory>
#include <Core/Types.h>
#include <Core/Names.h>
#include <boost/noncopyable.hpp>


namespace DB
{

/// A singleton implementing DNS names resolving with optional DNS cache
/// The cache is being updated asynchronous in separate thread (see DNSCacheUpdater)
/// or it could be updated manually via drop() method.
class DNSResolver : private boost::noncopyable
{
public:
    static DNSResolver & instance();

    DNSResolver(const DNSResolver &) = delete;

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolve its IP
    Poco::Net::IPAddress resolveHost(const std::string & host);

    /// Accepts host names like 'example.com:port' or '127.0.0.1:port' or '[::1]:port' and resolve its IP and port
    Poco::Net::SocketAddress resolveAddress(const std::string & host_and_port);

    Poco::Net::SocketAddress resolveAddress(const std::string & host, UInt16 port);

    /// Get this server host name
    String getHostName();

    /// Disables caching
    void setDisableCacheFlag(bool is_disabled = true);

    /// Drops all caches
    void dropCache();

    /// Updates all known hosts in cache.
    /// Returns true if IP of any host has been changed.
    bool updateCache();

    ~DNSResolver();

private:

    DNSResolver();

    struct Impl;
    std::unique_ptr<Impl> impl;

    /// Returns true if IP of host has been changed.
    bool updateHost(const String & host);

    void addToNewHosts(const String & host);
};

}
