#pragma once
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <memory>
#include <ext/singleton.h>
#include <Core/Types.h>


namespace DB
{

/// A singleton implementing DNS names resolving with optional permanent DNS cache
/// The cache could be updated only manually via drop() method
class DNSResolver : public ext::singleton<DNSResolver>
{
public:

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

    ~DNSResolver();

protected:

    DNSResolver();

    friend class ext::singleton<DNSResolver>;

    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
