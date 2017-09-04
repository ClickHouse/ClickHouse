#pragma once
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>
#include <memory>
#include <ext/singleton.h>


namespace DB
{

/// A singleton implementing global and permanent DNS cache
/// It could be updated only manually via drop() method
class DNSCache : public ext::singleton<DNSCache>
{
public:

    DNSCache(const DNSCache &) = delete;

    /// Accepts host names like 'example.com' or '127.0.0.1' or '::1' and resolve its IP
    Poco::Net::IPAddress resolveHost(const std::string & host);

    /// Accepts host names like 'example.com:port' or '127.0.0.1:port' or '[::1]:port' and resolve its IP and port
    Poco::Net::SocketAddress resolveHostAndPort(const std::string & host_and_port);

    /// Drops all caches
    void drop();

    ~DNSCache();

protected:

    DNSCache();

    friend class ext::singleton<DNSCache>;

    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
