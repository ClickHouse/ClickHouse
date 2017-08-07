#include "DNSCache.h"
#include <Poco/Net/DNS.h>
#include <Common/SimpleCache.h>
#include <Core/Types.h>


namespace DB
{

static Poco::Net::IPAddress resolveIPAddressImpl(const std::string & host)
{
    return Poco::Net::DNS::resolveOne(host);
}

static Poco::Net::SocketAddress resolveSocketAddressImpl(const std::string & host_and_port)
{
    return Poco::Net::SocketAddress(host_and_port);
}

struct DNSCache::Impl
{
    /// TODO: Use only one cache for different formats

    SimpleCache<decltype(resolveIPAddressImpl), &resolveIPAddressImpl> cache_host;
    SimpleCache<decltype(resolveSocketAddressImpl), &resolveSocketAddressImpl> cache_host_and_port;
};


DNSCache::DNSCache() : impl(std::make_unique<DNSCache::Impl>()) {}

Poco::Net::IPAddress DNSCache::resolveHost(const std::string & host)
{
    return impl->cache_host(host);
}

Poco::Net::SocketAddress DNSCache::resolveHostAndPort(const std::string & host_and_port)
{
    return impl->cache_host_and_port(host_and_port);
}

void DNSCache::dropCache()
{
    impl->cache_host.dropCache();
    impl->cache_host_and_port.dropCache();
}

DNSCache::~DNSCache() = default;


}
