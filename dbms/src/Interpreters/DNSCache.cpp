#include "DNSCache.h"
#include <Common/SimpleCache.h>
#include <Common/Exception.h>
#include <Core/Types.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/NetException.h>
#include <Poco/NumberParser.h>
#include <arpa/inet.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


/// Slightly altered implementation from https://github.com/pocoproject/poco/blob/poco-1.6.1/Net/src/SocketAddress.cpp#L86
static void splitHostAndPort(const std::string & host_and_port, std::string & out_host, UInt16 & out_port)
{
    String port_str;
    out_host.clear();

    auto it = host_and_port.begin();
    auto end = host_and_port.end();

    if (*it == '[') /// Try parse case '[<IPv6 or something else>]:<port>'
    {
        ++it;
        while (it != end && *it != ']')
            out_host += *it++;
        if (it == end)
            throw Exception("Malformed IPv6 address", ErrorCodes::BAD_ARGUMENTS);
        ++it;
    }
    else /// Case '<IPv4 or domain name or something else>:<port>'
    {
        while (it != end && *it != ':')
            out_host += *it++;
    }

    if (it != end && *it == ':')
    {
        ++it;
        while (it != end)
            port_str += *it++;
    }
    else
        throw Exception("Missing port number", ErrorCodes::BAD_ARGUMENTS);

    unsigned port;
    if (Poco::NumberParser::tryParseUnsigned(port_str, port) && port <= 0xFFFF)
    {
        out_port = static_cast<UInt16>(port);
    }
    else
    {
        struct servent * se = getservbyname(port_str.c_str(), nullptr);
        if (se)
            out_port = ntohs(static_cast<UInt16>(se->s_port));
        else
            throw Exception("Service not found", ErrorCodes::BAD_ARGUMENTS);
    }
}


static Poco::Net::IPAddress resolveIPAddressImpl(const std::string & host)
{
    /// NOTE: Poco::Net::DNS::resolveOne(host) doesn't work for IP addresses like 127.0.0.2
    /// Therefore we use SocketAddress constructor with dummy port to resolve IP
    return Poco::Net::SocketAddress(host, 0U).host();
}


struct DNSCache::Impl
{
    SimpleCache<decltype(resolveIPAddressImpl), &resolveIPAddressImpl> cache_host;
};


DNSCache::DNSCache() : impl(std::make_unique<DNSCache::Impl>()) {}

Poco::Net::IPAddress DNSCache::resolveHost(const std::string & host)
{
    return impl->cache_host(host);
}

Poco::Net::SocketAddress DNSCache::resolveHostAndPort(const std::string & host_and_port)
{
    String host;
    UInt16 port;
    splitHostAndPort(host_and_port, host, port);

    return Poco::Net::SocketAddress(impl->cache_host(host), port);
}

void DNSCache::drop()
{
    impl->cache_host.drop();
}

DNSCache::~DNSCache() = default;


}
