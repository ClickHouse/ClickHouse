#include <Common/makeSocketAddress.h>
#include <Common/DNSResolver.h>
#include <Common/NetException.h>
#include <Common/logger_useful.h>
#include <Poco/Net/IPAddress.h>

#include <optional>

#include <arpa/inet.h>
#include <netinet/in.h>

namespace DB
{

namespace
{

std::optional<Poco::Net::IPAddress> tryParseIPAddress(const std::string & host)
{
    Poco::Net::IPAddress address;
    if (Poco::Net::IPAddress::tryParse(host, address))
        return address;

    /// `Poco::Net::IPAddress::tryParse` cannot tell an address that parses to all zeroes from a
    /// parse failure, so it rejects the IPv6 wildcard `::` - the most common `listen_host` there is.
    /// (It special cases only the IPv4 wildcard spelled exactly `0.0.0.0`.) `inet_pton` has no such
    /// quirk; it does not accept a scope suffix, which is why `tryParse` is still tried first.
    struct in6_addr address_v6 {};
    if (inet_pton(AF_INET6, host.c_str(), &address_v6) == 1)
        return Poco::Net::IPAddress(&address_v6, sizeof(address_v6));

    struct in_addr address_v4 {};
    if (inet_pton(AF_INET, host.c_str(), &address_v4) == 1)
        return Poco::Net::IPAddress(&address_v4, sizeof(address_v4));

    return {};
}

}

Poco::Net::SocketAddress makeBindAddress(const std::string & host, uint16_t port)
{
    if (auto address = tryParseIPAddress(host))
        return Poco::Net::SocketAddress(*address, port);

    return DNSResolver::instance().resolveAddress(host, port);
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, Poco::Logger * log)
{
    try
    {
        return makeBindAddress(host, port);
    }
    catch (const NetException & e)
    {
        LOG_ERROR(log, "Cannot resolve listen_host ({}): {}. "
            "If it is an IPv6 address and your host has disabled IPv6, then consider to "
            "specify IPv4 address to listen in <listen_host> element of configuration "
            "file. Example: <listen_host>0.0.0.0</listen_host>",
            host, e.message());

        throw;
    }
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, LoggerPtr log)
{
    return makeSocketAddress(host, port, log.get());
}

}
