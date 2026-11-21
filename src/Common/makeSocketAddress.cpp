#include <Common/makeSocketAddress.h>
#include <Common/DNSResolver.h>
#include <Common/NetException.h>
#include <Common/logger_useful.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/NetException.h>

#if defined(OS_WINDOWS)
/// `ws2tcpip.h` covers what <arpa/inet.h>, <netdb.h> and <netinet/in.h> provide here
/// (`inet_pton`, `in6_addr`, the `EAI_*` codes).
#include <Poco/UnWindows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#endif

namespace DB
{

namespace
{

bool isIPAddressLiteral(const std::string & host)
{
    Poco::Net::IPAddress address;
    if (Poco::Net::IPAddress::tryParse(host, address))
        return true;

    /// `Poco::Net::IPAddress::tryParse` cannot tell an address that parses to all zeroes from a
    /// parse failure, so it rejects the IPv6 wildcard `::` - the most common `listen_host` there is.
    /// (It special cases only the IPv4 wildcard spelled exactly `0.0.0.0`.) `inet_pton` has no such
    /// quirk; it does not accept a scope suffix, which is why `tryParse` is still tried first.
    struct in6_addr address_v6 {};
    if (inet_pton(AF_INET6, host.c_str(), &address_v6) == 1)
        return true;

    struct in_addr address_v4 {};
    return inet_pton(AF_INET, host.c_str(), &address_v4) == 1;
}

}

Poco::Net::SocketAddress makeBindAddress(const std::string & host, uint16_t port)
{
    /// A literal is handed to Poco exactly as before, rather than parsed here. For `::` Poco falls
    /// back to `getaddrinfo` (see above), which refuses it on a host with no IPv6 address configured
    /// (`EAI_ADDRFAMILY`, because of `AI_ADDRCONFIG`). `listen_try` relies on that: with
    /// `<listen_host>::</listen_host>` and `<listen_host>0.0.0.0</listen_host>` such a host listens on
    /// the IPv4 wildcard only, and clients keep arriving with IPv4 addresses rather than IPv4-mapped
    /// IPv6 ones (which `host_regexp` cannot reverse resolve through `/etc/hosts`).
    if (isIPAddressLiteral(host))
        return Poco::Net::SocketAddress(host, port);

    return DNSResolver::instance().resolveAddress(host, port);
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, uint16_t port, Poco::Logger * log)
{
    try
    {
        return makeBindAddress(host, port);
    }
    catch (const Poco::Net::DNSException & e)
    {
        const auto code = e.code();
        if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                    || code == EAI_ADDRFAMILY
#endif
        )
        {
            LOG_ERROR(log, "Cannot resolve listen_host ({}), error {}: {}. "
                "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                "specify IPv4 address to listen in <listen_host> element of configuration "
                "file. Example: <listen_host>0.0.0.0</listen_host>",
                host, e.code(), e.message());
        }

        throw;
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
