#include <Common/isLocalAddress.h>

#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>
#include <winsock2.h>
#include <ws2ipdef.h>
#include <iphlpapi.h>
#include <vector>
#else
#include <ifaddrs.h>
#endif
#include <algorithm>
#include <cstring>
#include <optional>
#include <ranges>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/levenshteinDistance.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Net/SocketAddress.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
}

namespace
{

#if defined(OS_WINDOWS)

struct NetworkInterfaces : public boost::noncopyable
{
    /// Windows has no `getifaddrs`. `GetAdaptersAddresses` walks the adapters and, for each, a
    /// list of unicast addresses. Unlike `ifaddrs` the result is one allocation that we own, so
    /// collect what we need out of it up front rather than keeping the buffer alive.
    std::vector<Poco::Net::IPAddress> addresses;

    NetworkInterfaces()
    {
        /// The call reports the size it needs; the recommended starting buffer is 15 KB, and the
        /// set of adapters can change between the two calls, hence the retry.
        ULONG size = 15 * 1024;
        std::vector<char> buffer;
        ULONG result = ERROR_BUFFER_OVERFLOW;
        for (int attempt = 0; attempt < 3 && result == ERROR_BUFFER_OVERFLOW; ++attempt)
        {
            buffer.assign(size, 0);
            result = GetAdaptersAddresses(
                AF_UNSPEC,
                GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST | GAA_FLAG_SKIP_DNS_SERVER | GAA_FLAG_SKIP_FRIENDLY_NAME,
                nullptr,
                reinterpret_cast<IP_ADAPTER_ADDRESSES *>(buffer.data()),
                &size);
        }

        if (result != NO_ERROR)
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot GetAdaptersAddresses, error code: {}", result);

        for (auto * adapter = reinterpret_cast<IP_ADAPTER_ADDRESSES *>(buffer.data()); adapter; adapter = adapter->Next)
        {
            for (auto * unicast = adapter->FirstUnicastAddress; unicast; unicast = unicast->Next)
            {
                const auto * sockaddr = unicast->Address.lpSockaddr;
                if (!sockaddr)
                    continue;

                /// Only IP addresses, as in the POSIX branch.
                if (sockaddr->sa_family == AF_INET)
                    addresses.emplace_back(&reinterpret_cast<const sockaddr_in *>(sockaddr)->sin_addr, sizeof(in_addr));
                else if (sockaddr->sa_family == AF_INET6)
                    addresses.emplace_back(&reinterpret_cast<const sockaddr_in6 *>(sockaddr)->sin6_addr, sizeof(in6_addr));
            }
        }
    }

    bool hasAddress(const Poco::Net::IPAddress & address) const
    {
        for (const auto & interface_address : addresses)
        {
            /** Compare the addresses without taking into account `scope`, as the POSIX branch
              * does - see the note there.
              */
            if (interface_address.length() == address.length()
                && 0 == memcmp(interface_address.addr(), address.addr(), address.length()))
                return true;
        }
        return false;
    }
};

#else

struct NetworkInterfaces : public boost::noncopyable
{
    ifaddrs * ifaddr{};
    NetworkInterfaces()
    {
        if (getifaddrs(&ifaddr) == -1)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot getifaddrs");
    }

    bool hasAddress(const Poco::Net::IPAddress & address) const
    {
        ifaddrs * iface = nullptr;
        for (iface = ifaddr; iface != nullptr; iface = iface->ifa_next)
        {
            /// Point-to-point (VPN) addresses may have NULL ifa_addr
            if (!iface->ifa_addr)
                continue;

            auto family = iface->ifa_addr->sa_family;
            std::optional<Poco::Net::IPAddress> interface_address;
            switch (family)
            {
                /// We interested only in IP-addresses
                case AF_INET:
                {
                    interface_address.emplace(*(iface->ifa_addr));
                    break;
                }
                case AF_INET6:
                {
                    interface_address.emplace(&reinterpret_cast<const struct sockaddr_in6*>(iface->ifa_addr)->sin6_addr, sizeof(struct in6_addr));
                    break;
                }
                default:
                    continue;
            }

            /** Compare the addresses without taking into account `scope`.
              * Theoretically, this may not be correct - depends on `route` setting
              *  - through which interface we will actually access the specified address.
              */
            if (interface_address->length() == address.length()
                && 0 == memcmp(interface_address->addr(), address.addr(), address.length()))
                return true;
        }
        return false;
    }

    ~NetworkInterfaces()
    {
        freeifaddrs(ifaddr);
    }
};

#endif

}


bool isLocalAddress(const Poco::Net::IPAddress & address)
{
    /** 127.0.0.1 is treat as local address unconditionally.
      * ::1 is also treat as local address unconditionally.
      *
      * 127.0.0.{2..255} are not treat as local addresses, because they are used in tests
      *  to emulate distributed queries across localhost.
      *
      * But 127.{0,1}.{0,1}.{0,1} are treat as local addresses,
      *  because they are used in Debian for localhost.
      */
    if (address.isLoopback())
    {
        if (address.family() == Poco::Net::AddressFamily::IPv4)
        {
            /// The address is located in memory in big endian form (network byte order).
            const unsigned char * digits = static_cast<const unsigned char *>(address.addr());

            /// Decide by value only (see above); don't fall through to the interface scan, so
            /// 127.0.0.{2..255} stay non-local even when assigned to lo0 (e.g. macOS test aliases).
            return digits[0] == 127
                && digits[1] <= 1
                && digits[2] <= 1
                && digits[3] <= 1;
        }

        /// ::1
        return true;
    }

    static NetworkInterfaces network_interfaces;
    return network_interfaces.hasAddress(address);
}


bool isLocalAddress(const Poco::Net::SocketAddress & address, UInt16 clickhouse_port)
{
    return clickhouse_port == address.port() && isLocalAddress(address.host());
}

size_t getHostNamePrefixDistance(const std::string & local_hostname, const std::string & host)
{
    size_t hostname_difference = 0;
    for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
        if (local_hostname[i] != host[i])
            ++hostname_difference;
    return hostname_difference;
}

size_t getHostNameLevenshteinDistance(const std::string & local_hostname, const std::string & host)
{
    return levenshteinDistanceCaseInsensitive(local_hostname, host);
}

size_t getHostNameLongestCommonPrefix(const std::string & local_hostname, const std::string & host)
{
    /// Case-sensitive comparison, matching `getHostNamePrefixDistance` (`nearest_hostname`).
    const auto [it, _] = std::ranges::mismatch(local_hostname, host);
    return static_cast<size_t>(it - local_hostname.begin());
}

size_t getHostNameLongestCommonSuffix(const std::string & local_hostname, const std::string & host)
{
    /// Case-sensitive comparison, matching `getHostNamePrefixDistance` (`nearest_hostname`).
    const auto [it, _] = std::ranges::mismatch(local_hostname | std::views::reverse, host | std::views::reverse);
    return static_cast<size_t>(it - local_hostname.rbegin());
}

}
