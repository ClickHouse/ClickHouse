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
#include <vector>
#include <ranges>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/scope_guard_safe.h>
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

/** Compare the addresses without taking into account `scope`.
  * Theoretically, this may not be correct - depends on `route` setting
  *  - through which interface we will actually access the specified address.
  */
bool hasAddress(const std::vector<Poco::Net::IPAddress> & interface_addresses, const Poco::Net::IPAddress & address)
{
    for (const auto & interface_address : interface_addresses)
    {
        if (interface_address.length() == address.length()
            && 0 == memcmp(interface_address.addr(), address.addr(), address.length()))
            return true;
    }
    return false;
}

}

std::vector<Poco::Net::IPAddress> getLocalInterfaceAddresses()
{
    std::vector<Poco::Net::IPAddress> addresses;

#if defined(OS_WINDOWS)
    /// Windows has no `getifaddrs`. `GetAdaptersAddresses` walks the adapters and, for each, a
    /// list of unicast addresses. Unlike `ifaddrs` the result is one buffer that the caller owns,
    /// so collect what is needed out of it rather than keeping the buffer alive.
    ///
    /// The call reports the size it needs; the recommended starting buffer is 15 KB, and the set
    /// of adapters can change between the two calls, hence the retry.
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
            const auto * address = unicast->Address.lpSockaddr;
            if (!address)
                continue;

            /// Only IP addresses.
            if (address->sa_family == AF_INET)
                addresses.emplace_back(&reinterpret_cast<const sockaddr_in *>(address)->sin_addr, sizeof(in_addr));
            else if (address->sa_family == AF_INET6)
            {
                const auto & in6 = *reinterpret_cast<const sockaddr_in6 *>(address);
                addresses.emplace_back(&in6.sin6_addr, sizeof(in6_addr), in6.sin6_scope_id);
            }
        }
    }
#else
    ifaddrs * ifaddr = nullptr;
    if (getifaddrs(&ifaddr) == -1)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot getifaddrs");

    SCOPE_EXIT({ freeifaddrs(ifaddr); });

    for (const ifaddrs * iface = ifaddr; iface != nullptr; iface = iface->ifa_next)
    {
        /// Point-to-point (VPN) addresses may have NULL ifa_addr
        if (!iface->ifa_addr)
            continue;

        /// Only IP addresses.
        if (iface->ifa_addr->sa_family == AF_INET)
            addresses.emplace_back(*(iface->ifa_addr));
        else if (iface->ifa_addr->sa_family == AF_INET6)
        {
            const auto & in6 = *reinterpret_cast<const sockaddr_in6 *>(iface->ifa_addr);
            addresses.emplace_back(&in6.sin6_addr, sizeof(in6_addr), in6.sin6_scope_id);
        }
    }
#endif

    return addresses;
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

    static const std::vector<Poco::Net::IPAddress> interface_addresses = getLocalInterfaceAddresses();
    return hasAddress(interface_addresses, address);
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
