#include <Access/AllowedClientHosts.h>
#include <Common/Exception.h>
#include <common/SimpleCache.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadHelpers.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/RegularExpression.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm/find_first_of.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <ifaddrs.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int DNS_ERROR;
    extern const int IP_ADDRESS_NOT_ALLOWED;
}

namespace
{
    using IPAddress = Poco::Net::IPAddress;
    using IPSubnet = AllowedClientHosts::IPSubnet;
    const IPSubnet ALL_ADDRESSES{IPAddress{IPAddress::IPv6}, IPAddress{IPAddress::IPv6}};

    const IPAddress & getIPV6Loopback()
    {
        static const IPAddress ip("::1");
        return ip;
    }

    bool isIPV4LoopbackMappedToIPV6(const IPAddress & ip)
    {
        static const IPAddress prefix("::ffff:127.0.0.0");
        /// 104 == 128 - 24, we have to reset the lowest 24 bits of 128 before comparing with `prefix`
        /// (IPv4 loopback means any IP from 127.0.0.0 to 127.255.255.255).
        return (ip & IPAddress(104, IPAddress::IPv6)) == prefix;
    }

    /// Converts an address to IPv6.
    /// The loopback address "127.0.0.1" (or any "127.x.y.z") is converted to "::1".
    IPAddress toIPv6(const IPAddress & ip)
    {
        IPAddress v6;
        if (ip.family() == IPAddress::IPv6)
            v6 = ip;
        else
            v6 = IPAddress("::ffff:" + ip.toString());

        // ::ffff:127.XX.XX.XX -> ::1
        if (isIPV4LoopbackMappedToIPV6(v6))
            v6 = getIPV6Loopback();

        return v6;
    }

    /// Converts a subnet to IPv6.
    IPSubnet toIPv6(const IPSubnet & subnet)
    {
        IPSubnet v6;
        if (subnet.prefix.family() == IPAddress::IPv6)
            v6.prefix = subnet.prefix;
        else
            v6.prefix = IPAddress("::ffff:" + subnet.prefix.toString());

        if (subnet.mask.family() == IPAddress::IPv6)
            v6.mask = subnet.mask;
        else
            v6.mask = IPAddress(96, IPAddress::IPv6) | IPAddress("::ffff:" + subnet.mask.toString());

        v6.prefix = v6.prefix & v6.mask;

        // ::ffff:127.XX.XX.XX -> ::1
        if (isIPV4LoopbackMappedToIPV6(v6.prefix))
            v6 = {getIPV6Loopback(), IPAddress(128, IPAddress::IPv6)};

        return v6;
    }

    /// Helper function for isAddressOfHost().
    bool isAddressOfHostImpl(const IPAddress & address, const String & host)
    {
        IPAddress addr_v6 = toIPv6(address);

        /// Resolve by hand, because Poco don't use AI_ALL flag but we need it.
        addrinfo * ai_begin = nullptr;
        SCOPE_EXIT(
        {
            if (ai_begin)
                freeaddrinfo(ai_begin);
        });

        addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_flags |= AI_V4MAPPED | AI_ALL;

        int err = getaddrinfo(host.c_str(), nullptr, &hints, &ai_begin);
        if (err)
            throw Exception("Cannot getaddrinfo(" + host + "): " + gai_strerror(err), ErrorCodes::DNS_ERROR);

        for (const addrinfo * ai = ai_begin; ai; ai = ai->ai_next)
        {
            if (ai->ai_addrlen && ai->ai_addr)
            {
                if (ai->ai_family == AF_INET)
                {
                    const auto & sin = *reinterpret_cast<const sockaddr_in *>(ai->ai_addr);
                    if (addr_v6 == toIPv6(IPAddress(&sin.sin_addr, sizeof(sin.sin_addr))))
                    {
                        return true;
                    }
                }
                else if (ai->ai_family == AF_INET6)
                {
                    const auto & sin = *reinterpret_cast<const sockaddr_in6*>(ai->ai_addr);
                    if (addr_v6 == IPAddress(&sin.sin6_addr, sizeof(sin.sin6_addr), sin.sin6_scope_id))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// Whether a specified address is one of the addresses of a specified host.
    bool isAddressOfHost(const IPAddress & address, const String & host)
    {
        /// We need to cache DNS requests.
        static SimpleCache<decltype(isAddressOfHostImpl), isAddressOfHostImpl> cache;
        return cache(address, host);
    }

    /// Helper function for isAddressOfLocalhost().
    std::vector<IPAddress> getAddressesOfLocalhostImpl()
    {
        std::vector<IPAddress> addresses;

        ifaddrs * ifa_begin = nullptr;
        SCOPE_EXIT({
            if (ifa_begin)
                freeifaddrs(ifa_begin);
        });

        int err = getifaddrs(&ifa_begin);
        if (err)
            return {getIPV6Loopback()};

        for (const ifaddrs * ifa = ifa_begin; ifa; ifa = ifa->ifa_next)
        {
            if (!ifa->ifa_addr)
                continue;
            if (ifa->ifa_addr->sa_family == AF_INET)
            {
                const auto & sin = *reinterpret_cast<const sockaddr_in *>(ifa->ifa_addr);
                addresses.push_back(toIPv6(IPAddress(&sin.sin_addr, sizeof(sin.sin_addr))));
            }
            else if (ifa->ifa_addr->sa_family == AF_INET6)
            {
                const auto & sin = *reinterpret_cast<const sockaddr_in6 *>(ifa->ifa_addr);
                addresses.push_back(IPAddress(&sin.sin6_addr, sizeof(sin.sin6_addr), sin.sin6_scope_id));
            }
        }
        return addresses;
    }

    /// Whether a specified address is one of the addresses of the localhost.
    bool isAddressOfLocalhost(const IPAddress & address)
    {
        /// We need to cache DNS requests.
        static const std::vector<IPAddress> local_addresses = getAddressesOfLocalhostImpl();
        return boost::range::find(local_addresses, toIPv6(address)) != local_addresses.end();
    }

    /// Helper function for getHostByAddress().
    String getHostByAddressImpl(const IPAddress & address)
    {
        Poco::Net::SocketAddress sock_addr(address, 0);

        /// Resolve by hand, because Poco library doesn't have such functionality.
        char host[1024];
        int err = getnameinfo(sock_addr.addr(), sock_addr.length(), host, sizeof(host), nullptr, 0, NI_NAMEREQD);
        if (err)
            throw Exception("Cannot getnameinfo(" + address.toString() + "): " + gai_strerror(err), ErrorCodes::DNS_ERROR);

        /// Check that PTR record is resolved back to client address
        if (!isAddressOfHost(address, host))
            throw Exception("Host " + String(host) + " isn't resolved back to " + address.toString(), ErrorCodes::DNS_ERROR);

        return host;
    }

    /// Returns the host name by its address.
    String getHostByAddress(const IPAddress & address)
    {
        /// We need to cache DNS requests.
        static SimpleCache<decltype(getHostByAddressImpl), &getHostByAddressImpl> cache;
        return cache(address);
    }
}


String AllowedClientHosts::IPSubnet::toString() const
{
    unsigned int prefix_length = mask.prefixLength();
    if (IPAddress{prefix_length, mask.family()} == mask)
        return prefix.toString() + "/" + std::to_string(prefix_length);

    return prefix.toString() + "/" + mask.toString();
}


AllowedClientHosts::AllowedClientHosts()
{
}


AllowedClientHosts::AllowedClientHosts(AllAddressesTag)
{
    addAllAddresses();
}


AllowedClientHosts::~AllowedClientHosts() = default;


AllowedClientHosts::AllowedClientHosts(const AllowedClientHosts & src)
{
    *this = src;
}


AllowedClientHosts & AllowedClientHosts::operator =(const AllowedClientHosts & src)
{
    addresses = src.addresses;
    localhost = src.localhost;
    subnets = src.subnets;
    host_names = src.host_names;
    host_regexps = src.host_regexps;
    compiled_host_regexps.clear();
    return *this;
}


AllowedClientHosts::AllowedClientHosts(AllowedClientHosts && src) = default;
AllowedClientHosts & AllowedClientHosts::operator =(AllowedClientHosts && src) = default;


void AllowedClientHosts::clear()
{
    addresses.clear();
    localhost = false;
    subnets.clear();
    host_names.clear();
    host_regexps.clear();
    compiled_host_regexps.clear();
}


bool AllowedClientHosts::empty() const
{
    return addresses.empty() && subnets.empty() && host_names.empty() && host_regexps.empty();
}


void AllowedClientHosts::addAddress(const IPAddress & address)
{
    IPAddress addr_v6 = toIPv6(address);
    if (boost::range::find(addresses, addr_v6) != addresses.end())
        return;
    addresses.push_back(addr_v6);
    if (addr_v6.isLoopback())
        localhost = true;
}


void AllowedClientHosts::addAddress(const String & address)
{
    addAddress(IPAddress{address});
}


void AllowedClientHosts::addSubnet(const IPSubnet & subnet)
{
    IPSubnet subnet_v6 = toIPv6(subnet);

    if (subnet_v6.mask == IPAddress(128, IPAddress::IPv6))
    {
        addAddress(subnet_v6.prefix);
        return;
    }

    if (boost::range::find(subnets, subnet_v6) == subnets.end())
        subnets.push_back(subnet_v6);
}


void AllowedClientHosts::addSubnet(const IPAddress & prefix, const IPAddress & mask)
{
    addSubnet(IPSubnet{prefix, mask});
}


void AllowedClientHosts::addSubnet(const IPAddress & prefix, size_t num_prefix_bits)
{
    addSubnet(prefix, IPAddress(num_prefix_bits, prefix.family()));
}


void AllowedClientHosts::addSubnet(const String & subnet)
{
    size_t slash = subnet.find('/');
    if (slash == String::npos)
    {
        addAddress(subnet);
        return;
    }

    IPAddress prefix{String{subnet, 0, slash}};
    String mask(subnet, slash + 1, subnet.length() - slash - 1);
    if (std::all_of(mask.begin(), mask.end(), isNumericASCII))
        addSubnet(prefix, parseFromString<UInt8>(mask));
    else
        addSubnet(prefix, IPAddress{mask});
}


void AllowedClientHosts::addHostName(const String & host_name)
{
    if (boost::range::find(host_names, host_name) != host_names.end())
        return;
    host_names.push_back(host_name);
    if (boost::iequals(host_name, "localhost"))
        localhost = true;
}


void AllowedClientHosts::addHostRegexp(const String & host_regexp)
{
    if (boost::range::find(host_regexps, host_regexp) == host_regexps.end())
        host_regexps.push_back(host_regexp);
}


void AllowedClientHosts::addAllAddresses()
{
    clear();
    addSubnet(ALL_ADDRESSES);
}


bool AllowedClientHosts::containsAllAddresses() const
{
    return (boost::range::find(subnets, ALL_ADDRESSES) != subnets.end())
        || (boost::range::find(host_regexps, ".*") != host_regexps.end())
        || (boost::range::find(host_regexps, "$") != host_regexps.end());
}


void AllowedClientHosts::checkContains(const IPAddress & address, const String & user_name) const
{
    if (!contains(address))
    {
        if (user_name.empty())
            throw Exception("It's not allowed to connect from address " + address.toString(), ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
        else
            throw Exception("User " + user_name + " is not allowed to connect from address " + address.toString(), ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
    }
}


bool AllowedClientHosts::contains(const IPAddress & address) const
{
    /// Check `ip_addresses`.
    IPAddress addr_v6 = toIPv6(address);
    if (boost::range::find(addresses, addr_v6) != addresses.end())
        return true;

    if (localhost && isAddressOfLocalhost(addr_v6))
        return true;

    /// Check `ip_subnets`.
    for (const auto & subnet : subnets)
        if ((addr_v6 & subnet.mask) == subnet.prefix)
            return true;

    /// Check `hosts`.
    for (const String & host_name : host_names)
    {
        try
        {
            if (isAddressOfHost(addr_v6, host_name))
                return true;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DNS_ERROR)
                throw;
            /// Try to ignore DNS errors: if host cannot be resolved, skip it and try next.
            LOG_WARNING(
                &Logger::get("AddressPatterns"),
                "Failed to check if the allowed client hosts contain address " << address.toString() << ". " << e.displayText()
                                                                               << ", code = " << e.code());
        }
    }

    /// Check `host_regexps`.
    try
    {
        String resolved_host = getHostByAddress(addr_v6);
        if (!resolved_host.empty())
        {
            compileRegexps();
            for (const auto & compiled_regexp : compiled_host_regexps)
            {
                Poco::RegularExpression::Match match;
                if (compiled_regexp && compiled_regexp->match(resolved_host, match))
                    return true;
            }
        }
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::DNS_ERROR)
            throw;
        /// Try to ignore DNS errors: if host cannot be resolved, skip it and try next.
        LOG_WARNING(
            &Logger::get("AddressPatterns"),
            "Failed to check if the allowed client hosts contain address " << address.toString() << ". " << e.displayText()
                                                                         << ", code = " << e.code());
    }

    return false;
}


void AllowedClientHosts::compileRegexps() const
{
    if (compiled_host_regexps.size() == host_regexps.size())
        return;
    size_t old_size = compiled_host_regexps.size();
    compiled_host_regexps.reserve(host_regexps.size());
    for (size_t i = old_size; i != host_regexps.size(); ++i)
        compiled_host_regexps.emplace_back(std::make_unique<Poco::RegularExpression>(host_regexps[i]));
}


bool operator ==(const AllowedClientHosts & lhs, const AllowedClientHosts & rhs)
{
    return (lhs.addresses == rhs.addresses) && (lhs.subnets == rhs.subnets) && (lhs.host_names == rhs.host_names)
        && (lhs.host_regexps == rhs.host_regexps);
}
}
