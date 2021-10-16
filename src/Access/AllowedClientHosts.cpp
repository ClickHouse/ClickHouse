#include <Access/AllowedClientHosts.h>
#include <Common/Exception.h>
#include <common/SimpleCache.h>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <Functions/likePatternToRegexp.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/RegularExpression.h>
#include <boost/algorithm/string/replace.hpp>
#include <ifaddrs.h>
#include <Common/DNSResolver.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int DNS_ERROR;
}

namespace
{
    using IPAddress = Poco::Net::IPAddress;
    using IPSubnet = AllowedClientHosts::IPSubnet;

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
        if ((v6 & IPAddress(104, IPAddress::IPv6)) == IPAddress("::ffff:127.0.0.0"))
            v6 = IPAddress{"::1"};

        return v6;
    }

    IPSubnet toIPv6(const IPSubnet & subnet)
    {
        return IPSubnet(toIPv6(subnet.getPrefix()), subnet.getMask());
    }

    /// Whether a specified address is one of the addresses of a specified host.
    bool isAddressOfHost(const IPAddress & address, const String & host)
    {
        IPAddress addr_v6 = toIPv6(address);

        auto host_addresses = DNSResolver::instance().resolveHostAll(host);

        for (const auto & addr : host_addresses)
        {
            if (addr.family() == IPAddress::Family::IPv4 && addr_v6 == toIPv6(addr))
                return true;
            else if (addr.family() == IPAddress::Family::IPv6 && addr_v6 == addr)
                return true;
        }

        return false;
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
            return {IPAddress{"::1"}};

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

    /// Returns the host name by its address.
    String getHostByAddress(const IPAddress & address)
    {
        String host = DNSResolver::instance().reverseResolve(address);

        /// Check that PTR record is resolved back to client address
        if (!isAddressOfHost(address, host))
            throw Exception("Host " + String(host) + " isn't resolved back to " + address.toString(), ErrorCodes::DNS_ERROR);

        return host;
    }


    void parseLikePatternIfIPSubnet(const String & pattern, IPSubnet & subnet, IPAddress::Family address_family)
    {
        size_t slash = pattern.find('/');
        if (slash != String::npos)
        {
            /// IP subnet, e.g. "192.168.0.0/16" or "192.168.0.0/255.255.0.0".
            subnet = IPSubnet{pattern};
            return;
        }

        bool has_wildcard = (pattern.find_first_of("%_") != String::npos);
        if (has_wildcard)
        {
            /// IP subnet specified with one of the wildcard characters, e.g. "192.168.%.%".
            String wildcard_replaced_with_zero_bits = pattern;
            String wildcard_replaced_with_one_bits = pattern;
            if (address_family == IPAddress::IPv6)
            {
                boost::algorithm::replace_all(wildcard_replaced_with_zero_bits, "_", "0");
                boost::algorithm::replace_all(wildcard_replaced_with_zero_bits, "%", "0000");
                boost::algorithm::replace_all(wildcard_replaced_with_one_bits, "_", "f");
                boost::algorithm::replace_all(wildcard_replaced_with_one_bits, "%", "ffff");
            }
            else if (address_family == IPAddress::IPv4)
            {
                boost::algorithm::replace_all(wildcard_replaced_with_zero_bits, "%", "0");
                boost::algorithm::replace_all(wildcard_replaced_with_one_bits, "%", "255");
            }

            IPAddress prefix{wildcard_replaced_with_zero_bits};
            IPAddress mask = ~(prefix ^ IPAddress{wildcard_replaced_with_one_bits});
            subnet = IPSubnet{prefix, mask};
            return;
        }

        /// Exact IP address.
        subnet = IPSubnet{pattern};
    }

    /// Extracts a subnet, a host name or a host name regular expression from a like pattern.
    void parseLikePattern(
        const String & pattern, std::optional<IPSubnet> & subnet, std::optional<String> & name, std::optional<String> & name_regexp)
    {
        /// If `host` starts with digits and a dot then it's an IP pattern, otherwise it's a hostname pattern.
        size_t first_not_digit = pattern.find_first_not_of("0123456789");
        if ((first_not_digit != String::npos) && (first_not_digit != 0) && (pattern[first_not_digit] == '.'))
        {
            parseLikePatternIfIPSubnet(pattern, subnet.emplace(), IPAddress::IPv4);
            return;
        }

        size_t first_not_hex = pattern.find_first_not_of("0123456789ABCDEFabcdef");
        if (((first_not_hex == 4) && pattern[first_not_hex] == ':') || pattern.starts_with("::"))
        {
            parseLikePatternIfIPSubnet(pattern, subnet.emplace(), IPAddress::IPv6);
            return;
        }

        bool has_wildcard = (pattern.find_first_of("%_") != String::npos);
        if (has_wildcard)
        {
            name_regexp = likePatternToRegexp(pattern);
            return;
        }

        name = pattern;
    }
}


bool AllowedClientHosts::contains(const IPAddress & client_address) const
{
    if (any_host)
        return true;

    IPAddress client_v6 = toIPv6(client_address);

    std::optional<bool> is_client_local_value;
    auto is_client_local = [&]
    {
        if (is_client_local_value)
            return *is_client_local_value;
        is_client_local_value = isAddressOfLocalhost(client_v6);
        return *is_client_local_value;
    };

    if (local_host && is_client_local())
        return true;

    /// Check `addresses`.
    auto check_address = [&](const IPAddress & address_)
    {
        IPAddress address_v6 = toIPv6(address_);
        if (address_v6.isLoopback())
            return is_client_local();
        return address_v6 == client_v6;
    };

    for (const auto & address : addresses)
        if (check_address(address))
            return true;

    /// Check `subnets`.
    auto check_subnet = [&](const IPSubnet & subnet_)
    {
        IPSubnet subnet_v6 = toIPv6(subnet_);
        if (subnet_v6.isMaskAllBitsOne())
            return check_address(subnet_v6.getPrefix());
        return (client_v6 & subnet_v6.getMask()) == subnet_v6.getPrefix();
    };

    for (const auto & subnet : subnets)
        if (check_subnet(subnet))
            return true;

    /// Check `names`.
    auto check_name = [&](const String & name_)
    {
        if (boost::iequals(name_, "localhost"))
            return is_client_local();
        try
        {
            return isAddressOfHost(client_v6, name_);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DNS_ERROR)
                throw;
            /// Try to ignore DNS errors: if host cannot be resolved, skip it and try next.
            LOG_WARNING(
                &Poco::Logger::get("AddressPatterns"),
                "Failed to check if the allowed client hosts contain address {}. {}, code = {}",
                client_address.toString(), e.displayText(), e.code());
            return false;
        }
    };

    for (const String & name : names)
        if (check_name(name))
            return true;

    /// Check `name_regexps`.
    std::optional<String> resolved_host;
    auto check_name_regexp = [&](const String & name_regexp_)
    {
        try
        {
            if (boost::iequals(name_regexp_, "localhost"))
                return is_client_local();
            if (!resolved_host)
                resolved_host = getHostByAddress(client_v6);
            if (resolved_host->empty())
                return false;
            Poco::RegularExpression re(name_regexp_);
            Poco::RegularExpression::Match match;
            return re.match(*resolved_host, match) != 0;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DNS_ERROR)
                throw;
            /// Try to ignore DNS errors: if host cannot be resolved, skip it and try next.
            LOG_WARNING(
                &Poco::Logger::get("AddressPatterns"),
                "Failed to check if the allowed client hosts contain address {}. {}, code = {}",
                client_address.toString(), e.displayText(), e.code());
            return false;
        }
    };

    for (const String & name_regexp : name_regexps)
        if (check_name_regexp(name_regexp))
            return true;

    auto check_like_pattern = [&](const String & pattern)
    {
        std::optional<IPSubnet> subnet;
        std::optional<String> name;
        std::optional<String> name_regexp;
        parseLikePattern(pattern, subnet, name, name_regexp);
        if (subnet)
            return check_subnet(*subnet);
        else if (name)
            return check_name(*name);
        else if (name_regexp)
            return check_name_regexp(*name_regexp);
        else
            return false;
    };

    for (const String & like_pattern : like_patterns)
        if (check_like_pattern(like_pattern))
            return true;

    return false;
}

}
