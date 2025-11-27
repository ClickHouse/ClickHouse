#include <Access/Common/AllowedClientHosts.h>
#include <Common/Exception.h>
#include <Common/likePatternToRegexp.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/RegularExpression.h>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <Common/DNSResolver.h>
#include <ifaddrs.h>
#include <filesystem>

namespace fs = std::filesystem;


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

        auto host_addresses = DNSResolver::instance().resolveHostAllInOriginOrder(host);

        for (const auto & addr : host_addresses)
        {
            if (addr.family() == IPAddress::Family::IPv4 && addr_v6 == toIPv6(addr))
                return true;
            if (addr.family() == IPAddress::Family::IPv6 && addr_v6 == addr)
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
    std::unordered_set<String> getHostsByAddress(const IPAddress & address)
    {
        auto hosts = DNSResolver::instance().reverseResolve(address);

        if (hosts.empty())
            throw Exception(ErrorCodes::DNS_ERROR, "{} could not be resolved", address.toString());


        for (const auto & host : hosts)
        {
            /// Check that PTR record is resolved back to client address
            if (!isAddressOfHost(address, host))
                throw Exception(ErrorCodes::DNS_ERROR, "Host {} isn't resolved back to {}", host, address.toString());
        }

        return hosts;
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


void AllowedClientHosts::IPSubnet::set(const IPAddress & prefix_, const IPAddress & mask_)
{
    prefix = prefix_;
    mask = mask_;

    if (prefix.family() != mask.family())
    {
        if (prefix.family() == IPAddress::IPv4)
            prefix = IPAddress("::ffff:" + prefix.toString());

        if (mask.family() == IPAddress::IPv4)
            mask = IPAddress(96, IPAddress::IPv6) | IPAddress("::ffff:" + mask.toString());
    }

    prefix = prefix & mask;

    if (prefix.family() == IPAddress::IPv4)
    {
        if ((prefix & IPAddress{8, IPAddress::IPv4}) == IPAddress{"127.0.0.0"})
        {
            // 127.XX.XX.XX -> 127.0.0.1
            prefix = IPAddress{"127.0.0.1"};
            mask = IPAddress{32, IPAddress::IPv4};
        }
    }
    else
    {
        if ((prefix & IPAddress{104, IPAddress::IPv6}) == IPAddress{"::ffff:127.0.0.0"})
        {
            // ::ffff:127.XX.XX.XX -> ::1
            prefix = IPAddress{"::1"};
            mask = IPAddress{128, IPAddress::IPv6};
        }
    }
}

void AllowedClientHosts::IPSubnet::set(const IPAddress & prefix_, size_t num_prefix_bits)
{
    set(prefix_, IPAddress(static_cast<unsigned>(num_prefix_bits), prefix_.family()));
}

void AllowedClientHosts::IPSubnet::set(const IPAddress & address)
{
    set(address, address.length() * 8);
}

AllowedClientHosts::IPSubnet::IPSubnet(const String & str)
{
    size_t slash = str.find('/');
    if (slash == String::npos)
    {
        set(IPAddress(str));
        return;
    }

    IPAddress new_prefix{String{str, 0, slash}};
    String mask_str(str, slash + 1, str.length() - slash - 1);
    bool only_digits = (mask_str.find_first_not_of("0123456789") == std::string::npos);
    if (only_digits)
        set(new_prefix, std::stoul(mask_str));
    else
        set(new_prefix, IPAddress{mask_str});
}

String AllowedClientHosts::IPSubnet::toString() const
{
    unsigned int prefix_length = mask.prefixLength();
    if (isMaskAllBitsOne())
        return prefix.toString();
    if (IPAddress{prefix_length, mask.family()} == mask)
        return fs::path(prefix.toString()) / std::to_string(prefix_length);
    return fs::path(prefix.toString()) / mask.toString();
}

bool AllowedClientHosts::IPSubnet::isMaskAllBitsOne() const
{
    return mask == IPAddress(mask.length() * 8, mask.family());
}


void AllowedClientHosts::clear()
{
    addresses = {};
    subnets = {};
    names = {};
    name_regexps = {};
    like_patterns = {};
    any_host = false;
    local_host = false;
}

bool AllowedClientHosts::empty() const
{
    return !any_host && !local_host && addresses.empty() && subnets.empty() && names.empty() && name_regexps.empty() && like_patterns.empty();
}

void AllowedClientHosts::addAddress(const IPAddress & address)
{
    if (address.isLoopback())
        local_host = true;
    else if (boost::range::find(addresses, address) == addresses.end())
        addresses.push_back(address);
}

void AllowedClientHosts::removeAddress(const IPAddress & address)
{
    if (address.isLoopback())
        local_host = false;
    else
        std::erase(addresses, address);
}

void AllowedClientHosts::addSubnet(const IPSubnet & subnet)
{
    if (subnet.getMask().isWildcard())
        any_host = true;
    else if (subnet.isMaskAllBitsOne())
        addAddress(subnet.getPrefix());
    else if (boost::range::find(subnets, subnet) == subnets.end())
        subnets.push_back(subnet);
}

void AllowedClientHosts::removeSubnet(const IPSubnet & subnet)
{
    if (subnet.getMask().isWildcard())
        any_host = false;
    else if (subnet.isMaskAllBitsOne())
        removeAddress(subnet.getPrefix());
    else
        std::erase(subnets, subnet);
}

void AllowedClientHosts::addName(const String & name)
{
    if (boost::iequals(name, "localhost"))
        local_host = true;
    else if (boost::range::find(names, name) == names.end())
        names.push_back(name);
}

void AllowedClientHosts::removeName(const String & name)
{
    if (boost::iequals(name, "localhost"))
        local_host = false;
    else
        std::erase(names, name);
}

void AllowedClientHosts::addNameRegexp(const String & name_regexp)
{
    if (boost::iequals(name_regexp, "localhost"))
        local_host = true;
    else if (name_regexp == ".*")
        any_host = true;
    else if (boost::range::find(name_regexps, name_regexp) == name_regexps.end())
        name_regexps.push_back(name_regexp);
}

void AllowedClientHosts::removeNameRegexp(const String & name_regexp)
{
    if (boost::iequals(name_regexp, "localhost"))
        local_host = false;
    else if (name_regexp == ".*")
        any_host = false;
    else
        std::erase(name_regexps, name_regexp);
}

void AllowedClientHosts::addLikePattern(const String & pattern)
{
    if (boost::iequals(pattern, "localhost") || (pattern == "127.0.0.1") || (pattern == "::1"))
        local_host = true;
    else if ((pattern == "%") || (pattern == "0.0.0.0/0") || (pattern == "::/0"))
        any_host = true;
    else if (boost::range::find(like_patterns, pattern) == name_regexps.end())
        like_patterns.push_back(pattern);
}

void AllowedClientHosts::removeLikePattern(const String & pattern)
{
    if (boost::iequals(pattern, "localhost") || (pattern == "127.0.0.1") || (pattern == "::1"))
        local_host = false;
    else if ((pattern == "%") || (pattern == "0.0.0.0/0") || (pattern == "::/0"))
        any_host = false;
    else
        std::erase(like_patterns, pattern);
}

void AllowedClientHosts::addLocalHost()
{
    local_host = true;
}

void AllowedClientHosts::removeLocalHost()
{
    local_host = false;
}

void AllowedClientHosts::addAnyHost()
{
    clear();
    any_host = true;
}

void AllowedClientHosts::add(const AllowedClientHosts & other)
{
    if (other.containsAnyHost())
    {
        addAnyHost();
        return;
    }
    if (other.containsLocalHost())
        addLocalHost();
    for (const IPAddress & address : other.getAddresses())
        addAddress(address);
    for (const IPSubnet & subnet : other.getSubnets())
        addSubnet(subnet);
    for (const String & name : other.getNames())
        addName(name);
    for (const String & name_regexp : other.getNameRegexps())
        addNameRegexp(name_regexp);
    for (const String & like_pattern : other.getLikePatterns())
        addLikePattern(like_pattern);
}

void AllowedClientHosts::remove(const AllowedClientHosts & other)
{
    if (other.containsAnyHost())
    {
        clear();
        return;
    }
    if (other.containsLocalHost())
        removeLocalHost();
    for (const IPAddress & address : other.getAddresses())
        removeAddress(address);
    for (const IPSubnet & subnet : other.getSubnets())
        removeSubnet(subnet);
    for (const String & name : other.getNames())
        removeName(name);
    for (const String & name_regexp : other.getNameRegexps())
        removeNameRegexp(name_regexp);
    for (const String & like_pattern : other.getLikePatterns())
        removeLikePattern(like_pattern);
}


bool operator ==(const AllowedClientHosts & lhs, const AllowedClientHosts & rhs)
{
    return (lhs.any_host == rhs.any_host) && (lhs.local_host == rhs.local_host) && (lhs.addresses == rhs.addresses)
        && (lhs.subnets == rhs.subnets) && (lhs.names == rhs.names) && (lhs.name_regexps == rhs.name_regexps)
        && (lhs.like_patterns == rhs.like_patterns);
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
                getLogger("AddressPatterns"),
                "Failed to check if the allowed client hosts contain address {}. {}, code = {}",
                client_address.toString(), e.displayText(), e.code());
            return false;
        }
    };

    for (const String & name : names)
        if (check_name(name))
            return true;

    /// Check `name_regexps`.
    std::optional<std::unordered_set<String>> resolved_hosts;
    auto check_name_regexp = [&](const String & name_regexp_)
    {
        try
        {
            if (boost::iequals(name_regexp_, "localhost"))
                return is_client_local();
            if (!resolved_hosts)
            {
                resolved_hosts = getHostsByAddress(client_address);
            }

            for (const auto & host : resolved_hosts.value())
            {
                Poco::RegularExpression re(name_regexp_);
                Poco::RegularExpression::Match match;
                if (re.match(host, match) != 0)
                {
                    return true;
                }
            }

            return false;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::DNS_ERROR)
                throw;
            /// Try to ignore DNS errors: if host cannot be resolved, skip it and try next.
            LOG_WARNING(
                getLogger("AddressPatterns"),
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
        if (name)
            return check_name(*name);
        if (name_regexp)
            return check_name_regexp(*name_regexp);
        return false;
    };

    for (const String & like_pattern : like_patterns)
        if (check_like_pattern(like_pattern))
            return true;

    return false;
}

}
