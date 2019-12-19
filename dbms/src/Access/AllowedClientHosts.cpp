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

    const AllowedClientHosts::IPSubnet ALL_ADDRESSES = AllowedClientHosts::IPSubnet{IPAddress{IPAddress::IPv6}, IPAddress{IPAddress::IPv6}};

    IPAddress toIPv6(const IPAddress & addr)
    {
        if (addr.family() == IPAddress::IPv6)
            return addr;

        return IPAddress("::FFFF:" + addr.toString());
    }


    IPAddress maskToIPv6(const IPAddress & mask)
    {
        if (mask.family() == IPAddress::IPv6)
            return mask;

        return IPAddress(96, IPAddress::IPv6) | toIPv6(mask);
    }


    bool isAddressOfHostImpl(const IPAddress & address, const String & host)
    {
        IPAddress addr_v6 = toIPv6(address);

        /// Resolve by hand, because Poco don't use AI_ALL flag but we need it.
        addrinfo * ai = nullptr;
        SCOPE_EXIT(
        {
            if (ai)
                freeaddrinfo(ai);
        });

        addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_flags |= AI_V4MAPPED | AI_ALL;

        int ret = getaddrinfo(host.c_str(), nullptr, &hints, &ai);
        if (0 != ret)
            throw Exception("Cannot getaddrinfo: " + std::string(gai_strerror(ret)), ErrorCodes::DNS_ERROR);

        for (; ai != nullptr; ai = ai->ai_next)
        {
            if (ai->ai_addrlen && ai->ai_addr)
            {
                if (ai->ai_family == AF_INET6)
                {
                    if (addr_v6 == IPAddress(
                        &reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_addr, sizeof(in6_addr),
                        reinterpret_cast<sockaddr_in6*>(ai->ai_addr)->sin6_scope_id))
                    {
                        return true;
                    }
                }
                else if (ai->ai_family == AF_INET)
                {
                    if (addr_v6 == toIPv6(IPAddress(&reinterpret_cast<sockaddr_in *>(ai->ai_addr)->sin_addr, sizeof(in_addr))))
                    {
                        return true;
                    }
                }
            }
        }

        return false;
    }


    /// Cached version of isAddressOfHostImpl(). We need to cache DNS requests.
    bool isAddressOfHost(const IPAddress & address, const String & host)
    {
        static SimpleCache<decltype(isAddressOfHostImpl), isAddressOfHostImpl> cache;
        return cache(address, host);
    }


    String getHostByAddressImpl(const IPAddress & address)
    {
        Poco::Net::SocketAddress sock_addr(address, 0);

        /// Resolve by hand, because Poco library doesn't have such functionality.
        char host[1024];
        int gai_errno = getnameinfo(sock_addr.addr(), sock_addr.length(), host, sizeof(host), nullptr, 0, NI_NAMEREQD);
        if (0 != gai_errno)
            throw Exception("Cannot getnameinfo: " + std::string(gai_strerror(gai_errno)), ErrorCodes::DNS_ERROR);

        /// Check that PTR record is resolved back to client address
        if (!isAddressOfHost(address, host))
            throw Exception("Host " + String(host) + " isn't resolved back to " + address.toString(), ErrorCodes::DNS_ERROR);
        return host;
    }


    /// Cached version of getHostByAddressImpl(). We need to cache DNS requests.
    String getHostByAddress(const IPAddress & address)
    {
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
    subnets = src.subnets;
    host_names = src.host_names;
    host_regexps = src.host_regexps;
    compiled_host_regexps.clear();
    return *this;
}


AllowedClientHosts::AllowedClientHosts(AllowedClientHosts && src)
{
    *this = src;
}


AllowedClientHosts & AllowedClientHosts::operator =(AllowedClientHosts && src)
{
    addresses = std::move(src.addresses);
    subnets = std::move(src.subnets);
    host_names = std::move(src.host_names);
    host_regexps = std::move(src.host_regexps);
    compiled_host_regexps = std::move(src.compiled_host_regexps);
    return *this;
}


void AllowedClientHosts::clear()
{
    addresses.clear();
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
    if (boost::range::find(addresses, addr_v6) == addresses.end())
        addresses.push_back(addr_v6);
}


void AllowedClientHosts::addAddress(const String & address)
{
    addAddress(IPAddress{address});
}


void AllowedClientHosts::addSubnet(const IPSubnet & subnet)
{
    IPSubnet subnet_v6;
    subnet_v6.prefix = toIPv6(subnet.prefix);
    subnet_v6.mask = maskToIPv6(subnet.mask);

    if (subnet_v6.mask == IPAddress(128, IPAddress::IPv6))
    {
        addAddress(subnet_v6.prefix);
        return;
    }

    subnet_v6.prefix = subnet_v6.prefix & subnet_v6.mask;

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
    if (boost::range::find(host_names, host_name) == host_names.end())
        host_names.push_back(host_name);
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


bool AllowedClientHosts::contains(const IPAddress & address) const
{
    return containsImpl(address, String(), nullptr);
}


void AllowedClientHosts::checkContains(const IPAddress & address, const String & user_name) const
{
    String error;
    if (!containsImpl(address, user_name, &error))
        throw Exception(error, ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
}


bool AllowedClientHosts::containsImpl(const IPAddress & address, const String & user_name, String * error) const
{
    if (error)
        error->clear();

    /// Check `ip_addresses`.
    IPAddress addr_v6 = toIPv6(address);
    if (boost::range::find(addresses, addr_v6) != addresses.end())
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
            if (isAddressOfHost(address, host_name))
                return true;
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::DNS_ERROR)
                e.rethrow();

            /// Try to ignore DNS errors: if host cannot be resolved, skip it and try next.
            LOG_WARNING(
                &Logger::get("AddressPatterns"),
                "Failed to check if the allowed client hosts contain address " << address.toString() << ". " << e.displayText()
                                                                               << ", code = " << e.code());
        }
    }

    /// Check `host_regexps`.
    if (!host_regexps.empty())
    {
        compileRegexps();
        try
        {
            String resolved_host = getHostByAddress(address);
            for (const auto & compiled_regexp : compiled_host_regexps)
            {
                if (compiled_regexp && compiled_regexp->match(resolved_host))
                    return true;
            }
        }
        catch (Exception & e)
        {
            if (e.code() != ErrorCodes::DNS_ERROR)
                e.rethrow();

            /// Try to ignore DNS errors: if host cannot be resolved, skip it and try next.
            LOG_WARNING(
                &Logger::get("AddressPatterns"),
                "Failed to check if the allowed client hosts contain address " << address.toString() << ". " << e.displayText()
                                                                               << ", code = " << e.code());
        }
    }

    if (error)
    {
        if (user_name.empty())
            *error = "It's not allowed to connect from address " + address.toString();
        else
            *error = "User " + user_name + " is not allowed to connect from address " + address.toString();
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
