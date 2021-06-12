#pragma once

#include <common/types.h>
#include <Poco/Net/IPAddress.h>
#include <memory>
#include <vector>
#include <boost/range/algorithm/find.hpp>
#include <boost/range/algorithm_ext/erase.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

using Strings = std::vector<String>;

/// Represents lists of hosts a user is allowed to connect to server from.
class AllowedClientHosts
{
public:
    using IPAddress = Poco::Net::IPAddress;

    class IPSubnet
    {
    public:
        IPSubnet() {}
        IPSubnet(const IPAddress & prefix_, const IPAddress & mask_) { set(prefix_, mask_); }
        IPSubnet(const IPAddress & prefix_, size_t num_prefix_bits) { set(prefix_, num_prefix_bits); }
        explicit IPSubnet(const IPAddress & address) { set(address); }
        explicit IPSubnet(const String & str);

        const IPAddress & getPrefix() const { return prefix; }
        const IPAddress & getMask() const { return mask; }
        bool isMaskAllBitsOne() const;
        String toString() const;

        friend bool operator ==(const IPSubnet & lhs, const IPSubnet & rhs) { return (lhs.prefix == rhs.prefix) && (lhs.mask == rhs.mask); }
        friend bool operator !=(const IPSubnet & lhs, const IPSubnet & rhs) { return !(lhs == rhs); }

    private:
        void set(const IPAddress & prefix_, const IPAddress & mask_);
        void set(const IPAddress & prefix_, size_t num_prefix_bits);
        void set(const IPAddress & address);

        IPAddress prefix;
        IPAddress mask;
    };

    struct AnyHostTag {};

    AllowedClientHosts() {}
    AllowedClientHosts(AnyHostTag) { addAnyHost(); }
    ~AllowedClientHosts() {}

    AllowedClientHosts(const AllowedClientHosts & src) = default;
    AllowedClientHosts & operator =(const AllowedClientHosts & src) = default;
    AllowedClientHosts(AllowedClientHosts && src) = default;
    AllowedClientHosts & operator =(AllowedClientHosts && src) = default;

    /// Removes all contained addresses. This will disallow all hosts.
    void clear();

    bool empty() const;

    /// Allows exact IP address.
    /// For example, 213.180.204.3 or 2a02:6b8::3
    void addAddress(const IPAddress & address);
    void addAddress(const String & address) { addAddress(IPAddress(address)); }
    void removeAddress(const IPAddress & address);
    void removeAddress(const String & address) { removeAddress(IPAddress{address}); }
    const std::vector<IPAddress> & getAddresses() const { return addresses; }

    /// Allows an IP subnet.
    /// For example, 312.234.1.1/255.255.255.0 or 2a02:6b8::3/64
    void addSubnet(const IPSubnet & subnet);
    void addSubnet(const String & subnet) { addSubnet(IPSubnet{subnet}); }
    void addSubnet(const IPAddress & prefix, const IPAddress & mask) { addSubnet(IPSubnet{prefix, mask}); }
    void addSubnet(const IPAddress & prefix, size_t num_prefix_bits) { addSubnet(IPSubnet{prefix, num_prefix_bits}); }
    void removeSubnet(const IPSubnet & subnet);
    void removeSubnet(const String & subnet) { removeSubnet(IPSubnet{subnet}); }
    void removeSubnet(const IPAddress & prefix, const IPAddress & mask) { removeSubnet(IPSubnet{prefix, mask}); }
    void removeSubnet(const IPAddress & prefix, size_t num_prefix_bits) { removeSubnet(IPSubnet{prefix, num_prefix_bits}); }
    const std::vector<IPSubnet> & getSubnets() const { return subnets; }

    /// Allows an exact host name. The `contains()` function will check that the provided address equals to one of that host's addresses.
    void addName(const String & name);
    void removeName(const String & name);
    const std::vector<String> & getNames() const { return names; }

    /// Allows the host names matching a regular expression.
    void addNameRegexp(const String & name_regexp);
    void removeNameRegexp(const String & name_regexp);
    const std::vector<String> & getNameRegexps() const { return name_regexps; }

    /// Allows IP addresses or host names using LIKE pattern.
    /// This pattern can contain % and _ wildcard characters.
    /// For example, addLikePattern("%") will allow all addresses.
    void addLikePattern(const String & pattern);
    void removeLikePattern(const String & like_pattern);
    const std::vector<String> & getLikePatterns() const { return like_patterns; }

    /// Allows local host.
    void addLocalHost();
    void removeLocalHost();
    bool containsLocalHost() const { return local_host;}

    /// Allows any host.
    void addAnyHost();
    bool containsAnyHost() const { return any_host;}

    void add(const AllowedClientHosts & other);
    void remove(const AllowedClientHosts & other);

    /// Checks if the provided address is in the list. Returns false if not.
    bool contains(const IPAddress & address) const;

    friend bool operator ==(const AllowedClientHosts & lhs, const AllowedClientHosts & rhs);
    friend bool operator !=(const AllowedClientHosts & lhs, const AllowedClientHosts & rhs) { return !(lhs == rhs); }

private:
    std::vector<IPAddress> addresses;
    std::vector<IPSubnet> subnets;
    Strings names;
    Strings name_regexps;
    Strings like_patterns;
    bool any_host = false;
    bool local_host = false;
};


inline void AllowedClientHosts::IPSubnet::set(const IPAddress & prefix_, const IPAddress & mask_)
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

inline void AllowedClientHosts::IPSubnet::set(const IPAddress & prefix_, size_t num_prefix_bits)
{
    set(prefix_, IPAddress(num_prefix_bits, prefix_.family()));
}

inline void AllowedClientHosts::IPSubnet::set(const IPAddress & address)
{
    set(address, address.length() * 8);
}

inline AllowedClientHosts::IPSubnet::IPSubnet(const String & str)
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

inline String AllowedClientHosts::IPSubnet::toString() const
{
    unsigned int prefix_length = mask.prefixLength();
    if (isMaskAllBitsOne())
        return prefix.toString();
    else if (IPAddress{prefix_length, mask.family()} == mask)
        return fs::path(prefix.toString()) / std::to_string(prefix_length);
    else
        return fs::path(prefix.toString()) / mask.toString();
}

inline bool AllowedClientHosts::IPSubnet::isMaskAllBitsOne() const
{
    return mask == IPAddress(mask.length() * 8, mask.family());
}


inline void AllowedClientHosts::clear()
{
    addresses = {};
    subnets = {};
    names = {};
    name_regexps = {};
    like_patterns = {};
    any_host = false;
    local_host = false;
}

inline bool AllowedClientHosts::empty() const
{
    return !any_host && !local_host && addresses.empty() && subnets.empty() && names.empty() && name_regexps.empty() && like_patterns.empty();
}

inline void AllowedClientHosts::addAddress(const IPAddress & address)
{
    if (address.isLoopback())
        local_host = true;
    else if (boost::range::find(addresses, address) == addresses.end())
        addresses.push_back(address);
}

inline void AllowedClientHosts::removeAddress(const IPAddress & address)
{
    if (address.isLoopback())
        local_host = false;
    else
        boost::range::remove_erase(addresses, address);
}

inline void AllowedClientHosts::addSubnet(const IPSubnet & subnet)
{
    if (subnet.getMask().isWildcard())
        any_host = true;
    else if (subnet.isMaskAllBitsOne())
        addAddress(subnet.getPrefix());
    else if (boost::range::find(subnets, subnet) == subnets.end())
        subnets.push_back(subnet);
}

inline void AllowedClientHosts::removeSubnet(const IPSubnet & subnet)
{
    if (subnet.getMask().isWildcard())
        any_host = false;
    else if (subnet.isMaskAllBitsOne())
        removeAddress(subnet.getPrefix());
    else
        boost::range::remove_erase(subnets, subnet);
}

inline void AllowedClientHosts::addName(const String & name)
{
    if (boost::iequals(name, "localhost"))
        local_host = true;
    else if (boost::range::find(names, name) == names.end())
        names.push_back(name);
}

inline void AllowedClientHosts::removeName(const String & name)
{
    if (boost::iequals(name, "localhost"))
        local_host = false;
    else
        boost::range::remove_erase(names, name);
}

inline void AllowedClientHosts::addNameRegexp(const String & name_regexp)
{
    if (boost::iequals(name_regexp, "localhost"))
        local_host = true;
    else if (name_regexp == ".*")
        any_host = true;
    else if (boost::range::find(name_regexps, name_regexp) == name_regexps.end())
        name_regexps.push_back(name_regexp);
}

inline void AllowedClientHosts::removeNameRegexp(const String & name_regexp)
{
    if (boost::iequals(name_regexp, "localhost"))
        local_host = false;
    else if (name_regexp == ".*")
        any_host = false;
    else
        boost::range::remove_erase(name_regexps, name_regexp);
}

inline void AllowedClientHosts::addLikePattern(const String & pattern)
{
    if (boost::iequals(pattern, "localhost") || (pattern == "127.0.0.1") || (pattern == "::1"))
        local_host = true;
    else if ((pattern == "%") || (pattern == "0.0.0.0/0") || (pattern == "::/0"))
        any_host = true;
    else if (boost::range::find(like_patterns, pattern) == name_regexps.end())
        like_patterns.push_back(pattern);
}

inline void AllowedClientHosts::removeLikePattern(const String & pattern)
{
    if (boost::iequals(pattern, "localhost") || (pattern == "127.0.0.1") || (pattern == "::1"))
        local_host = false;
    else if ((pattern == "%") || (pattern == "0.0.0.0/0") || (pattern == "::/0"))
        any_host = false;
    else
        boost::range::remove_erase(like_patterns, pattern);
}

inline void AllowedClientHosts::addLocalHost()
{
    local_host = true;
}

inline void AllowedClientHosts::removeLocalHost()
{
    local_host = false;
}

inline void AllowedClientHosts::addAnyHost()
{
    clear();
    any_host = true;
}

inline void AllowedClientHosts::add(const AllowedClientHosts & other)
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

inline void AllowedClientHosts::remove(const AllowedClientHosts & other)
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


inline bool operator ==(const AllowedClientHosts & lhs, const AllowedClientHosts & rhs)
{
    return (lhs.any_host == rhs.any_host) && (lhs.local_host == rhs.local_host) && (lhs.addresses == rhs.addresses)
        && (lhs.subnets == rhs.subnets) && (lhs.names == rhs.names) && (lhs.name_regexps == rhs.name_regexps)
        && (lhs.like_patterns == rhs.like_patterns);
}

}
