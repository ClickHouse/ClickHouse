#pragma once

#include <base/types.h>
#include <Poco/Net/IPAddress.h>
#include <vector>


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
        IPSubnet() = default;
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

    AllowedClientHosts() = default;
    AllowedClientHosts(AnyHostTag) { addAnyHost(); } /// NOLINT
    ~AllowedClientHosts() = default;

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

}
