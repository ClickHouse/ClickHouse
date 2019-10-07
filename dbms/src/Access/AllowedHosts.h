#pragma once

#include <Core/Types.h>
#include <Poco/Net/IPAddress.h>
#include <vector>
#include <memory>


namespace Poco
{
class RegularExpression;
}


namespace DB
{
/// Represents lists of hosts an user is allowed to connect to the server from.
class AllowedHosts
{
public:
    using IPAddress = Poco::Net::IPAddress;

    struct IPSubnet
    {
        IPAddress prefix;
        IPAddress mask;

        String toString() const;
        friend bool operator ==(const IPSubnet & lhs, const IPSubnet & rhs);
        friend bool operator !=(const IPSubnet & lhs, const IPSubnet & rhs) { return !(lhs == rhs); }
    };

    struct AllAddressesTag {};

    AllowedHosts();
    AllowedHosts(AllAddressesTag);
    ~AllowedHosts();

    AllowedHosts(const AllowedHosts & src);
    AllowedHosts & operator =(const AllowedHosts & src);
    AllowedHosts(AllowedHosts && src);
    AllowedHosts & operator =(AllowedHosts && src);

    /// Removes all contained hosts. This will allow all hosts.
    void clear();
    bool empty() const;

    /// Allows exact IP address.
    /// For example, 213.180.204.3 or 2a02:6b8::3
    void addAddress(const IPAddress & address);
    void addAddress(const String & address);

    /// Allows an IP subnet.
    void addSubnet(const IPSubnet & subnet);
    void addSubnet(const String & subnet);

    /// Allows an IP subnet.
    /// For example, 312.234.1.1/255.255.255.0 or 2a02:6b8::3/FFFF:FFFF:FFFF:FFFF::
    void addSubnet(const IPAddress & prefix, const IPAddress & mask);

    /// Allows an IP subnet.
    /// For example, 10.0.0.1/8 or 2a02:6b8::3/64
    void addSubnet(const IPAddress & prefix, size_t num_prefix_bits);

    /// Allows an exact host. The `contains()` function will check that the provided address equals to one of that host's addresses.
    void addHostName(const String & host_name);

    /// Allows a regular expression for the host.
    void addHostRegexp(const String & host_regexp);

    /// Allows all addresses.
    void addAllAddresses();
    bool containsAllAddresses() const;

    const std::unordered_set<IPAddress> & getIPAddresses() const { return ip_addresses; }
    const std::unordered_set<IPSubnet> & getIPSubnets() const { return ip_subnets; }
    const std::unordered_set<String> & getHostNames() const { return host_names; }
    const std::unordered_set<String> & getHostRegexps() const { return host_regexps; }

    /// Checks if the provided address is in the list. Returns false if not.
    bool contains(const IPAddress & address) const;

    /// Checks if the provided address is in the list. Throws an exception if not.
    /// `username` is only used for generating an error message if the address isn't in the list.
    void checkContains(const IPAddress & address, const String & user_name = String()) const;

    friend bool operator ==(const AllowedHosts & lhs, const AllowedHosts & rhs);
    friend bool operator !=(const AllowedHosts & lhs, const AllowedHosts & rhs) { return !(lhs == rhs); }

private:
    bool containsImpl(const IPAddress & address, const String & user_name, String * error) const;
    void ensureRegexpsCompiled() const;

    std::unordered_set<IPAddress> ip_addresses;
    std::unordered_set<IPSubnet> ip_subnets;
    std::unordered_set<String> host_names;
    std::unordered_set<String> host_regexps;
    mutable std::vector<std::unique_ptr<Poco::RegularExpression>> compiled_host_regexps;
};
}
