#include "DNSResolver.h"
#include <common/SimpleCache.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/NetException.h>
#include <Poco/NumberParser.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <arpa/inet.h>
#include <atomic>
#include <optional>

namespace ProfileEvents
{
    extern Event DNSError;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


/// Slightly altered implementation from https://github.com/pocoproject/poco/blob/poco-1.6.1/Net/src/SocketAddress.cpp#L86
static void splitHostAndPort(const std::string & host_and_port, std::string & out_host, UInt16 & out_port)
{
    String port_str;
    out_host.clear();

    auto it = host_and_port.begin();
    auto end = host_and_port.end();

    if (*it == '[') /// Try parse case '[<IPv6 or something else>]:<port>'
    {
        ++it;
        while (it != end && *it != ']')
            out_host += *it++;
        if (it == end)
            throw Exception("Malformed IPv6 address", ErrorCodes::BAD_ARGUMENTS);
        ++it;
    }
    else /// Case '<IPv4 or domain name or something else>:<port>'
    {
        while (it != end && *it != ':')
            out_host += *it++;
    }

    if (it != end && *it == ':')
    {
        ++it;
        while (it != end)
            port_str += *it++;
    }
    else
        throw Exception("Missing port number", ErrorCodes::BAD_ARGUMENTS);

    unsigned port;
    if (Poco::NumberParser::tryParseUnsigned(port_str, port) && port <= 0xFFFF)
    {
        out_port = static_cast<UInt16>(port);
    }
    else
    {
        struct servent * se = getservbyname(port_str.c_str(), nullptr);
        if (se)
            out_port = ntohs(static_cast<UInt16>(se->s_port));
        else
            throw Exception("Service not found", ErrorCodes::BAD_ARGUMENTS);
    }
}

static Poco::Net::IPAddress resolveIPAddressImpl(const std::string & host)
{
    /// NOTE: Poco::Net::DNS::resolveOne(host) doesn't work for IP addresses like 127.0.0.2
    /// Therefore we use SocketAddress constructor with dummy port to resolve IP
    return Poco::Net::SocketAddress(host, 0U).host();
}

struct DNSResolver::Impl
{
    SimpleCache<decltype(resolveIPAddressImpl), &resolveIPAddressImpl> cache_host;

    std::mutex drop_mutex;
    std::mutex update_mutex;

    /// Cached server host name
    std::optional<String> host_name;

    /// Store hosts, which was asked to resolve from last update of DNS cache.
    NameSet new_hosts;

    /// Store all hosts, which was whenever asked to resolve
    NameSet known_hosts;

    /// If disabled, will not make cache lookups, will resolve addresses manually on each call
    std::atomic<bool> disable_cache{false};
};


DNSResolver::DNSResolver() : impl(std::make_unique<DNSResolver::Impl>()) {}

Poco::Net::IPAddress DNSResolver::resolveHost(const std::string & host)
{
    if (impl->disable_cache)
        return resolveIPAddressImpl(host);

    addToNewHosts(host);
    return impl->cache_host(host);
}

Poco::Net::SocketAddress DNSResolver::resolveAddress(const std::string & host_and_port)
{
    if (impl->disable_cache)
        return Poco::Net::SocketAddress(host_and_port);

    String host;
    UInt16 port;
    splitHostAndPort(host_and_port, host, port);

    addToNewHosts(host);
    return Poco::Net::SocketAddress(impl->cache_host(host), port);
}

Poco::Net::SocketAddress DNSResolver::resolveAddress(const std::string & host, UInt16 port)
{
    if (impl->disable_cache)
        return Poco::Net::SocketAddress(host, port);

    addToNewHosts(host);
    return  Poco::Net::SocketAddress(impl->cache_host(host), port);
}

void DNSResolver::dropCache()
{
    impl->cache_host.drop();

    std::scoped_lock lock(impl->update_mutex, impl->drop_mutex);

    impl->known_hosts.clear();
    impl->new_hosts.clear();
    impl->host_name.reset();
}

void DNSResolver::setDisableCacheFlag(bool is_disabled)
{
    impl->disable_cache = is_disabled;
}

String DNSResolver::getHostName()
{
    if (impl->disable_cache)
        return Poco::Net::DNS::hostName();

    std::lock_guard lock(impl->drop_mutex);

    if (!impl->host_name.has_value())
        impl->host_name.emplace(Poco::Net::DNS::hostName());

    return *impl->host_name;
}

bool DNSResolver::updateCache()
{
    {
        std::lock_guard lock(impl->drop_mutex);
        for (const auto & host : impl->new_hosts)
            impl->known_hosts.insert(host);
        impl->new_hosts.clear();

        impl->host_name.emplace(Poco::Net::DNS::hostName());
    }

    std::lock_guard lock(impl->update_mutex);

    bool updated = false;
    String lost_hosts;
    for (const auto & host : impl->known_hosts)
    {
        try
        {
            updated |= updateHost(host);
        }
        catch (const Poco::Net::NetException &)
        {
            ProfileEvents::increment(ProfileEvents::DNSError);

            if (!lost_hosts.empty())
                lost_hosts += ", ";
            lost_hosts += host;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    if (!lost_hosts.empty())
        LOG_INFO(&Poco::Logger::get("DNSResolver"), "Cached hosts not found: {}", lost_hosts);

    return updated;
}

bool DNSResolver::updateHost(const String & host)
{
    /// Usage of updateHost implies that host is already in cache and there is no extra computations
    auto old_value = impl->cache_host(host);
    impl->cache_host.update(host);
    return old_value != impl->cache_host(host);
}

void DNSResolver::addToNewHosts(const String & host)
{
    std::lock_guard lock(impl->drop_mutex);
    impl->new_hosts.insert(host);
}

DNSResolver::~DNSResolver() = default;

DNSResolver & DNSResolver::instance()
{
    static DNSResolver ret;
    return ret;
}

}
