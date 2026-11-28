#pragma once

#include <string>
#include <string_view>

#include <base/types.h>
#include <boost/noncopyable.hpp>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

/// Applies `remote_url_allow_hosts` to the name resolutions of the whole process, when
/// `<remote_url_allow_hosts><check_name_resolution>true</check_name_resolution></remote_url_allow_hosts>` is set.
///
/// The checks of `RemoteHostFilter` are made by every integration before it connects, so they miss the hosts
/// that an integration did not check: the peers that a client library discovers by itself (the brokers advertised
/// by a Kafka cluster, the servers advertised by NATS, the members of a MongoDB replica set), redirects followed
/// inside a library, and call sites that forget the check. All of them resolve the host name first, so the filter
/// is applied in `getaddrinfo`, which the server interposes on Linux (see `NameResolutionFilter.cpp`), and in
/// `DNSResolver` (its cache and literal IP addresses do not reach `getaddrinfo`). A rejected resolution fails as
/// a host that does not exist (`EAI_NONAME`); `DNSResolver` throws `UNACCEPTABLE_URL` instead.
///
/// The check knows only the host, not the port: see `RemoteHostFilter::checkHostName`. Literal IP addresses
/// are checked as host names, as in `RemoteHostFilter`.
///
/// The server's own connections are not subject to `remote_url_allow_hosts`. They are exempted by the code path
/// that makes them, never by the host alone, so that a client library cannot be pointed at an internal host
/// (a NATS server advertising a replica of a cluster in `connect_urls`):
/// - a resolution made by a thread inside an `InternalScope` is not checked: ZooKeeper nodes, cloud credential
///   providers, fetches of parts from other replicas;
/// - hosts registered with `addInternalHost` (cluster replicas, the ODBC and library bridges, the endpoints of
///   the disks from the server configuration) are exempt only in the resolver of the server itself, `DNSResolver`
///   and the HTTP connection pool, which is what `check` is for. A `getaddrinfo` call of a library does not
///   consult them;
/// - endpoints registered with `addInternalEndpoint` are exempt in `getaddrinfo` as well, but only together with
///   their port. This is for the hosts that a library resolves in a thread which no `InternalScope` reaches:
///   the raft peers, resolved by a thread of `asio` inside NuRaft.
class NameResolutionFilter : private boost::noncopyable
{
public:
    /// Reads `remote_url_allow_hosts`. Called at startup and on every reload of the configuration.
    static void setValuesFromConfig(const Poco::Util::AbstractConfiguration & config);

    static bool isEnabled();

    /// Allows the resolver of the server itself (see `check`) to resolve `host` regardless of
    /// `remote_url_allow_hosts`, for hosts that the server connects to on its own. Does not affect `getaddrinfo`
    /// calls of libraries. The registration lasts until the server stops.
    static void addInternalHost(std::string_view host);

    /// Allows resolving `host` together with `port` regardless of `remote_url_allow_hosts`, in `getaddrinfo` too,
    /// for an endpoint that a library resolves in a thread of its own. Also registers `host` as `addInternalHost`.
    static void addInternalEndpoint(std::string_view host, UInt16 port);

    /// The same for an endpoint written as `host:port` or `[ipv6]:port`.
    static void addInternalEndpoint(std::string_view host_and_port);

    /// Whether a library may resolve `host` for the port or service name `service`. Logs a rejection.
    /// Called from `getaddrinfo`, so from any thread.
    static bool isAllowed(std::string_view host, std::string_view service = {});

    /// The check of the resolver of the server itself: throws `UNACCEPTABLE_URL` if `host` may not be resolved.
    /// Unlike `isAllowed`, it lets the hosts of `addInternalHost` through.
    static void check(std::string_view host);

    /// Exempts the name resolutions made by the current thread while the scope exists.
    class InternalScope : private boost::noncopyable
    {
    public:
        InternalScope();
        ~InternalScope();
    };
};

}
