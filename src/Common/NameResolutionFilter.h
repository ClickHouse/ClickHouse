#pragma once

#include <string>
#include <string_view>

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
/// The server's own connections are not subject to `remote_url_allow_hosts`. They are exempted in two ways:
/// - hosts that come from the server configuration or from the cluster metadata are registered with
///   `addInternalHost`: ZooKeeper and Keeper nodes, cluster replicas, the ODBC and library bridges;
/// - a resolution made by a thread inside an `InternalScope` is not checked: the server's own host name,
///   cloud credential providers, fetches of parts from other replicas.
class NameResolutionFilter : private boost::noncopyable
{
public:
    /// Reads `remote_url_allow_hosts`. Called at startup and on every reload of the configuration.
    static void setValuesFromConfig(const Poco::Util::AbstractConfiguration & config);

    static bool isEnabled();

    /// Allows resolving `host` regardless of `remote_url_allow_hosts`, for hosts that the server connects to on its own.
    /// The registration lasts until the server stops.
    static void addInternalHost(std::string_view host);

    /// Whether `host` may be resolved. Logs a rejection. Called from `getaddrinfo`, so from any thread.
    static bool isAllowed(std::string_view host);

    /// Throws `UNACCEPTABLE_URL` if `host` may not be resolved.
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
