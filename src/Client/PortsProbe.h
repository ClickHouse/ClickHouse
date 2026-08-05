#pragma once

#include <base/types.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Timespan.h>

#include <optional>

namespace DB
{

/// Result of concurrently probing TCP connectivity to the plain and the secure native protocol ports.
struct PortsProbeResult
{
    enum class Choice
    {
        PreferPlain,  /// The plain port is reachable. Connect to it first; the secure port may serve as a fallback.
        SecureOnly,   /// Only the secure port is reachable. Connect to it with TLS.
        Neither,      /// No port answered.
    };

    Choice choice = Choice::Neither;

    /// The address that answered on the chosen port. The host can resolve to several addresses, and only
    /// some of them may be reachable, so the caller has to connect to this one instead of starting over
    /// from the first resolved address: otherwise the connection waits out the timeout of every address
    /// that the probe has already found unresponsive.
    std::optional<Poco::Net::SocketAddress> address;

    /// The address that answered on the secure port when the plain port was chosen, if a secure
    /// probe had also connected by that time (with Choice::SecureOnly the secure address is
    /// `address` itself). When the connection to the plain port fails at the native protocol
    /// level (e.g. a proxy accepts TCP on the plain port but only serves TLS there) and the
    /// caller falls back to TLS on the secure port, the fallback has to start from this address,
    /// for the same reason as above.
    std::optional<Poco::Net::SocketAddress> secure_address;

    /// A description of the per-address failures, for Choice::Neither.
    String failure_reason;

    /// Whether some probe hit the timeout instead of failing outright. Affects the reported error code.
    bool timed_out = false;
};

/// Concurrently attempts TCP connections to `host` on both `plain_port` and `secure_port` (on every resolved
/// address of the host) to choose the protocol automatically when neither `port` nor `secure`/`no-secure`
/// is specified explicitly.
///
/// The plain port is preferred for compatibility: if it becomes reachable no later than `plain_preference_window`
/// after the secure port, the plain port is chosen. The secure port is chosen only when the plain port is
/// unreachable: refused, unroutable, or not answering within the window (as with servers behind a firewall
/// that silently drops packets to the plain port, e.g. play.clickhouse.com).
///
/// Only raw TCP reachability is checked, concurrently for all addresses and ports, bounded by `timeout` in total.
/// The caller performs the actual connection afterwards.
PortsProbeResult probePlainAndSecurePorts(
    const String & host,
    const String & bind_host,
    UInt16 plain_port,
    UInt16 secure_port,
    Poco::Timespan timeout,
    Poco::Timespan plain_preference_window);

}
