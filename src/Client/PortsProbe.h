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
        PreferSecure, /// The secure port is reachable. Connect to it with TLS.
        PlainOnly,    /// Only the plain port answered. Connect to it; the secure port may serve as a fallback.
        Neither,      /// No port answered.
    };

    Choice choice = Choice::Neither;

    /// The address that answered on the chosen port. The host can resolve to several addresses, and only
    /// some of them may be reachable, so the caller has to connect to this one instead of starting over
    /// from the first resolved address: otherwise the connection waits out the timeout of every address
    /// that the probe has already found unresponsive.
    std::optional<Poco::Net::SocketAddress> address;

    /// A description of the per-address failures, for Choice::Neither.
    String failure_reason;

    /// Whether some probe hit the timeout instead of failing outright. Affects the reported error code.
    bool timed_out = false;
};

/// Concurrently attempts TCP connections to `host` on both `plain_port` and `secure_port` (on every resolved
/// address of the host) to choose the protocol automatically when neither `port` nor `secure`/`no-secure`
/// is specified explicitly.
///
/// TLS is preferred: the secure port is chosen as soon as it becomes reachable. The plain port is chosen
/// only when the secure port is unreachable — refused, unroutable, or not answering within
/// `secure_preference_window` after the plain port (the window gives the secure port a head start over
/// a plain port that merely answered faster, without stalling plain-only servers, which is the most
/// common setup).
///
/// Only raw TCP reachability is checked, concurrently for all addresses and ports, bounded by `timeout` in total.
/// The caller performs the actual connection afterwards.
PortsProbeResult probePlainAndSecurePorts(
    const String & host,
    const String & bind_host,
    UInt16 plain_port,
    UInt16 secure_port,
    Poco::Timespan timeout,
    Poco::Timespan secure_preference_window);

}
