#pragma once

#include <base/types.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <optional>

namespace DB
{

/// Result of concurrently probing TCP connectivity to the plain and the secure native protocol ports.
struct PortsProbeResult
{
    /// A port that accepted a connection, together with that connection.
    struct Endpoint
    {
        /// The address that answered. The host can resolve to several addresses, and only some of them
        /// may be reachable, so the caller has to connect to this one instead of starting over from the
        /// first resolved address: otherwise the connection waits out the timeout of every address that
        /// the probe has already found unresponsive.
        Poco::Net::SocketAddress address;

        /// The connection the probe has established, handed over to the caller so that the real
        /// connection reuses it instead of opening a second one to the same endpoint. Otherwise every
        /// automatically detected connection would leave a short-lived session on the server, which
        /// sends nothing and is logged as `Client has not sent any data.`
        ///
        /// The socket is left non-blocking, as `connectNB` leaves it.
        Poco::Net::StreamSocket socket;
    };

    /// The endpoints that answered, if any.
    ///
    /// `secure` is set when the secure port answered within the preference window, i.e. when TLS is to
    /// be used. `plain` may be set alongside it, and then its connection is ready to be used if the
    /// secure connection turns out to be unusable after all (an untrusted certificate, for example).
    std::optional<Endpoint> plain;
    std::optional<Endpoint> secure;

    /// A description of the per-address failures, when neither port answered.
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
/// Only raw TCP reachability is checked, concurrently for all addresses and ports, bounded by `timeout` in
/// total. The connection to the chosen endpoint is returned to the caller, which completes the handshake
/// on it; the connections to the endpoints that lost the race are closed.
PortsProbeResult probePlainAndSecurePorts(
    const String & host,
    const String & bind_host,
    UInt16 plain_port,
    UInt16 secure_port,
    Poco::Timespan timeout,
    Poco::Timespan secure_preference_window);

}
