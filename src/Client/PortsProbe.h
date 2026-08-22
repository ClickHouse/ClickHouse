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
    /// The port that answered, together with the connection to it.
    struct Endpoint
    {
        /// Whether it is the secure port that answered, i.e. whether TLS is to be used.
        bool secure = false;

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

    /// The endpoint that answered, if any.
    std::optional<Endpoint> endpoint;

    /// A description of the per-address failures, when neither port answered.
    String failure_reason;

    /// Whether some probe hit the timeout instead of failing outright. Affects the reported error code.
    bool timed_out = false;
};

/// Concurrently attempts TCP connections to `host` on both `plain_port` and `secure_port` (on every resolved
/// address of the host) to choose the protocol automatically when neither `port` nor `secure`/`no-secure`
/// is specified explicitly.
///
/// The port that answers first is the one to use: when both are reachable, either of them will do, so there
/// is nothing to be gained by waiting for the other one. TLS wins when both answer at the same time, which
/// costs no waiting.
///
/// Only raw TCP reachability is checked, bounded by `timeout` in total. The connection to the port that
/// answered is returned to the caller, which completes the handshake on it; the connections to the endpoints
/// that lost the race are closed.
///
/// The two ports are always probed concurrently, but the addresses of a port are attempted one at a time,
/// the next one only after `attempt_delay` without an answer (or immediately, when the previous attempt
/// has already failed). Otherwise a host that resolves to several reachable backends would leave an
/// accepted connection that sends nothing on every backend but the one that wins. This is the
/// "Connection Attempt Delay" of RFC 8305 (Happy Eyeballs), for the same reason.
PortsProbeResult probePlainAndSecurePorts(
    const String & host,
    const String & bind_host,
    UInt16 plain_port,
    UInt16 secure_port,
    Poco::Timespan timeout,
    Poco::Timespan attempt_delay);

}
