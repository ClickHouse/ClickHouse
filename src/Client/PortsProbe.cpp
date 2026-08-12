#include <Client/PortsProbe.h>

#include <Common/DNSResolver.h>
#include <Common/Stopwatch.h>
#include <base/errnoToString.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <Common/Exception.h>

#include <algorithm>
#include <array>
#include <deque>
#include <limits>
#include <optional>
#include <vector>

namespace DB
{

namespace
{

struct Probe
{
    Poco::Net::StreamSocket socket;
    Poco::Net::SocketAddress address;
    bool to_secure_port = false;
    bool pending = false;
    bool connected = false;
    bool timed_out = false;
    String failure;
};

/// The addresses of one port, tried one at a time (see `attempt_delay` below).
struct Port
{
    UInt16 port = 0;
    bool to_secure_port = false;
    std::vector<Poco::Net::SocketAddress> addresses;
    size_t next_address = 0;
    /// When the address that is currently being attempted was started.
    std::optional<UInt64> attempt_started_at_us;
};

PortsProbeResult::Endpoint makeEndpoint(const Probe & probe)
{
    /// Poco sockets are reference-counted handles, so the connection outlives the probe it came from.
    return PortsProbeResult::Endpoint{probe.address, probe.socket};
}

}

PortsProbeResult probePlainAndSecurePorts(
    const String & host,
    const String & bind_host,
    UInt16 plain_port,
    UInt16 secure_port,
    Poco::Timespan timeout,
    Poco::Timespan secure_preference_window,
    Poco::Timespan attempt_delay)
{
    /// The probes of a port are not started all at once: the addresses of a host are tried one at a
    /// time, and the next one only after this much without an answer. Otherwise a host that resolves to
    /// several reachable backends (a load balancer, most typically) would leave an accepted session that
    /// sends nothing on every backend but the one that wins, on every single connect.
    ///
    /// This is the "Connection Attempt Delay" of RFC 8305 (Happy Eyeballs), for the same reason and with
    /// the same default. Note that it does not apply between the two ports: those are always probed
    /// concurrently, which is the whole point of the probing - a firewalled plain port would otherwise
    /// stall the client for the entire connection timeout before TLS is even attempted.
    std::array<Port, 2> ports{
        Port{plain_port, /*to_secure_port=*/ false, DNSResolver::instance().resolveAddressList(host, plain_port), 0, {}},
        Port{secure_port, /*to_secure_port=*/ true, DNSResolver::instance().resolveAddressList(host, secure_port), 0, {}}};

    /// Pointers into this container are taken below, so it must not reallocate.
    std::deque<Probe> probes;

    Stopwatch watch;
    const UInt64 timeout_us = static_cast<UInt64>(timeout.totalMicroseconds());
    const UInt64 window_us = static_cast<UInt64>(secure_preference_window.totalMicroseconds());
    const UInt64 attempt_delay_us = static_cast<UInt64>(attempt_delay.totalMicroseconds());

    /// The moment the first probe of the plain port connected: starts the secure preference window.
    std::optional<UInt64> plain_connected_at_us;

    auto start_next_address = [&](Port & port)
    {
        auto & probe = probes.emplace_back();
        probe.address = port.addresses[port.next_address];
        probe.to_secure_port = port.to_secure_port;
        ++port.next_address;
        port.attempt_started_at_us = watch.elapsedMicroseconds();
        try
        {
            if (!bind_host.empty())
                probe.socket.bind(Poco::Net::SocketAddress(bind_host, 0), /*reuseAddress=*/ true);

            probe.socket.connectNB(probe.address);
            probe.pending = true;
        }
        catch (const Poco::Exception & e)
        {
            probe.pending = false;
            probe.failure = e.displayText();
        }
    };

    while (true)
    {
        const Probe * plain_connected = nullptr;
        const Probe * secure_connected = nullptr;
        bool plain_pending = false;
        bool secure_pending = false;

        for (const auto & probe : probes)
        {
            if (probe.connected)
            {
                const Probe *& connected = probe.to_secure_port ? secure_connected : plain_connected;
                if (!connected)
                    connected = &probe;
            }
            if (probe.pending)
                (probe.to_secure_port ? secure_pending : plain_pending) = true;
        }

        /// TLS is preferred, so the secure port wins as soon as it answers. The plain port is reported
        /// alongside it when it has answered too: the secure connection can still turn out to be unusable,
        /// and then the caller falls back to a port it already knows answers.
        if (secure_connected)
        {
            PortsProbeResult result;
            result.secure = makeEndpoint(*secure_connected);
            if (plain_connected)
                result.plain = makeEndpoint(*plain_connected);
            return result;
        }

        const UInt64 elapsed_us = watch.elapsedMicroseconds();

        /// Whether the port can still produce an answer: an attempt is in flight, or an address of it has
        /// not been attempted yet.
        auto can_still_answer = [&](const Port & port, bool port_pending)
        { return port_pending || port.next_address < port.addresses.size(); };

        const bool secure_can_still_answer = can_still_answer(ports[1], secure_pending);

        if (plain_connected && (!secure_can_still_answer || elapsed_us >= *plain_connected_at_us + window_us))
        {
            PortsProbeResult result;
            result.plain = makeEndpoint(*plain_connected);
            return result;
        }

        if (elapsed_us >= timeout_us)
        {
            if (plain_connected)
            {
                PortsProbeResult result;
                result.plain = makeEndpoint(*plain_connected);
                return result;
            }

            for (auto & probe : probes)
            {
                if (probe.pending)
                {
                    probe.pending = false;
                    probe.timed_out = true;
                    probe.failure = "timed out";
                }
            }
            break;
        }

        /// Start the next address of a port that has not answered yet, either right away when its previous
        /// attempt has already failed, or once the attempt delay has elapsed without an answer.
        bool started_any = false;
        UInt64 next_attempt_at_us = std::numeric_limits<UInt64>::max();
        for (size_t i = 0; i < ports.size(); ++i)
        {
            auto & port = ports[i];
            const bool port_connected = i == 0 ? plain_connected != nullptr : secure_connected != nullptr;
            const bool port_pending = i == 0 ? plain_pending : secure_pending;

            if (port_connected || port.next_address >= port.addresses.size())
                continue;

            /// A port with no attempt in flight goes on to its next address at once: the previous one has
            /// already failed (it was refused, for example), so there is nothing left to wait for.
            const UInt64 ready_at_us
                = port.attempt_started_at_us && port_pending ? *port.attempt_started_at_us + attempt_delay_us : 0;

            if (elapsed_us >= ready_at_us)
            {
                start_next_address(port);
                started_any = true;
            }
            else
                next_attempt_at_us = std::min(next_attempt_at_us, ready_at_us);
        }

        /// Re-evaluate with the attempts that have just been started.
        if (started_any)
            continue;

        /// Nothing left to start, and nothing left in flight (see above: a port with nothing in flight and
        /// an address left would have been started right here).
        if (!plain_pending && !secure_pending)
            break;

        UInt64 deadline_us = std::min(timeout_us, next_attempt_at_us);
        if (plain_connected)
            deadline_us = std::min(deadline_us, *plain_connected_at_us + window_us);

        /// Every deadline above is strictly in the future: an elapsed one is handled before this point.
        chassert(deadline_us > elapsed_us);

        Poco::Net::Socket::SocketList read_list;
        Poco::Net::Socket::SocketList write_list;
        Poco::Net::Socket::SocketList except_list;
        for (const auto & probe : probes)
        {
            if (probe.pending)
            {
                write_list.push_back(probe.socket);
                except_list.push_back(probe.socket);
            }
        }

        Poco::Net::Socket::select(read_list, write_list, except_list, Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(deadline_us - elapsed_us)));

        auto contains = [](const Poco::Net::Socket::SocketList & list, const Poco::Net::Socket & socket)
        {
            return std::find(list.begin(), list.end(), socket) != list.end();
        };

        for (auto & probe : probes)
        {
            if (!probe.pending)
                continue;

            const bool writable = contains(write_list, probe.socket);
            const bool excepted = contains(except_list, probe.socket);
            if (!writable && !excepted)
                continue;

            probe.pending = false;

            /// A non-blocking connect reports its outcome through SO_ERROR once the socket becomes writable
            /// (successful and refused connections both wake up the poll).
            const int error = probe.socket.impl()->socketError();
            if (error == 0 && writable)
            {
                probe.connected = true;
                if (!probe.to_secure_port && !plain_connected_at_us)
                    plain_connected_at_us = watch.elapsedMicroseconds();
            }
            else
            {
                probe.failure = error != 0 ? errnoToString(error) : "connection aborted";
            }
        }
    }

    /// Neither port answered: report the failure of every address that was attempted.
    PortsProbeResult result;
    for (const auto & probe : probes)
    {
        if (!result.failure_reason.empty())
            result.failure_reason += ", ";
        result.failure_reason += probe.address.toString() + " - " + probe.failure;
        result.timed_out |= probe.timed_out;
    }
    return result;
}

}
