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
    bool secure = false;
    bool pending = false;
    bool timed_out = false;
    String failure;
};

/// The addresses of one port, tried one at a time (see `attempt_delay` below).
struct Port
{
    UInt16 port = 0;
    bool secure = false;
    std::vector<Poco::Net::SocketAddress> addresses;
    size_t next_address = 0;
    /// When the address that is currently being attempted was started.
    UInt64 attempt_started_at_us = 0;
};

}

PortsProbeResult probePlainAndSecurePorts(
    const String & host,
    const String & bind_host,
    UInt16 plain_port,
    UInt16 secure_port,
    Poco::Timespan timeout,
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
        Port{plain_port, /*secure=*/ false, DNSResolver::instance().resolveAddressList(host, plain_port), 0, 0},
        Port{secure_port, /*secure=*/ true, DNSResolver::instance().resolveAddressList(host, secure_port), 0, 0}};

    /// Pointers into this container are taken below, so it must not reallocate.
    std::deque<Probe> probes;

    Stopwatch watch;
    const UInt64 timeout_us = static_cast<UInt64>(timeout.totalMicroseconds());
    const UInt64 attempt_delay_us = static_cast<UInt64>(attempt_delay.totalMicroseconds());

    auto start_next_address = [&](Port & port)
    {
        auto & probe = probes.emplace_back();
        probe.address = port.addresses[port.next_address];
        probe.secure = port.secure;
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

    /// Whether an attempt on this port is in flight. There can be several of them at once: the next
    /// address of a port is attempted while the previous one is still unanswered (see `attempt_delay`).
    auto has_attempt_in_flight = [&](const Port & port)
    {
        return std::any_of(
            probes.begin(), probes.end(), [&](const Probe & probe) { return probe.pending && probe.secure == port.secure; });
    };

    while (true)
    {
        const UInt64 elapsed_us = watch.elapsedMicroseconds();

        if (elapsed_us >= timeout_us)
        {
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

        /// Start the next address of a port, either right away when its previous attempt has already
        /// failed, or once the attempt delay has elapsed without an answer.
        bool started_any = false;
        UInt64 next_attempt_at_us = std::numeric_limits<UInt64>::max();
        for (auto & port : ports)
        {
            if (port.next_address >= port.addresses.size())
                continue;

            /// A port with no attempt in flight goes on to its next address at once: the previous one has
            /// already failed (it was refused, for example), so there is nothing left to wait for.
            const UInt64 ready_at_us = has_attempt_in_flight(port) ? port.attempt_started_at_us + attempt_delay_us : 0;

            if (elapsed_us >= ready_at_us)
            {
                start_next_address(port);
                started_any = true;
            }
            else
                next_attempt_at_us = std::min(next_attempt_at_us, ready_at_us);
        }

        /// Wait for the attempts that have just been started as well.
        if (started_any)
            continue;

        /// Nothing left to start, and nothing left in flight (see above: a port with nothing in flight and
        /// an address left would have been started right here).
        if (std::none_of(probes.begin(), probes.end(), [](const Probe & probe) { return probe.pending; }))
            break;

        const UInt64 deadline_us = std::min(timeout_us, next_attempt_at_us);

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

        const Probe * answered = nullptr;
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
                /// The port that answers first is the one to use: when both are reachable, either of them
                /// will do. When both answered at the same time, TLS wins - preferring it here costs no
                /// waiting, unlike giving the secure port a head start over a plain port that answered.
                if (!answered || probe.secure)
                    answered = &probe;
            }
            else
            {
                probe.failure = error != 0 ? errnoToString(error) : "connection aborted";
            }
        }

        if (answered)
        {
            PortsProbeResult result;
            /// Poco sockets are reference-counted handles, so the connection outlives the probe it came from.
            result.endpoint = PortsProbeResult::Endpoint{answered->secure, answered->address, answered->socket};
            return result;
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
