#include <Client/PortsProbe.h>

#include <Common/DNSResolver.h>
#include <Common/Stopwatch.h>
#include <base/errnoToString.h>

#include <Poco/Net/NetException.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <algorithm>
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

}

PortsProbeResult probePlainAndSecurePorts(
    const String & host,
    const String & bind_host,
    UInt16 plain_port,
    UInt16 secure_port,
    Poco::Timespan timeout,
    Poco::Timespan secure_preference_window)
{
    std::vector<Probe> probes;

    auto add_probes = [&](UInt16 port, bool to_secure_port)
    {
        for (const auto & address : DNSResolver::instance().resolveAddressList(host, port))
        {
            auto & probe = probes.emplace_back();
            probe.address = address;
            probe.to_secure_port = to_secure_port;
            try
            {
                if (!bind_host.empty())
                    probe.socket.bind(Poco::Net::SocketAddress(bind_host, 0), /*reuseAddress=*/ true);

                probe.socket.connectNB(address);
                probe.pending = true;
            }
            catch (const Poco::Exception & e)
            {
                probe.pending = false;
                probe.failure = e.displayText();
            }
        }
    };

    add_probes(plain_port, /*to_secure_port=*/ false);
    add_probes(secure_port, /*to_secure_port=*/ true);

    Stopwatch watch;
    const UInt64 timeout_us = static_cast<UInt64>(timeout.totalMicroseconds());
    const UInt64 window_us = static_cast<UInt64>(secure_preference_window.totalMicroseconds());

    /// The moment the first probe of the plain port connected: starts the secure preference window.
    std::optional<UInt64> plain_connected_at_us;

    /// The probes are all created before this loop, so pointers into `probes` stay valid.
    auto chosen = [](PortsProbeResult::Choice choice, const Probe & probe)
    {
        PortsProbeResult result;
        result.choice = choice;
        result.address = probe.address;
        return result;
    };

    while (true)
    {
        const Probe * plain_connected = nullptr;
        const Probe * secure_connected = nullptr;
        bool secure_pending = false;
        bool any_pending = false;

        for (const auto & probe : probes)
        {
            if (probe.connected)
            {
                const Probe *& connected = probe.to_secure_port ? secure_connected : plain_connected;
                if (!connected)
                    connected = &probe;
            }
            if (probe.pending)
            {
                any_pending = true;
                if (probe.to_secure_port)
                    secure_pending = true;
            }
        }

        if (secure_connected)
            return chosen(PortsProbeResult::Choice::PreferSecure, *secure_connected);

        const UInt64 elapsed_us = watch.elapsedMicroseconds();

        if (plain_connected && (!secure_pending || elapsed_us >= *plain_connected_at_us + window_us))
            return chosen(PortsProbeResult::Choice::PlainOnly, *plain_connected);

        if (!any_pending)
            break;

        UInt64 deadline_us = timeout_us;
        if (plain_connected)
            deadline_us = std::min(deadline_us, *plain_connected_at_us + window_us);

        if (elapsed_us >= deadline_us)
        {
            if (plain_connected)
                return chosen(PortsProbeResult::Choice::PlainOnly, *plain_connected);

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

    /// Neither port answered: report the failure of every probed address.
    PortsProbeResult result;
    result.choice = PortsProbeResult::Choice::Neither;
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
