#pragma once

#include <Core/Defines.h>
#include <Interpreters/Context_fwd.h>

#include <Poco/Timespan.h>

namespace DB
{

struct Settings;

struct ConnectionTimeouts
{
    Poco::Timespan connection_timeout;
    Poco::Timespan send_timeout;
    Poco::Timespan receive_timeout;
    Poco::Timespan tcp_keep_alive_timeout;
    Poco::Timespan http_keep_alive_timeout;
    Poco::Timespan secure_connection_timeout;

    /// Timeouts for HedgedConnections
    Poco::Timespan hedged_connection_timeout;
    Poco::Timespan receive_data_timeout;

    /// Timeout for receiving HELLO packet
    Poco::Timespan handshake_timeout;

    /// Timeout for synchronous request-result protocol call (like Ping or TablesStatus)
    Poco::Timespan sync_request_timeout = Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0);

    ConnectionTimeouts() = default;

    ConnectionTimeouts(Poco::Timespan connection_timeout_,
                       Poco::Timespan send_timeout_,
                       Poco::Timespan receive_timeout_);

    ConnectionTimeouts(Poco::Timespan connection_timeout_,
                       Poco::Timespan send_timeout_,
                       Poco::Timespan receive_timeout_,
                       Poco::Timespan tcp_keep_alive_timeout_,
                       Poco::Timespan handshake_timeout_);

    ConnectionTimeouts(Poco::Timespan connection_timeout_,
                       Poco::Timespan send_timeout_,
                       Poco::Timespan receive_timeout_,
                       Poco::Timespan tcp_keep_alive_timeout_,
                       Poco::Timespan http_keep_alive_timeout_,
                       Poco::Timespan handshake_timeout_);

    ConnectionTimeouts(Poco::Timespan connection_timeout_,
                       Poco::Timespan send_timeout_,
                       Poco::Timespan receive_timeout_,
                       Poco::Timespan tcp_keep_alive_timeout_,
                       Poco::Timespan http_keep_alive_timeout_,
                       Poco::Timespan secure_connection_timeout_,
                       Poco::Timespan hedged_connection_timeout_,
                       Poco::Timespan receive_data_timeout_,
                       Poco::Timespan handshake_timeout_);

    static Poco::Timespan saturate(Poco::Timespan timespan, Poco::Timespan limit);
    ConnectionTimeouts getSaturated(Poco::Timespan limit) const;

    /// Timeouts for the case when we have just single attempt to connect.
    static ConnectionTimeouts getTCPTimeoutsWithoutFailover(const Settings & settings);

    /// Timeouts for the case when we will try many addresses in a loop.
    static ConnectionTimeouts getTCPTimeoutsWithFailover(const Settings & settings);
    static ConnectionTimeouts getHTTPTimeouts(const Settings & settings, Poco::Timespan http_keep_alive_timeout);
};

}
