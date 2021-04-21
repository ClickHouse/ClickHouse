#pragma once

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

    ConnectionTimeouts() = default;

    ConnectionTimeouts(const Poco::Timespan & connection_timeout_,
                       const Poco::Timespan & send_timeout_,
                       const Poco::Timespan & receive_timeout_)
    : connection_timeout(connection_timeout_),
      send_timeout(send_timeout_),
      receive_timeout(receive_timeout_),
      tcp_keep_alive_timeout(0),
      http_keep_alive_timeout(0),
      secure_connection_timeout(connection_timeout),
      hedged_connection_timeout(receive_timeout_),
      receive_data_timeout(receive_timeout_)
    {
    }

    ConnectionTimeouts(const Poco::Timespan & connection_timeout_,
                       const Poco::Timespan & send_timeout_,
                       const Poco::Timespan & receive_timeout_,
                       const Poco::Timespan & tcp_keep_alive_timeout_)
    : connection_timeout(connection_timeout_),
      send_timeout(send_timeout_),
      receive_timeout(receive_timeout_),
      tcp_keep_alive_timeout(tcp_keep_alive_timeout_),
      http_keep_alive_timeout(0),
      secure_connection_timeout(connection_timeout),
      hedged_connection_timeout(receive_timeout_),
      receive_data_timeout(receive_timeout_)
    {
    }
    ConnectionTimeouts(const Poco::Timespan & connection_timeout_,
                       const Poco::Timespan & send_timeout_,
                       const Poco::Timespan & receive_timeout_,
                       const Poco::Timespan & tcp_keep_alive_timeout_,
                       const Poco::Timespan & http_keep_alive_timeout_)
        : connection_timeout(connection_timeout_),
          send_timeout(send_timeout_),
          receive_timeout(receive_timeout_),
          tcp_keep_alive_timeout(tcp_keep_alive_timeout_),
          http_keep_alive_timeout(http_keep_alive_timeout_),
          secure_connection_timeout(connection_timeout),
          hedged_connection_timeout(receive_timeout_),
          receive_data_timeout(receive_timeout_)
    {
    }

    ConnectionTimeouts(const Poco::Timespan & connection_timeout_,
                       const Poco::Timespan & send_timeout_,
                       const Poco::Timespan & receive_timeout_,
                       const Poco::Timespan & tcp_keep_alive_timeout_,
                       const Poco::Timespan & http_keep_alive_timeout_,
                       const Poco::Timespan & secure_connection_timeout_,
                       const Poco::Timespan & receive_hello_timeout_,
                       const Poco::Timespan & receive_data_timeout_)
        : connection_timeout(connection_timeout_),
          send_timeout(send_timeout_),
          receive_timeout(receive_timeout_),
          tcp_keep_alive_timeout(tcp_keep_alive_timeout_),
          http_keep_alive_timeout(http_keep_alive_timeout_),
          secure_connection_timeout(secure_connection_timeout_),
          hedged_connection_timeout(receive_hello_timeout_),
          receive_data_timeout(receive_data_timeout_)
    {
    }

    static Poco::Timespan saturate(const Poco::Timespan & timespan, const Poco::Timespan & limit)
    {
        if (limit.totalMicroseconds() == 0)
            return timespan;
        else
            return (timespan > limit) ? limit : timespan;
    }

    ConnectionTimeouts getSaturated(const Poco::Timespan & limit) const
    {
        return ConnectionTimeouts(saturate(connection_timeout, limit),
                                  saturate(send_timeout, limit),
                                  saturate(receive_timeout, limit),
                                  saturate(tcp_keep_alive_timeout, limit),
                                  saturate(http_keep_alive_timeout, limit),
                                  saturate(secure_connection_timeout, limit),
                                  saturate(hedged_connection_timeout, limit),
                                  saturate(receive_data_timeout, limit));
    }

    /// Timeouts for the case when we have just single attempt to connect.
    static ConnectionTimeouts getTCPTimeoutsWithoutFailover(const Settings & settings);
    /// Timeouts for the case when we will try many addresses in a loop.
    static ConnectionTimeouts getTCPTimeoutsWithFailover(const Settings & settings);
    static ConnectionTimeouts getHTTPTimeouts(ContextPtr context);
};

}
