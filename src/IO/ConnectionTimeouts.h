#pragma once

#include <Poco/Timespan.h>
#include <Core/Settings.h>

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

struct ConnectionTimeouts
{
    Poco::Timespan connection_timeout;
    Poco::Timespan send_timeout;
    Poco::Timespan receive_timeout;
    Poco::Timespan tcp_keep_alive_timeout;
    Poco::Timespan http_keep_alive_timeout;
    Poco::Timespan secure_connection_timeout;

    ConnectionTimeouts() = default;

    ConnectionTimeouts(const Poco::Timespan & connection_timeout_,
                       const Poco::Timespan & send_timeout_,
                       const Poco::Timespan & receive_timeout_)
    : connection_timeout(connection_timeout_),
      send_timeout(send_timeout_),
      receive_timeout(receive_timeout_),
      tcp_keep_alive_timeout(0),
      http_keep_alive_timeout(0),
      secure_connection_timeout(connection_timeout)
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
      secure_connection_timeout(connection_timeout)
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
          secure_connection_timeout(connection_timeout)
    {
    }

    ConnectionTimeouts(const Poco::Timespan & connection_timeout_,
                       const Poco::Timespan & send_timeout_,
                       const Poco::Timespan & receive_timeout_,
                       const Poco::Timespan & tcp_keep_alive_timeout_,
                       const Poco::Timespan & http_keep_alive_timeout_,
                       const Poco::Timespan & secure_connection_timeout_)
            : connection_timeout(connection_timeout_),
              send_timeout(send_timeout_),
              receive_timeout(receive_timeout_),
              tcp_keep_alive_timeout(tcp_keep_alive_timeout_),
              http_keep_alive_timeout(http_keep_alive_timeout_),
              secure_connection_timeout(secure_connection_timeout_)
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
                                  saturate(secure_connection_timeout, limit));
    }

    /// Timeouts for the case when we have just single attempt to connect.
    static ConnectionTimeouts getTCPTimeoutsWithoutFailover(const Settings & settings)
    {
        return ConnectionTimeouts(settings.connect_timeout, settings.send_timeout, settings.receive_timeout, settings.tcp_keep_alive_timeout);
    }

    /// Timeouts for the case when we will try many addresses in a loop.
    static ConnectionTimeouts getTCPTimeoutsWithFailover(const Settings & settings)
    {
        return ConnectionTimeouts(settings.connect_timeout_with_failover_ms, settings.send_timeout, settings.receive_timeout, settings.tcp_keep_alive_timeout, 0, settings.connect_timeout_with_failover_secure_ms);
    }

    static ConnectionTimeouts getHTTPTimeouts(const Context & context)
    {
        const auto & settings = context.getSettingsRef();
        const auto & config = context.getConfigRef();
        Poco::Timespan http_keep_alive_timeout{config.getUInt("keep_alive_timeout", 10), 0};
        return ConnectionTimeouts(settings.http_connection_timeout, settings.http_send_timeout, settings.http_receive_timeout, settings.tcp_keep_alive_timeout, http_keep_alive_timeout);
    }
};

}
