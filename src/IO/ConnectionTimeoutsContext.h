#pragma once

#include <IO/ConnectionTimeouts.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

namespace DB
{

/// Timeouts for the case when we have just single attempt to connect.
inline ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithoutFailover(const Settings & settings)
{
    return ConnectionTimeouts(settings.connect_timeout, settings.send_timeout, settings.receive_timeout, settings.tcp_keep_alive_timeout);
}

/// Timeouts for the case when we will try many addresses in a loop.
inline ConnectionTimeouts ConnectionTimeouts::getTCPTimeoutsWithFailover(const Settings & settings)
{
    return ConnectionTimeouts(settings.connect_timeout_with_failover_ms, settings.send_timeout, settings.receive_timeout, settings.tcp_keep_alive_timeout, 0, settings.connect_timeout_with_failover_secure_ms);
}

inline ConnectionTimeouts ConnectionTimeouts::getHTTPTimeouts(const Context & context)
{
    const auto & settings = context.getSettingsRef();
    const auto & config = context.getConfigRef();
    Poco::Timespan http_keep_alive_timeout{config.getUInt("keep_alive_timeout", 10), 0};
    return ConnectionTimeouts(settings.http_connection_timeout, settings.http_send_timeout, settings.http_receive_timeout, settings.tcp_keep_alive_timeout, http_keep_alive_timeout);
}

}
