#pragma once

#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>


namespace DB
{
/// Temporarily overrides socket send/receive timeouts and reset them back into destructor
/// If "limit_max_timeout" is true, timeouts could be only decreased (maxed by previous value).
struct TimeoutSetter
{
    TimeoutSetter(Poco::Net::StreamSocket & socket_,
        const Poco::Timespan & send_timeout_,
        const Poco::Timespan & receive_timeout_,
        bool limit_max_timeout = false);

    TimeoutSetter(Poco::Net::StreamSocket & socket_, const Poco::Timespan & timeout_, bool limit_max_timeout = false);

    ~TimeoutSetter();

    Poco::Net::StreamSocket & socket;

    Poco::Timespan send_timeout;
    Poco::Timespan receive_timeout;

    Poco::Timespan old_send_timeout;
    Poco::Timespan old_receive_timeout;
};
}
