#pragma once

#include <Poco/Timespan.h>
#include <Poco/Net/StreamSocket.h>


namespace DB
{

/// Temporarily overrides socket send/recieve timeouts and reset them back into destructor
/// If "limit_max_timeout" is true, timeouts could be only decreased (maxed by previous value).
struct TimeoutSetter
{
    TimeoutSetter(Poco::Net::StreamSocket & socket_, const Poco::Timespan & send_timeout_, const Poco::Timespan & recieve_timeout_,
                  bool limit_max_timeout = false)
        : socket(socket_), send_timeout(send_timeout_), recieve_timeout(recieve_timeout_)
    {
        old_send_timeout = socket.getSendTimeout();
        old_receive_timeout = socket.getReceiveTimeout();

        if (!limit_max_timeout || old_send_timeout > send_timeout)
            socket.setSendTimeout(send_timeout);

        if (!limit_max_timeout || old_receive_timeout > recieve_timeout)
            socket.setReceiveTimeout(recieve_timeout);
    }

    TimeoutSetter(Poco::Net::StreamSocket & socket_, const Poco::Timespan & timeout_, bool limit_max_timeout = false)
        : TimeoutSetter(socket_, timeout_, timeout_, limit_max_timeout) {}

    ~TimeoutSetter()
    {
        socket.setSendTimeout(old_send_timeout);
        socket.setReceiveTimeout(old_receive_timeout);
    }

    Poco::Net::StreamSocket & socket;

    Poco::Timespan send_timeout;
    Poco::Timespan recieve_timeout;

    Poco::Timespan old_send_timeout;
    Poco::Timespan old_receive_timeout;
};


}
