#pragma once

#include <Poco/Timespan.h>
#include <Poco/Net/StreamSocket.h>


namespace DB
{

/// Temporarily overrides socket send/recieve timeouts and reset them back into destructor
/// Timeouts could be only decreased
struct TimeoutSetter
{
    TimeoutSetter(Poco::Net::StreamSocket & socket_, const Poco::Timespan & send_timeout_, const Poco::Timespan & recieve_timeout_)
        : socket(socket_), send_timeout(send_timeout_), recieve_timeout(recieve_timeout_)
    {
        old_send_timeout = socket.getSendTimeout();
        old_receive_timeout = socket.getReceiveTimeout();

        if (old_send_timeout > send_timeout)
            socket.setSendTimeout(send_timeout);

        if (old_receive_timeout > recieve_timeout)
            socket.setReceiveTimeout(recieve_timeout);
    }

    TimeoutSetter(Poco::Net::StreamSocket & socket_, const Poco::Timespan & timeout_)
        : TimeoutSetter(socket_, timeout_, timeout_) {}

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
