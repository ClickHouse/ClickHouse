#include "TimeoutSetter.h"

#include <common/logger_useful.h>

namespace DB
{
TimeoutSetter::TimeoutSetter(Poco::Net::StreamSocket & socket_,
    const Poco::Timespan & send_timeout_,
    const Poco::Timespan & receive_timeout_,
    bool limit_max_timeout)
    : socket(socket_), send_timeout(send_timeout_), receive_timeout(receive_timeout_)
{
    old_send_timeout = socket.getSendTimeout();
    old_receive_timeout = socket.getReceiveTimeout();

    if (!limit_max_timeout || old_send_timeout > send_timeout)
        socket.setSendTimeout(send_timeout);

    if (!limit_max_timeout || old_receive_timeout > receive_timeout)
        socket.setReceiveTimeout(receive_timeout);
}

TimeoutSetter::TimeoutSetter(Poco::Net::StreamSocket & socket_, const Poco::Timespan & timeout_, bool limit_max_timeout)
    : TimeoutSetter(socket_, timeout_, timeout_, limit_max_timeout)
{
}

TimeoutSetter::~TimeoutSetter()
{
    try
    {
        socket.setSendTimeout(old_send_timeout);
        socket.setReceiveTimeout(old_receive_timeout);
    }
    catch (std::exception & e)
    {
        // Sometimes caught on macos
        LOG_ERROR(&Poco::Logger::get("Client"), "TimeoutSetter: Can't reset timeouts: {}", e.what());
    }
}
}
