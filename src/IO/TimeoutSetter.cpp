#include <IO/TimeoutSetter.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

TimeoutSetter::TimeoutSetter(Poco::Net::StreamSocket & socket_,
    Poco::Timespan send_timeout_,
    Poco::Timespan receive_timeout_,
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

TimeoutSetter::TimeoutSetter(Poco::Net::StreamSocket & socket_, Poco::Timespan timeout_, bool limit_max_timeout)
    : TimeoutSetter(socket_, timeout_, timeout_, limit_max_timeout)
{
}

TimeoutSetter::~TimeoutSetter()
{
    if (was_reset)
        return;

    try
    {
        reset();
    }
    catch (...)
    {
        /// It's known that setting timeouts for a socket often does not work on Apple macOS.
        /// Let's not confuse the users of Apple macOS with extraneous error messages.
#if !defined(OS_DARWIN)
        tryLogCurrentException("Client", "TimeoutSetter: Can't reset timeouts");
#endif
    }
}

void TimeoutSetter::reset()
{
    bool connected = socket.impl()->initialized();
    if (!connected)
        return;

    socket.setSendTimeout(old_send_timeout);
    socket.setReceiveTimeout(old_receive_timeout);
    was_reset = true;
}

}
