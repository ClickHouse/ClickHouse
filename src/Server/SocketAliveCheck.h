#pragma once

#include <Poco/Exception.h>
#include <Poco/Net/Socket.h>

#include <functional>

namespace DB
{

/// Check if a Poco::Net::Socket peer is still connected via MSG_DONTWAIT | MSG_PEEK.
/// The lambda captures `socket_` by reference; the caller must ensure the socket outlives it.
inline std::function<bool()> makeSocketAliveCheckCallback(Poco::Net::Socket & socket_)
{
    return [&socket_]() -> bool
    {
        try
        {
            char b = 0;
            if (!socket_.impl()->receiveBytes(&b, 1, MSG_DONTWAIT | MSG_PEEK))
                return false;
        }
        catch (Poco::TimeoutException &) // NOLINT(bugprone-empty-catch)
        {
            /// EAGAIN / EWOULDBLOCK — no data available but connection is alive.
        }
        catch (...) // Ok: any non-timeout exception means the peer is gone
        {
            return false;
        }
        return true;
    };
}

}
