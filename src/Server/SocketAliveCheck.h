#pragma once

#include <IO/SocketPeerClosed.h>

#include <Poco/Net/StreamSocket.h>

#include <functional>

namespace DB
{

/// Check whether a Poco stream-socket peer is still connected. This is TLS-aware, so a TLS
/// `close_notify` is not mistaken for unread application data.
/// The lambda captures `socket_` by reference; the caller must ensure the socket outlives it.
inline std::function<bool()> makeSocketAliveCheckCallback(Poco::Net::StreamSocket & socket_)
{
    return [&socket_]() -> bool
    {
        try
        {
            return !isSocketPeerClosed(socket_);
        }
        catch (...) // Ok: any non-timeout exception means the peer is gone
        {
            return false;
        }
    };
}

}
