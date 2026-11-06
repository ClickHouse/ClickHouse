#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <chrono>
#include <functional>
#include <thread>

#include <poll.h>
#if defined(OS_LINUX)
#include <linux/sockios.h>
#include <sys/ioctl.h>
#else
#include <sys/socket.h>
#endif

#include <gtest/gtest.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <IO/WriteBufferFromString.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <base/errnoToString.h>
#include <base/types.h>

namespace DB::ExchangeTest
{

/// The sink's end of an exchange connection, driven by a thread: `behaviour` runs on the socket the
/// source connected to. Tests use it to show a real `StreamingExchangeSource` a peer that misbehaves.
class FakePeer
{
public:
    explicit FakePeer(std::function<void(Poco::Net::StreamSocket &)> behaviour_)
        : listener(Poco::Net::SocketAddress("127.0.0.1", 0))
        , thread([this, behaviour = std::move(behaviour_)]
        {
            accepted = listener.acceptConnection();
            behaviour(accepted);
        })
    {
    }

    ~FakePeer()
    {
        join();
        accepted.close();
        listener.close();
    }

    void join()
    {
        if (thread.joinable())
            thread.join();
    }

    UInt16 port() const { return listener.address().port(); }

private:
    Poco::Net::ServerSocket listener;
    Poco::Net::StreamSocket accepted;
    std::thread thread;
};

/// Reads the source's `SourceHello`.
inline void receiveSourceHello(Poco::Net::StreamSocket & socket)
{
    using namespace StreamingExchangeProtocol;
    PacketHeader header{};
    size_t position = 0;
    while (position < sizeof(header))
        position += socket.receiveBytes(reinterpret_cast<char *>(&header) + position, static_cast<int>(sizeof(header) - position));
    std::string body(header.bytes_size, '\0');
    position = 0;
    while (position < body.size())
        position += socket.receiveBytes(body.data() + position, static_cast<int>(body.size() - position));
}

/// Reads the source's `SourceHello` and answers with a `SinkHello` of the same protocol version.
inline void completeSinkHandshake(Poco::Net::StreamSocket & socket)
{
    using namespace StreamingExchangeProtocol;
    receiveSourceHello(socket);

    WriteBufferFromOwnString reply_body;
    SinkHelloBody{.sink_version = PROTOCOL_VERSION}.write(reply_body);
    reply_body.finalize();
    PacketHeader reply_header{.packet_type = PacketType::SinkHello, .bytes_size = reply_body.str().size()};
    sendAll(socket, reinterpret_cast<const char *>(&reply_header), sizeof(reply_header), "SinkHello header");
    sendAll(socket, reply_body.str().data(), reply_body.str().size(), "SinkHello body");
}

/// Bytes sent on `socket` that the other end has not acknowledged yet.
inline int unacknowledgedBytes(const Poco::Net::StreamSocket & socket)
{
    int bytes = 0;
#if defined(OS_LINUX)
    const int result = ::ioctl(socket.sockfd(), SIOCOUTQ, &bytes);
#else
    socklen_t size = sizeof(bytes);
    const int result = ::getsockopt(socket.sockfd(), SOL_SOCKET, SO_NWRITE, &bytes, &size);
#endif
    if (result != 0)
    {
        ADD_FAILURE() << "Cannot read the unacknowledged byte count: " << errnoToString();
        return 0;
    }
    return bytes;
}

/// Resets the connection once the source acknowledged everything sent to it: a reset discards the
/// bytes the kernel has not sent yet, which could cut the `SinkHello` short.
inline void resetAfterDelivery(Poco::Net::StreamSocket & socket)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    pollfd descriptor{.fd = socket.sockfd(), .events = 0, .revents = 0};
    /// Each poll waits 1 ms; an error or a hang-up means the source is gone and acknowledges nothing more.
    while (unacknowledgedBytes(socket) > 0 && ::poll(&descriptor, 1, 1) <= 0)
    {
        if (std::chrono::steady_clock::now() > deadline)
        {
            ADD_FAILURE() << "The source did not acknowledge the bytes of the peer within 5 s";
            break;
        }
    }
    socket.setLinger(true, 0);
    socket.close();
}

}

#endif
