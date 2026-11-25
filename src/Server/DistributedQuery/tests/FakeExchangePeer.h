#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <functional>
#include <thread>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <IO/WriteBufferFromString.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
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

/// Reads the source's `SourceHello` and answers with a `SinkHello` of the same protocol version.
inline void completeSinkHandshake(Poco::Net::StreamSocket & socket)
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

    WriteBufferFromOwnString reply_body;
    SinkHelloBody{.sink_version = PROTOCOL_VERSION}.write(reply_body);
    reply_body.finalize();
    PacketHeader reply_header{.packet_type = PacketType::SinkHello, .bytes_size = reply_body.str().size()};
    sendAll(socket, reinterpret_cast<const char *>(&reply_header), sizeof(reply_header), "SinkHello header");
    sendAll(socket, reply_body.str().data(), reply_body.str().size(), "SinkHello body");
}

}

#endif
