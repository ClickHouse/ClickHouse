#include <gtest/gtest.h>

#include <Server/HTTP/HTTPContext.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Common/NetException.h>

#include <Poco/Exception.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerSession.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/StreamSocketImpl.h>

#include <sys/socket.h>

#include <cerrno>
#include <memory>
#include <optional>
#include <string>

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

namespace
{

struct TestHTTPContext : DB::IHTTPContext
{
    uint64_t getMaxHstsAge() const override { return 0; }
    uint64_t getMaxUriSize() const override { return 1 << 20; }
    uint64_t getMaxFields() const override { return 100; }
    uint64_t getMaxFieldNameSize() const override { return 1 << 10; }
    uint64_t getMaxFieldValueSize() const override { return 1 << 10; }
    uint64_t getMaxRequestHeaderSize() const override { return 1 << 20; }
    /// Shorter than the body timeouts, so the header read changes the socket timeouts.
    Poco::Timespan getHeadersReadTimeout() const override { return Poco::Timespan(5, 0); }
    Poco::Timespan getReceiveTimeout() const override { return Poco::Timespan(60, 0); }
    Poco::Timespan getSendTimeout() const override { return Poco::Timespan(45, 0); }
};

/// With `reset_on_read`, behaves like a macOS socket whose peer has reset the connection:
/// the read fails, and every later `setsockopt` fails with EINVAL.
class ResetByPeerSocketImpl final : public Poco::Net::StreamSocketImpl
{
public:
    ResetByPeerSocketImpl(poco_socket_t fd, bool reset_on_read_) : StreamSocketImpl(fd), reset_on_read(reset_on_read_) {}

    int receiveBytes(void * buffer, int length, int flags) override
    {
        if (!reset_on_read)
            return StreamSocketImpl::receiveBytes(buffer, length, flags);
        reset = true;
        throw Poco::Net::ConnectionResetException(POCO_ECONNRESET);
    }

    void setReceiveTimeout(const Poco::Timespan & timeout) override
    {
        if (reset)
            throw Poco::InvalidArgumentException("Invalid argument", POCO_EINVAL);
        StreamSocketImpl::setReceiveTimeout(timeout);
    }

    void setSendTimeout(const Poco::Timespan & timeout) override
    {
        if (reset)
            throw Poco::InvalidArgumentException("Invalid argument", POCO_EINVAL);
        StreamSocketImpl::setSendTimeout(timeout);
    }

private:
    const bool reset_on_read;
    bool reset = false;
};

/// A loopback connection whose server side is a `ResetByPeerSocketImpl`.
struct LoopbackConnection
{
    Poco::Net::ServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0)};
    Poco::Net::StreamSocket client;
    std::optional<Poco::Net::StreamSocket> server;

    explicit LoopbackConnection(bool reset_on_read)
    {
        client.connect(listener.address());
        const poco_socket_t fd = ::accept(listener.impl()->sockfd(), nullptr, nullptr);
        if (fd < 0)
            throw Poco::Net::NetException("accept failed", errno);
        server.emplace(new ResetByPeerSocketImpl(fd, reset_on_read));
    }
};

}

TEST(HTTPServerRequest, PeerResetWhileReadingHeadersDoesNotTerminate)
{
    LoopbackConnection connection(/* reset_on_read */ true);
    Poco::Net::HTTPServerSession session(*connection.server, new Poco::Net::HTTPServerParams);
    DB::HTTPServerResponse response(session);
    try
    {
        DB::HTTPServerRequest request(std::make_shared<TestHTTPContext>(), response, session);
        FAIL() << "the read error did not reach the caller";
    }
    catch (const DB::NetException & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NETWORK_ERROR);
    }
}

TEST(HTTPServerRequest, RestoresBodyTimeoutsAfterReadingHeaders)
{
    LoopbackConnection connection(/* reset_on_read */ false);
    const std::string request_text = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
    connection.client.sendBytes(request_text.data(), static_cast<int>(request_text.size()));

    Poco::Net::HTTPServerSession session(*connection.server, new Poco::Net::HTTPServerParams);
    DB::HTTPServerResponse response(session);
    auto context = std::make_shared<TestHTTPContext>();
    DB::HTTPServerRequest request(context, response, session);

    EXPECT_EQ(request.getMethod(), "GET");
    EXPECT_EQ(connection.server->getReceiveTimeout(), context->getReceiveTimeout());
    EXPECT_EQ(connection.server->getSendTimeout(), context->getSendTimeout());
}
