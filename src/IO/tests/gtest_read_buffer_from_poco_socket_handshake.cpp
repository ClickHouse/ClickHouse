#include <gtest/gtest.h>

#include <IO/ReadBufferFromPocoSocket.h>

#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <sys/socket.h>

namespace
{

Poco::Timespan::TimeDiff getKernelTimeoutMicroseconds(const Poco::Net::Socket & socket, int option)
{
    Poco::Timespan timeout;
    socket.getOption(SOL_SOCKET, option, timeout);
    return timeout.totalMicroseconds();
}

}

/// Like `PostgreSQLHandler`: the accepted socket has only the timeouts it inherited from the listener,
/// which Poco does not know about. Ending the handshake must put those back, not zero (no timeout).
TEST(ReadBufferFromPocoSocket, HandshakeTimeoutKeepsTimeoutsInheritedFromListener)
{
    const Poco::Timespan receive_timeout(7, 0);
    const Poco::Timespan send_timeout(9, 0);

    Poco::Net::ServerSocket listener(Poco::Net::SocketAddress("127.0.0.1", 0));
    listener.setReceiveTimeout(receive_timeout);
    listener.setSendTimeout(send_timeout);

    Poco::Net::StreamSocket client;
    client.connect(listener.address());
    Poco::Net::StreamSocket server = listener.acceptConnection();
    ASSERT_EQ(getKernelTimeoutMicroseconds(server, SO_RCVTIMEO), receive_timeout.totalMicroseconds());
    ASSERT_EQ(getKernelTimeoutMicroseconds(server, SO_SNDTIMEO), send_timeout.totalMicroseconds());

    DB::ReadBufferFromPocoSocket in(server);
    in.setHandshakeTimeout(1000);
    in.clearHandshakeTimeout();

    EXPECT_EQ(getKernelTimeoutMicroseconds(server, SO_RCVTIMEO), receive_timeout.totalMicroseconds());
    EXPECT_EQ(getKernelTimeoutMicroseconds(server, SO_SNDTIMEO), send_timeout.totalMicroseconds());
    /// Poco bounds its own wait before each write by this cached value.
    EXPECT_EQ(server.getSendTimeout().totalMicroseconds(), send_timeout.totalMicroseconds());
}
