#include <gtest/gtest.h>

#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/NetException.h>


namespace
{

/// A listening socket that never accepts. The kernel's listen backlog
/// completes the TCP handshake, so a client can connect and write small
/// amounts of data (absorbed by the kernel send buffer) without a server
/// thread.
class PassiveListener
{
public:
    PassiveListener() : server_socket(Poco::Net::SocketAddress("127.0.0.1", 0), 1) {}

    Poco::Net::StreamSocket connect()
    {
        Poco::Net::StreamSocket sock;
        sock.connect(Poco::Net::SocketAddress("127.0.0.1", server_socket.address().port()));
        return sock;
    }

private:
    Poco::Net::ServerSocket server_socket;
};

}


/// Writing exactly Content-Length bytes should succeed.
TEST(HTTPFixedLengthStreamBuf, WriteExactLength)
{
    PassiveListener listener;
    Poco::Net::HTTPClientSession session(listener.connect());

    Poco::Net::HTTPFixedLengthOutputStream stream(session, 10);

    stream.write("0123456789", 10);
    stream.flush();

    ASSERT_TRUE(stream.good()) << "Stream should be good after writing exactly Content-Length bytes";
}


/// Writing more than Content-Length should throw MessageException.
TEST(HTTPFixedLengthStreamBuf, WriteOverLengthThrows)
{
    PassiveListener listener;
    Poco::Net::HTTPClientSession session(listener.connect());

    /// Content-Length is 5, but we will try to write 10 bytes.
    Poco::Net::HTTPFixedLengthOutputStream stream(session, 5);

    /// The data goes into the 8KB buffer first. On flush, flushBuffer calls
    /// writeToDevice which clamps to Content-Length (writes 5 bytes), then the
    /// loop calls writeToDevice again with the remaining 5 bytes, which throws
    /// MessageException because _count >= _length.
    stream.write("0123456789", 10);

    bool got_exception = false;
    try
    {
        stream.flush();
    }
    catch (const Poco::Net::MessageException &)
    {
        got_exception = true;
    }

    ASSERT_TRUE(got_exception) << "Expected MessageException when writing past Content-Length";
}


/// Writing exactly Content-Length and then one more byte should throw.
TEST(HTTPFixedLengthStreamBuf, WriteBoundaryPlusOneThrows)
{
    PassiveListener listener;
    Poco::Net::HTTPClientSession session(listener.connect());

    Poco::Net::HTTPFixedLengthOutputStream stream(session, 5);

    /// Write exactly 5 + 1 bytes.
    stream.write("012345", 6);

    bool got_exception = false;
    try
    {
        stream.flush();
    }
    catch (const Poco::Net::MessageException &)
    {
        got_exception = true;
    }

    ASSERT_TRUE(got_exception) << "Expected MessageException when writing Content-Length + 1";
}
