#include <gtest/gtest.h>

#include <Poco/Net/HTTPChunkedStream.h>
#include <Poco/Net/HTTPClientSession.h>


TEST(HTTPChunkedStreamBuf, IsCompleteHandlesInvalidSocketException)
{
    Poco::Net::HTTPClientSession session;
    Poco::Net::HTTPChunkedStreamBuf buf(session, std::ios::in);

    /// Default-initialized socket throws InvalidSocketException in SocketImpl::receiveBytes,
    /// which HTTPChunkedStreamBuf::isComplete should swallow and return false.
    bool complete = buf.isComplete(true);
    ASSERT_FALSE(complete);
}
