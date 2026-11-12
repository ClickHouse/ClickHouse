#include <gtest/gtest.h>

#include <IO/HTTP/HTTPClientIO.h>
#include <IO/WriteHelpers.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPResponseParser.h>
#include <Poco/Net/HTTPSession.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Timespan.h>

#include <functional>
#include <memory>
#include <string>


namespace
{

/// A server that answers a request as soon as it has read the request header, before the body.
/// The client and the server run on one thread: the answer is in the receive buffer of the client
/// before the client writes the body, so the client sees it while it sends the body.
class HTTPEarlyResponseTest : public ::testing::Test
{
protected:
    static constexpr size_t body_size = 1 << 20;

    Poco::Net::ServerSocket listener{Poco::Net::SocketAddress("127.0.0.1", 0)};
    Poco::Net::HTTPClientSession session{"127.0.0.1", listener.address().port()};
    Poco::Net::StreamSocket peer;

    void SetUp() override
    {
        session.setTimeout(Poco::Timespan(5, 0), Poco::Timespan(5, 0), Poco::Timespan(2, 0));
    }

    /// Sends the request header, and answers it with `answer` before the body.
    std::unique_ptr<DB::HTTPRequestBodyWriteBuffer> sendAndAnswer(const std::string & answer, const Poco::Net::HTTPHeaderLimits & limits = {})
    {
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", Poco::Net::HTTPMessage::HTTP_1_1);
        request.setContentLength(body_size);
        auto body = DB::sendHTTPRequest(session, request, DB::DBMS_DEFAULT_BUFFER_SIZE, nullptr, nullptr, limits);

        peer = listener.acceptConnection();
        std::string header;
        char ch = 0;
        while (!header.ends_with("\r\n\r\n") && peer.receiveBytes(&ch, 1) == 1)
            header += ch;

        peer.sendBytes(answer.data(), static_cast<int>(answer.size()));
        EXPECT_TRUE(session.socket().poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ));
        return body;
    }
};

/// Runs `answer` once, in the first send that starts after `armed` is set. The data hooks and the
/// send throttler wait in the same place, so this is where a response can arrive after the socket
/// was found writable.
struct AnswerBeforeSend : public Poco::Net::IHTTPSessionDataHooks
{
    explicit AnswerBeforeSend(std::function<void()> answer_)
        : answer(std::move(answer_))
    {
    }

    void atStart(int /*bytes*/) override
    {
        if (armed && !answered)
        {
            answered = true;
            answer();
        }
    }
    void atFinish(int /*bytes*/) override {}
    void atFail() override {}

    bool armed = false;
    bool answered = false;
    std::function<void()> answer;
};

}


/// An error that arrives before the body stops it: nothing of the body is sent, the error is
/// received, and the request stays incomplete, so the connection is not reused.
TEST_F(HTTPEarlyResponseTest, ErrorStopsTheBody)
{
    auto body = sendAndAnswer("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
    DB::writeString(std::string(body_size, 'x'), *body);
    body->finalize();

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    EXPECT_FALSE(session.isRequestBodyComplete());
    EXPECT_FALSE(peer.poll(Poco::Timespan(0, 100'000), Poco::Net::Socket::SELECT_READ));
}


/// A field longer than the read buffer of the session, but within its limit, is accepted also when
/// it arrives while the body is sent.
TEST_F(HTTPEarlyResponseTest, LongFieldWithinItsLimit)
{
    const std::string value(9000, 'v');
    Poco::Net::HTTPHeaderLimits limits;
    limits.value_length = 16384;
    auto body = sendAndAnswer("HTTP/1.1 403 Forbidden\r\nX-Long: " + value + "\r\nContent-Length: 0\r\n\r\n", limits);
    DB::writeString(std::string(body_size, 'x'), *body);
    body->finalize();

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
    EXPECT_EQ(response.get("X-Long"), value);
}

/// The field limit holds before the header ends: a peer that keeps sending fields gets an error at
/// the first field over the limit, instead of making the client collect them all.
TEST_F(HTTPEarlyResponseTest, FieldLimitHoldsBeforeTheHeaderEnds)
{
    std::string answer = "HTTP/1.1 404 Not Found\r\n";
    for (size_t i = 0; i < 200; ++i)
        answer += "X-" + std::to_string(i) + ": v\r\n";
    auto body = sendAndAnswer(answer);

    try
    {
        DB::writeString(std::string(body_size, 'x'), *body);
        body->finalize();
        Poco::Net::HTTPResponse response;
        DB::receiveHTTPResponse(session, response);
        FAIL() << "A response header with too many fields was accepted";
    }
    catch (const Poco::Net::MessageException & e)
    {
        EXPECT_EQ(e.message(), "Too many header fields");
    }
}

/// The server answers and resets the connection while the send waits in the data hooks. The send
/// must not go out: it would fail on the reset connection instead of reporting the answer.
TEST_F(HTTPEarlyResponseTest, AnswerWhileTheSendWaits)
{
    auto hooks = std::make_shared<AnswerBeforeSend>([this]
    {
        const std::string answer = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        peer.sendBytes(answer.data(), static_cast<int>(answer.size()));
        peer.setLinger(true, 0);
        peer.close();
        EXPECT_TRUE(session.socket().poll(Poco::Timespan(5, 0), Poco::Net::Socket::SELECT_READ));
    });
    session.setSendDataHooks(hooks);

    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", Poco::Net::HTTPMessage::HTTP_1_1);
    request.setContentLength(body_size);
    auto body = DB::sendHTTPRequest(session, request);

    peer = listener.acceptConnection();
    std::string header;
    char ch = 0;
    while (!header.ends_with("\r\n\r\n") && peer.receiveBytes(&ch, 1) == 1)
        header += ch;
    hooks->armed = true;

    DB::writeString(std::string(body_size, 'x'), *body);
    body->finalize();

    Poco::Net::HTTPResponse response;
    auto response_body = DB::receiveHTTPResponse(session, response);
    EXPECT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
}
