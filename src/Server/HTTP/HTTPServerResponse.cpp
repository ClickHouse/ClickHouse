#include <Server/HTTP/HTTPServerResponse.h>

#include <IO/AutoFinalizedWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/Net/HTTPChunkedStream.h>
#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPStream.h>
#include <IO/NullWriteBuffer.h>


namespace DB
{

HTTPServerResponse::HTTPServerResponse(Poco::Net::HTTPServerSession & session_, const ProfileEvents::Event & write_event_)
    : session(session_)
    , write_event(write_event_)
{
}

void HTTPServerResponse::sendContinue()
{
    Poco::Net::HTTPHeaderOutputStream hs(session);
    hs << getVersion() << " 100 Continue\r\n\r\n";
}

std::shared_ptr<WriteBuffer> HTTPServerResponse::send()
{
    poco_assert(!stream);

    if (request && request->getMethod() == HTTPRequest::HTTP_HEAD)
    {
        // HTTP_HEAD is a special case
        // client usually reads nothing from socket after headers even when 'Contex-Lenght' is sent
        // if server wrote a message to the connection with enabled 'Connection: Keep-Alive'
        // the connection would be poisoned.
        // Next request over that connection reads previously unreaded message as a HTTP status line

        // make sure that nothing is sent to the client if it was HTTP_HEAD request
        stream = std::make_shared<NullWriteBuffer>(write_event);

    }
    else if (getStatus() < 200 || getStatus() == HTTPResponse::HTTP_NOT_MODIFIED || getStatus() == HTTPResponse::HTTP_NO_CONTENT)
    {
        // I really do not know why do we consider this cases as special one
        // but if we do, then it is safer to close the connection at the end
        setKeepAlive(false);

        stream = std::make_shared<AutoFinalizedWriteBuffer<WriteBufferFromPocoSocket>>(session.socket(), write_event);
    }
    else if (getChunkedTransferEncoding())
    {
        stream = std::make_shared<AutoFinalizedWriteBuffer<HTTPWriteBufferChunked>>(session.socket(), write_event);
    }
    else if (hasContentLength())
    {
        stream = std::make_shared<AutoFinalizedWriteBuffer<HTTPWriteBufferFixedLength>>(session.socket(), getContentLength(), write_event);
    }
    else
    {
        setKeepAlive(false);

        stream = std::make_shared<AutoFinalizedWriteBuffer<WriteBufferFromPocoSocket>>(session.socket(), write_event);
    }

    Poco::Net::HTTPHeaderOutputStream hs(session);
    writeStatusAndHeaders(hs);

    return stream;
}

/// Only this method is called inside WriteBufferFromHTTPServerResponse
void HTTPServerResponse::writeStatus(std::ostream & ostr)
{
    ostr << getVersion() << " " << static_cast<int>(getStatus()) << " " << getReason() << "\r\n";
    ostr.flush();
}

/// Only this method is called inside WriteBufferFromHTTPServerResponse
void HTTPServerResponse::writeHeaders(std::ostream & ostr)
{
    allowKeepAliveIFFRequestIsFullyRead();

    Poco::Net::HTTPMessage::write(ostr); // NOLINT (bugprone-parent-virtual-call)
    ostr << "\r\n";
    ostr.flush();

    send_started = true;
}

void HTTPServerResponse::writeStatusAndHeaders(std::ostream & ostr)
{
    writeStatus(ostr);
    writeHeaders(ostr);
}

void HTTPServerResponse::sendBuffer(const void * buffer, std::size_t length)
{
    setContentLength(static_cast<int>(length));
    setChunkedTransferEncoding(false);

    // Send header
    Poco::Net::HTTPHeaderOutputStream hs(session);
    writeStatusAndHeaders(hs);

    if (request && request->getMethod() != HTTPRequest::HTTP_HEAD)
    {
        auto wb = WriteBufferFromPocoSocket(session.socket(), write_event);
        wb.write(static_cast<const char *>(buffer), length);
        wb.finalize();
    }
}

void HTTPServerResponse::requireAuthentication(const std::string & realm)
{
    poco_assert(!stream);

    setStatusAndReason(HTTPResponse::HTTP_UNAUTHORIZED);
    std::string auth("Basic realm=\"");
    auth.append(realm);
    auth.append("\"");
    set("WWW-Authenticate", auth);
}

void HTTPServerResponse::redirect(const std::string & uri, HTTPStatus status)
{
    poco_assert(!stream);

    setContentLength(0);
    setChunkedTransferEncoding(false);

    setStatusAndReason(status);
    set("Location", uri);

    // Send header
    Poco::Net::HTTPHeaderOutputStream hs(session);
    writeStatusAndHeaders(hs);
}

void HTTPServerResponse::allowKeepAliveIFFRequestIsFullyRead()
{
    /// Connection can only be reused if we've fully read the previous request and all its POST data.
    /// Otherwise we'd misinterpret the leftover data as part of the next request's header.
    /// HTTPServerRequest::canKeepAlive() checks that request stream is bounded and is fully read.
    if (!request || !request->canKeepAlive())
        setKeepAlive(false);
}
}
