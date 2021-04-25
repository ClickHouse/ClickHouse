#include <Server/HTTP/HTTPServerResponse.h>

#include <Server/HTTP/HTTPServerRequest.h>

#include <Poco/CountingStream.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/File.h>
#include <Poco/FileStream.h>
#include <Poco/Net/HTTPChunkedStream.h>
#include <Poco/Net/HTTPFixedLengthStream.h>
#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/StreamCopier.h>

namespace DB
{

HTTPServerResponse::HTTPServerResponse(Poco::Net::HTTPServerSession & session_) : session(session_)
{
}

void HTTPServerResponse::sendContinue()
{
    Poco::Net::HTTPHeaderOutputStream hs(session);
    hs << getVersion() << " 100 Continue\r\n\r\n";
}

std::shared_ptr<std::ostream> HTTPServerResponse::send()
{
    poco_assert(!stream);

    if ((request && request->getMethod() == HTTPRequest::HTTP_HEAD) || getStatus() < 200 || getStatus() == HTTPResponse::HTTP_NO_CONTENT
        || getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
    {
        Poco::CountingOutputStream cs;
        write(cs);
        stream = std::make_shared<Poco::Net::HTTPFixedLengthOutputStream>(session, cs.chars());
        write(*stream);
    }
    else if (getChunkedTransferEncoding())
    {
        Poco::Net::HTTPHeaderOutputStream hs(session);
        write(hs);
        stream = std::make_shared<Poco::Net::HTTPChunkedOutputStream>(session);
    }
    else if (hasContentLength())
    {
        Poco::CountingOutputStream cs;
        write(cs);
        stream = std::make_shared<Poco::Net::HTTPFixedLengthOutputStream>(session, getContentLength64() + cs.chars());
        write(*stream);
    }
    else
    {
        stream = std::make_shared<Poco::Net::HTTPOutputStream>(session);
        setKeepAlive(false);
        write(*stream);
    }

    return stream;
}

std::pair<std::shared_ptr<std::ostream>, std::shared_ptr<std::ostream>> HTTPServerResponse::beginSend()
{
    poco_assert(!stream);
    poco_assert(!header_stream);

    /// NOTE: Code is not exception safe.

    if ((request && request->getMethod() == HTTPRequest::HTTP_HEAD) || getStatus() < 200 || getStatus() == HTTPResponse::HTTP_NO_CONTENT
        || getStatus() == HTTPResponse::HTTP_NOT_MODIFIED)
    {
        throw Poco::Exception("HTTPServerResponse::beginSend is invalid for HEAD request");
    }
    else if (getChunkedTransferEncoding())
    {
        header_stream = std::make_shared<Poco::Net::HTTPHeaderOutputStream>(session);
        beginWrite(*header_stream);
        stream = std::make_shared<Poco::Net::HTTPChunkedOutputStream>(session);
    }
    else if (hasContentLength())
    {
        throw Poco::Exception("HTTPServerResponse::beginSend is invalid for response with Content-Length header");
    }
    else
    {
        stream = std::make_shared<Poco::Net::HTTPOutputStream>(session);
        header_stream = stream;
        setKeepAlive(false);
        beginWrite(*stream);
    }

    return std::make_pair(header_stream, stream);
}

void HTTPServerResponse::sendBuffer(const void * buffer, std::size_t length)
{
    poco_assert(!stream);

    setContentLength(static_cast<int>(length));
    setChunkedTransferEncoding(false);

    stream = std::make_shared<Poco::Net::HTTPHeaderOutputStream>(session);
    write(*stream);
    if (request && request->getMethod() != HTTPRequest::HTTP_HEAD)
    {
        stream->write(static_cast<const char *>(buffer), static_cast<std::streamsize>(length));
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

}
