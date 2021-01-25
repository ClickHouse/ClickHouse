#include <Server/HTTP/HTTPServerRequest.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/HTTPChunkedReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Server/HTTP/HTTPServerResponse.h>

#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/Net/HTTPStream.h>

namespace DB
{

HTTPServerRequest::HTTPServerRequest(HTTPServerResponse & response, Poco::Net::HTTPServerSession & session)
{
    response.attachRequest(this);

    /// FIXME: potentially we can read headers with our ReadBuffer too.
    Poco::Net::HTTPHeaderInputStream hs(session);
    read(hs);

    /// Now that we know socket is still connected, obtain addresses
    client_address = session.clientAddress();
    server_address = session.serverAddress();

    auto in = std::make_unique<ReadBufferFromPocoSocket>(session.socket());

    if (getChunkedTransferEncoding())
        stream = std::make_unique<HTTPChunkedReadBuffer>(std::move(in));
    else if (hasContentLength())
        stream = std::make_unique<LimitReadBuffer>(std::move(in), getContentLength(), false);
    else if (getMethod() != HTTPRequest::HTTP_GET && getMethod() != HTTPRequest::HTTP_HEAD && getMethod() != HTTPRequest::HTTP_DELETE)
        stream = std::move(in);
    else
        /// We have to distinguish empty buffer and nullptr.
        stream = std::make_unique<EmptyReadBuffer>();
}

}
