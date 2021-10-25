#include <Server/HTTP/HTTPServerRequest.h>

#include <Interpreters/Context.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/HTTPChunkedReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/ReadHeaders.h>

#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/NetException.h>

namespace DB
{
HTTPServerRequest::HTTPServerRequest(ContextPtr context, HTTPServerResponse & response, Poco::Net::HTTPServerSession & session)
    : max_uri_size(context->getSettingsRef().http_max_uri_size)
    , max_fields_number(context->getSettingsRef().http_max_fields)
    , max_field_name_size(context->getSettingsRef().http_max_field_name_size)
    , max_field_value_size(context->getSettingsRef().http_max_field_value_size)
{
    response.attachRequest(this);

    /// Now that we know socket is still connected, obtain addresses
    client_address = session.clientAddress();
    server_address = session.serverAddress();

    auto receive_timeout = context->getSettingsRef().http_receive_timeout;
    auto send_timeout = context->getSettingsRef().http_send_timeout;

    session.socket().setReceiveTimeout(receive_timeout);
    session.socket().setSendTimeout(send_timeout);

    auto in = std::make_unique<ReadBufferFromPocoSocket>(session.socket());
    socket = session.socket().impl();

    readRequest(*in);  /// Try parse according to RFC7230

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

bool HTTPServerRequest::checkPeerConnected() const
{
    try
    {
        char b;
        if (!socket->receiveBytes(&b, 1, MSG_DONTWAIT | MSG_PEEK))
            return false;
    }
    catch (Poco::TimeoutException &)
    {
    }
    catch (...)
    {
        return false;
    }

    return true;
}

void HTTPServerRequest::readRequest(ReadBuffer & in)
{
    char ch;
    std::string method;
    std::string uri;
    std::string version;

    method.reserve(16);
    uri.reserve(64);
    version.reserve(16);

    if (in.eof())
        throw Poco::Net::NoMessageException();

    skipWhitespaceIfAny(in);

    if (in.eof())
        throw Poco::Net::MessageException("No HTTP request header");

    while (in.read(ch) && !Poco::Ascii::isSpace(ch) && method.size() <= MAX_METHOD_LENGTH)
        method += ch;

    if (method.size() > MAX_METHOD_LENGTH)
        throw Poco::Net::MessageException("HTTP request method invalid or too long");

    skipWhitespaceIfAny(in);

    while (in.read(ch) && !Poco::Ascii::isSpace(ch) && uri.size() <= max_uri_size)
        uri += ch;

    if (uri.size() > max_uri_size)
        throw Poco::Net::MessageException("HTTP request URI invalid or too long");

    skipWhitespaceIfAny(in);

    while (in.read(ch) && !Poco::Ascii::isSpace(ch) && version.size() <= MAX_VERSION_LENGTH)
        version += ch;

    if (version.size() > MAX_VERSION_LENGTH)
        throw Poco::Net::MessageException("Invalid HTTP version string");

    // since HTTP always use Windows-style EOL '\r\n' we always can safely skip to '\n'

    skipToNextLineOrEOF(in);

    readHeaders(*this, in, max_fields_number, max_field_name_size, max_field_value_size);

    skipToNextLineOrEOF(in);

    setMethod(method);
    setURI(uri);
    setVersion(version);
}

}
