#include <Server/HTTP/HTTPServerRequest.h>

#include <Interpreters/Context.h>
#include <IO/EmptyReadBuffer.h>
#include <IO/HTTPChunkedReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <Server/HTTP/HTTPServerResponse.h>

#include <Poco/Net/HTTPHeaderStream.h>
#include <Poco/Net/HTTPStream.h>
#include <Poco/Net/NetException.h>

namespace DB
{

HTTPServerRequest::HTTPServerRequest(const Context & context, HTTPServerResponse & response, Poco::Net::HTTPServerSession & session)
{
    response.attachRequest(this);

    /// Now that we know socket is still connected, obtain addresses
    client_address = session.clientAddress();
    server_address = session.serverAddress();

    auto receive_timeout = context.getSettingsRef().http_receive_timeout;
    auto send_timeout = context.getSettingsRef().http_send_timeout;

    session.socket().setReceiveTimeout(receive_timeout);
    session.socket().setSendTimeout(send_timeout);

    auto in = std::make_unique<ReadBufferFromPocoSocket>(session.socket());

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

    while (in.read(ch) && !Poco::Ascii::isSpace(ch) && uri.size() <= MAX_URI_LENGTH)
        uri += ch;

    if (uri.size() > MAX_URI_LENGTH)
        throw Poco::Net::MessageException("HTTP request URI invalid or too long");

    skipWhitespaceIfAny(in);

    while (in.read(ch) && !Poco::Ascii::isSpace(ch) && version.size() <= MAX_VERSION_LENGTH)
        version += ch;

    if (version.size() > MAX_VERSION_LENGTH)
        throw Poco::Net::MessageException("Invalid HTTP version string");

    // since HTTP always use Windows-style EOL '\r\n' we always can safely skip to '\n'

    skipToNextLineOrEOF(in);

    readHeaders(in);

    skipToNextLineOrEOF(in);

    setMethod(method);
    setURI(uri);
    setVersion(version);
}

void HTTPServerRequest::readHeaders(ReadBuffer & in)
{
    char ch;
    std::string name;
    std::string value;

    name.reserve(32);
    value.reserve(64);

    int fields = 0;

    while (true)
    {
        if (fields > MAX_FIELDS_NUMBER)
            throw Poco::Net::MessageException("Too many header fields");

        name.clear();
        value.clear();

        /// Field name
        while (in.read(ch) && ch != ':' && !Poco::Ascii::isSpace(ch) && name.size() <= MAX_NAME_LENGTH)
            name += ch;

        if (in.eof())
            throw Poco::Net::MessageException("Field is invalid");

        if (name.empty())
        {
            if (ch == '\r')
                /// Start of the empty-line delimiter
                break;
            if (ch == ':')
                throw Poco::Net::MessageException("Field name is empty");
        }
        else
        {
            if (name.size() > MAX_NAME_LENGTH)
                throw Poco::Net::MessageException("Field name is too long");
            if (ch != ':')
                throw Poco::Net::MessageException("Field name is invalid or no colon found");
        }

        skipWhitespaceIfAny(in, true);

        if (in.eof())
            throw Poco::Net::MessageException("Field is invalid");

        /// Field value - folded values not supported.
        while(in.read(ch) && ch != '\r' && ch != '\n' && value.size() <= MAX_VALUE_LENGTH)
            value += ch;

        if (in.eof())
            throw Poco::Net::MessageException("Field is invalid");

        if (value.empty())
            throw Poco::Net::MessageException("Field value is empty");

        if (ch == '\n')
            throw Poco::Net::MessageException("No CRLF found");

        if (value.size() > MAX_VALUE_LENGTH)
            throw Poco::Net::MessageException("Field value is too long");

        skipToNextLineOrEOF(in);

        Poco::trimRightInPlace(value);
        add(name, decodeWord(value));
        ++fields;
    }
}

}
