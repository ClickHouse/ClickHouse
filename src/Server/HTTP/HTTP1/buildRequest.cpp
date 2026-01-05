#include <Server/HTTP/HTTP1/buildRequest.h>
#include <Server/HTTP/HTTP1/ReadHeaders.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/HTTPChunkedReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>

#include <Poco/Net/NetException.h>

#include <memory>

namespace DB
{

namespace
{

constexpr UInt64 HTTP_MAX_CHUNK_SIZE = 100ULL << 30;

/// Limits for basic sanity checks when reading a header
enum Limits
{
    MAX_METHOD_LENGTH = 32,
    MAX_VERSION_LENGTH = 8,
};

HTTPRequest readRequestHeader(ReadBuffer & in, size_t max_uri_size, size_t max_fields_number, size_t max_field_name_size, size_t max_field_value_size);

}

HTTPServerRequest buildRequest(
    HTTPContextPtr context,
    Poco::Net::HTTPServerSession & session,
    const ProfileEvents::Event & read_event)
{
    /// Now that we know socket is still connected, obtain addresses
    Poco::Net::SocketAddress client_address = session.clientAddress();
    Poco::Net::SocketAddress server_address = session.serverAddress();
    const bool secure = session.socket().secure();

    auto receive_timeout = context->getReceiveTimeout();
    auto send_timeout = context->getSendTimeout();

    session.socket().setReceiveTimeout(receive_timeout);
    session.socket().setSendTimeout(send_timeout);

    auto in = std::make_unique<ReadBufferFromPocoSocket>(session.socket(), read_event);

    HTTPRequest request = readRequestHeader(*in, context->getMaxUriSize(),
        context->getMaxFields(), context->getMaxFieldNameSize(), context->getMaxFieldValueSize());  /// Try parse according to RFC7230

    ReadBufferPtr stream;
    bool stream_is_bounded;
    /// If a client crashes, most systems will gracefully terminate the connection with FIN just like it's done on close().
    /// So we will get 0 from recv(...) and will not be able to understand that something went wrong (well, we probably
    /// will get RST later on attempt to write to the socket that closed on the other side, but it will happen when the query is finished).
    /// If we are extremely unlucky and data format is TSV, for example, then we may stop parsing exactly between rows
    /// and decide that it's EOF (but it is not). It may break deduplication, because clients cannot control it
    /// and retry with exactly the same (incomplete) set of rows.
    /// That's why we have to check body size if it's provided.
    if (request.getChunkedTransferEncoding())
    {
        stream = std::make_shared<HTTPChunkedReadBuffer>(std::move(in), HTTP_MAX_CHUNK_SIZE);
        stream_is_bounded = true;
    }
    else if (request.hasContentLength())
    {
        size_t content_length = request.getContentLength();
        stream = std::make_shared<LimitReadBuffer>(std::move(in), LimitReadBuffer::Settings{.read_no_less = content_length, .read_no_more = content_length, .expect_eof = true});
        stream_is_bounded = true;
    }
    else if (request.getMethod() != HTTPRequest::HTTP_GET && request.getMethod() != HTTPRequest::HTTP_HEAD && request.getMethod() != HTTPRequest::HTTP_DELETE)
    {
        /// FIXME: the body here must also be empty according to the RFC https://www.ietf.org/rfc/rfc9112.html#name-message-body-length
        stream = std::move(in);
        stream_is_bounded = false;
        if (!startsWith(request.getContentType(), "multipart/form-data"))
            LOG_WARNING(LogFrequencyLimiter(getLogger("HTTPServerRequest"), 10), "Got an HTTP request with no content length "
                "and no chunked/multipart encoding, it may be impossible to distinguish graceful EOF from abnormal connection loss");
    }
    else
    {
        /// We have to distinguish empty buffer and nullptr.
        stream = std::make_shared<EmptyReadBuffer>();
        stream_is_bounded = true;
    }

    return HTTPServerRequest(std::move(request), std::move(stream), stream_is_bounded, client_address, server_address, secure, session.socket().impl());
}

namespace
{

HTTPRequest readRequestHeader(ReadBuffer & in, size_t max_uri_size, size_t max_fields_number, size_t max_field_name_size, size_t max_field_value_size)
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
        throw Poco::Net::MessageException(fmt::format("Invalid HTTP version string: {}", version));

    // since HTTP always use Windows-style EOL '\r\n' we always can safely skip to '\n'

    skipToNextLineOrEOF(in);

    HTTPRequest res(method, uri, version);
    readHeaders(res, in, max_fields_number, max_field_name_size, max_field_value_size);

    skipToNextLineOrEOF(in);

    return res;
}

}

}
