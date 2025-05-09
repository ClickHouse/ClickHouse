#include <memory>
#include <Server/HTTP/HTTP1/HTTP1ServerConnection.h>
#include <Server/HTTP/HTTP1/HTTP1ServerResponse.h>
#include <Server/HTTP/HTTP1/ReadHeaders.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/TCPServer.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/HTTPChunkedReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>

#include <Common/logger_useful.h>
#include "IO/ReadBuffer.h"
#include "Server/HTTP/HTTPRequest.h"

#include <Poco/Net/NetException.h>

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

    HTTPRequest res(method, uri, version);

    // since HTTP always use Windows-style EOL '\r\n' we always can safely skip to '\n'

    skipToNextLineOrEOF(in);

    readHeaders(res, in, max_fields_number, max_field_name_size, max_field_value_size);

    skipToNextLineOrEOF(in);

    return res;
}

HTTPServerRequest buildRequest(HTTPContextPtr context, Poco::Net::HTTPServerSession & session, const ProfileEvents::Event & read_event)
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

    std::unique_ptr<ReadBuffer> body;
    /// If a client crashes, most systems will gracefully terminate the connection with FIN just like it's done on close().
    /// So we will get 0 from recv(...) and will not be able to understand that something went wrong (well, we probably
    /// will get RST later on attempt to write to the socket that closed on the other side, but it will happen when the query is finished).
    /// If we are extremely unlucky and data format is TSV, for example, then we may stop parsing exactly between rows
    /// and decide that it's EOF (but it is not). It may break deduplication, because clients cannot control it
    /// and retry with exactly the same (incomplete) set of rows.
    /// That's why we have to check body size if it's provided.
    if (request.getChunkedTransferEncoding())
        body = std::make_unique<HTTPChunkedReadBuffer>(std::move(in), HTTP_MAX_CHUNK_SIZE);
    else if (request.hasContentLength())
    {
        size_t content_length = request.getContentLength();
        body = std::make_unique<LimitReadBuffer>(std::move(in), LimitReadBuffer::Settings{.read_no_less = content_length, .read_no_more = content_length, .expect_eof = true});
    }
    else if (request.getMethod() != HTTPRequest::HTTP_GET && request.getMethod() != HTTPRequest::HTTP_HEAD && request.getMethod() != HTTPRequest::HTTP_DELETE)
    {
        body = std::move(in);
        if (!startsWith(request.getContentType(), "multipart/form-data"))
            LOG_WARNING(LogFrequencyLimiter(getLogger("HTTPServerRequest"), 10), "Got an HTTP request with no content length "
                "and no chunked/multipart encoding, it may be impossible to distinguish graceful EOF from abnormal connection loss");
    }
    else
        /// We have to distinguish empty buffer and nullptr.
        body = std::make_unique<EmptyReadBuffer>();

    return HTTPServerRequest(std::move(request), std::move(body), client_address, server_address, secure, session.socket().impl());
}

void sendErrorResponse(HTTP1ServerResponse & response, Poco::Net::HTTPResponse::HTTPStatus status)
{
    response.setVersion(Poco::Net::HTTPMessage::HTTP_1_1);
    response.setStatusAndReason(status);
    response.setKeepAlive(false);
    response.getSession().setKeepAlive(false);
    response.makeStream()->finalize();
}

}

HTTP1ServerConnection::HTTP1ServerConnection(
    HTTPContextPtr context_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket,
    Poco::Net::HTTPServerParams::Ptr params_,
    HTTPRequestHandlerFactoryPtr factory_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : TCPServerConnection(socket), context(std::move(context_)), tcp_server(tcp_server_), params(params_), factory(factory_), read_event(read_event_), write_event(write_event_), stopped(false)
{
    poco_check_ptr(factory);
}

void HTTP1ServerConnection::run()
{
    /// FIXME: server seems to be always empty
    std::string server = params->getSoftwareVersion();
    Poco::Net::HTTPServerSession session(socket(), params);

    /// FIXME: stopped seems to be useless
    while (!stopped && tcp_server.isOpen() && session.hasMoreRequests() && session.connected())
    {
        try
        {
            std::lock_guard lock(mutex);
            if (!stopped && tcp_server.isOpen() && session.connected())
            {
                HTTPServerRequest request = buildRequest(context, session, read_event);
                HTTP1ServerResponse response(session, write_event);
                response.attachRequest(&request);

                Poco::Timestamp now;

                if (!forwarded_for.empty())
                    request.set("X-Forwarded-For", forwarded_for);

                if (request.isSecure())
                {
                    size_t hsts_max_age = context->getMaxHstsAge();

                    if (hsts_max_age > 0)
                        response.add("Strict-Transport-Security", "max-age=" + std::to_string(hsts_max_age));

                }

                /// FIXME: move all these to some function to reuse in HTTP2ServerConnection
                response.setDate(now);
                response.setVersion(request.getVersion());
                response.setKeepAlive(params->getKeepAlive() && request.getKeepAlive() && session.canKeepAlive());
                if (!server.empty())
                    response.set("Server", server);
                try
                {
                    if (!tcp_server.isOpen())
                    {
                        sendErrorResponse(response, Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
                        break;
                    }
                    std::unique_ptr<HTTPRequestHandler> handler(factory->createRequestHandler(request));

                    if (handler)
                    {
                        if (request.getExpectContinue() && response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
                            response.send100Continue();

                        handler->handleRequest(request, response, write_event);
                        session.setKeepAlive(params->getKeepAlive() && response.getKeepAlive() && session.canKeepAlive());
                    }
                    else
                        sendErrorResponse(response, Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED);
                }
                catch (Poco::Exception &)
                {
                    if (!response.sendStarted())
                    {
                        try
                        {
                            sendErrorResponse(response, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                        }
                        catch (...) // NOLINT(bugprone-empty-catch)
                        {
                        }
                    }
                    throw;
                }
            }
        }
        catch (const Poco::Net::NoMessageException &)
        {
            break;
        }
        catch (const Poco::Net::MessageException & e)
        {
            LOG_DEBUG(LogFrequencyLimiter(getLogger("HTTP1ServerConnection"), 10), "HTTP request failed: {}: {}", HTTPResponse::HTTP_REASON_BAD_REQUEST, e.displayText());
            // MessageException should be thrown only on request parsing error so it is safe to create a new response object here
            HTTP1ServerResponse response(session, write_event);
            sendErrorResponse(response, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        }
        catch (const Poco::Net::NetException & e)
        {
            /// Do not spam logs with messages related to connection reset by peer.
            if (e.code() == POCO_ENOTCONN)
            {
                LOG_DEBUG(LogFrequencyLimiter(getLogger("HTTP1ServerConnection"), 10), "Connection reset by peer while processing HTTP request: {}", e.message());
                break;
            }

            if (session.networkException())
                session.networkException()->rethrow();
            else
                throw;
        }
        catch (const Poco::Exception &)
        {
            if (session.networkException())
            {
                session.networkException()->rethrow();
            }
            else
                throw;
        }
    }
}

}
