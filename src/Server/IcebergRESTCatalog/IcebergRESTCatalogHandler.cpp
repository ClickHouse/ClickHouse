#include <Server/IcebergRESTCatalog/IcebergRESTCatalogHandler.h>

#include <Access/Credentials.h>
#include <IO/HTTPCommon.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/HTTPHandler.h>
#include <Server/IServer.h>

#include <base/unit.h>

namespace DB
{

namespace
{

constexpr size_t MAX_REQUEST_BODY_SIZE = 1_MiB;

}

IcebergRESTCatalogHandler::IcebergRESTCatalogHandler(IServer & server_, IcebergRESTCatalogServicePtr service_)
    : log(getLogger("IcebergRESTCatalogHandler"))
    , server(server_)
    , service(std::move(service_))
{
}

ContextMutablePtr IcebergRESTCatalogHandler::authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response, Session & session) const
{
    /// Only the URI is parsed here, so the body stays unread until the caller consumes it.
    HTMLForm params(server.context()->getSettingsRef(), request);
    /// No fixed user: every request must carry its own credentials.
    const HTTPHandlerConnectionConfig connection_config;
    /// Each request gets a fresh handler, so partial multi-step credentials do not survive a 401 anyway.
    std::unique_ptr<Credentials> request_credentials;
    if (!authenticateUserByHTTP(request, params, response, session, request_credentials, connection_config, server.context(), log))
    {
        /// Unauthenticated clients get no body read. The 401 is already sent, so close the connection instead.
        response.setKeepAlive(false);
        return nullptr;
    }

    auto context = session.makeQueryContext();
    context->setCurrentQueryId("");
    return context;
}

std::optional<String> IcebergRESTCatalogHandler::readRequestBody(HTTPServerRequest & request, HTTPServerResponse & response, size_t max_size)
{
    String body;
    /// Read one byte past the limit, otherwise an oversized body is silently truncated.
    LimitReadBuffer limited_stream(*request.getStream(), LimitReadBuffer::Settings{.read_no_more = max_size + 1});
    readStringUntilEOF(body, limited_stream);

    if (body.size() > max_size)
    {
        /// The rest of the body stays unread, so the connection cannot be reused for the next request.
        response.setKeepAlive(false);
        sendResponse(
            response,
            IcebergRESTCatalogService::makeError(
                Poco::Net::HTTPResponse::HTTP_REQUEST_ENTITY_TOO_LARGE,
                "RequestEntityTooLargeException",
                fmt::format("Request body must not exceed {} bytes", max_size)));
        return {};
    }

    return body;
}

void IcebergRESTCatalogHandler::sendResponse(HTTPServerResponse & response, const IcebergRESTResponse & result)
{
    setResponseDefaultHeaders(response);

    if (result.body.empty())
    {
        response.setStatusAndReason(result.status);
        response.send();
        return;
    }

    response.setStatus(result.status);
    response.setContentType("application/json");
    /// The declared length marks the end of the body, so the connection can stay open for the next request.
    response.setContentLength(result.body.size());
    *response.send() << result.body;
}

void IcebergRESTCatalogHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    try
    {
        Session session(server.context(), ClientInfo::Interface::ICEBERG_REST_CATALOG, request.isSecure());
        auto context = authenticateUser(request, response, session);
        if (!context)
            return; /// 401 with `WWW-Authenticate` is already sent.

        /// Consume the body here, before anything is sent.
        /// Without `Content-Length` or chunked encoding the request has no body, and the raw socket must not be read.
        String body;
        if (request.getChunkedTransferEncoding() || request.hasContentLength())
        {
            auto read = readRequestBody(request, response, MAX_REQUEST_BODY_SIZE);
            if (!read)
                return;
            body = std::move(*read);
        }

        sendResponse(response, service->execute(request.getMethod(), request.getURI(), body, context.get()));
    }
    catch (...)
    {
        const auto error = service->makeErrorFromCurrentException();
        try
        {
            if (!response.sent())
            {
                /// Consume an unread POST body, so the connection can be reused for the next request.
                drainRequestIfNeeded(request, response);
                sendResponse(response, error);
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot send exception to client");
        }
    }
}

}
