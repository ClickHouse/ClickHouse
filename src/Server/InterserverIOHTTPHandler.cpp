#include "InterserverIOHTTPHandler.h"

#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <common/logger_useful.h>
#include <Common/HTMLForm.h>
#include <Common/setThreadName.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <Interpreters/InterserverIOHandler.h>
#include "IServer.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
}

std::pair<String, bool> InterserverIOHTTPHandler::checkAuthentication(Poco::Net::HTTPServerRequest & request) const
{
    const auto & config = server.config();

    if (config.has("interserver_http_credentials.user"))
    {
        if (!request.hasCredentials())
            return {"Server requires HTTP Basic authentication, but client doesn't provide it", false};
        String scheme, info;
        request.getCredentials(scheme, info);

        if (scheme != "Basic")
            return {"Server requires HTTP Basic authentication but client provides another method", false};

        String user = config.getString("interserver_http_credentials.user");
        String password = config.getString("interserver_http_credentials.password", "");

        Poco::Net::HTTPBasicCredentials credentials(info);
        if (std::make_pair(user, password) != std::make_pair(credentials.getUsername(), credentials.getPassword()))
            return {"Incorrect user or password in HTTP Basic authentication", false};
    }
    else if (request.hasCredentials())
    {
        return {"Client requires HTTP Basic authentication, but server doesn't provide it", false};
    }
    return {"", true};
}

void InterserverIOHTTPHandler::processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, Output & used_output)
{
    HTMLForm params(request);

    LOG_TRACE(log, "Request URI: {}", request.getURI());

    String endpoint_name = params.get("endpoint");
    bool compress = params.get("compress") == "true";

    ReadBufferFromIStream body(request.stream());

    auto endpoint = server.context().getInterserverIOHandler().getEndpoint(endpoint_name);
    /// Locked for read while query processing
    std::shared_lock lock(endpoint->rwlock);
    if (endpoint->blocker.isCancelled())
        throw Exception("Transferring part to replica was cancelled", ErrorCodes::ABORTED);

    if (compress)
    {
        CompressedWriteBuffer compressed_out(*used_output.out);
        endpoint->processQuery(params, body, compressed_out, response);
    }
    else
    {
        endpoint->processQuery(params, body, *used_output.out, response);
    }
}


void InterserverIOHTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    setThreadName("IntersrvHandler");

    /// In order to work keep-alive.
    if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    Output used_output;
    const auto & config = server.config();
    unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);
    used_output.out = std::make_shared<WriteBufferFromHTTPServerResponse>(request, response, keep_alive_timeout);

    try
    {
        if (auto [message, success] = checkAuthentication(request); success)
        {
            processQuery(request, response, used_output);
            LOG_DEBUG(log, "Done processing query");
        }
        else
        {
            response.setStatusAndReason(Poco::Net::HTTPServerResponse::HTTP_UNAUTHORIZED);
            if (!response.sent())
                writeString(message, *used_output.out);
            LOG_WARNING(log, "Query processing failed request: '{}' authentication failed", request.getURI());
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES)
            return;

        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        /// Sending to remote server was cancelled due to server shutdown or drop table.
        bool is_real_error = e.code() != ErrorCodes::ABORTED;

        std::string message = getCurrentExceptionMessage(is_real_error);
        if (!response.sent())
            writeString(message, *used_output.out);

        if (is_real_error)
            LOG_ERROR(log, message);
        else
            LOG_INFO(log, message);
    }
    catch (...)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        std::string message = getCurrentExceptionMessage(false);
        if (!response.sent())
            writeString(message, *used_output.out);

        LOG_ERROR(log, message);
    }
}


}
