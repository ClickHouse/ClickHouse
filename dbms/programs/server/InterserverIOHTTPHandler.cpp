#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <common/logger_useful.h>

#include <Common/HTMLForm.h>
#include <Common/setThreadName.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <Interpreters/InterserverIOHandler.h>

#include "InterserverIOHTTPHandler.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
}

std::pair<String, bool> InterserverIOHTTPHandler::checkAuthentication(Poco::Net::HTTPServerRequest & request) const
{
    const auto & config = server.config();

    if (config.has("interserver_http_credentials.user"))
    {
        if (!request.hasCredentials())
            return {"Server requires HTTP Basic authentification, but client doesn't provide it", false};
        String scheme, info;
        request.getCredentials(scheme, info);

        if (scheme != "Basic")
            return {"Server requires HTTP Basic authentification but client provides another method", false};

        String user = config.getString("interserver_http_credentials.user");
        String password = config.getString("interserver_http_credentials.password", "");

        Poco::Net::HTTPBasicCredentials credentials(info);
        if (std::make_pair(user, password) != std::make_pair(credentials.getUsername(), credentials.getPassword()))
            return {"Incorrect user or password in HTTP Basic authentification", false};
    }
    else if (request.hasCredentials())
    {
        return {"Client requires HTTP Basic authentification, but server doesn't provide it", false};
    }
    return {"", true};
}

void InterserverIOHTTPHandler::processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    HTMLForm params(request);

    LOG_TRACE(log, "Request URI: " << request.getURI());

    String endpoint_name = params.get("endpoint");
    bool compress = params.get("compress") == "true";

    ReadBufferFromIStream body(request.stream());

    const auto & config = server.config();
    unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

    WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);

    auto endpoint = server.context().getInterserverIOHandler().getEndpoint(endpoint_name);

    if (compress)
    {
        CompressedWriteBuffer compressed_out(out);
        endpoint->processQuery(params, body, compressed_out, response);
    }
    else
    {
        endpoint->processQuery(params, body, out, response);
    }

    out.finalize();
}


void InterserverIOHTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    setThreadName("IntersrvHandler");

    /// In order to work keep-alive.
    if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    try
    {
        if (auto [msg, success] = checkAuthentication(request); success)
        {
            processQuery(request, response);
            LOG_INFO(log, "Done processing query");
        }
        else
        {
            response.setStatusAndReason(Poco::Net::HTTPServerResponse::HTTP_UNAUTHORIZED);
            if (!response.sent())
                response.send() << msg << std::endl;
            LOG_WARNING(log, "Query processing failed request: '" << request.getURI() << "' authentification failed");
        }
    }
    catch (Exception & e)
    {

        if (e.code() == ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES)
        {
            if (!response.sent())
                response.send();
            return;
        }

        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        /// Sending to remote server was cancelled due to server shutdown or drop table.
        bool is_real_error = e.code() != ErrorCodes::ABORTED;

        std::string message = getCurrentExceptionMessage(is_real_error);
        if (!response.sent())
            response.send() << message << std::endl;

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
            response.send() << message << std::endl;
        LOG_ERROR(log, message);
    }
}


}
