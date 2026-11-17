#include <Server/InterserverIOHTTPHandler.h>

#include <Server/IServer.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Core/ServerSettings.h>
#include <IO/ReadBufferFromIStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <shared_mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int REQUIRED_PASSWORD;
}

std::pair<String, bool> InterserverIOHTTPHandler::checkAuthentication(HTTPServerRequest & request) const
{
    auto server_credentials = server.context()->getInterserverCredentials();
    if (server_credentials)
    {
        if (!request.hasCredentials())
            return server_credentials->isValidUser("", "");

        String scheme;
        String info;
        request.getCredentials(scheme, info);

        if (scheme != "Basic")
            return {"Server requires HTTP Basic authentication but client provides another method", false};

        Poco::Net::HTTPBasicCredentials credentials(info);
        return server_credentials->isValidUser(credentials.getUsername(), credentials.getPassword());
    }
    if (request.hasCredentials())
    {
        return {"Client requires HTTP Basic authentication, but server doesn't provide it", false};
    }

    return {"", true};
}

void InterserverIOHTTPHandler::processQuery(HTTPServerRequest & request, HTTPServerResponse & response, OutputPtr output)
{
    HTMLForm params(server.context()->getSettingsRef(), request);

    LOG_TRACE(log, "Request URI: {}", request.getURI());

    String endpoint_name = params.get("endpoint");
    bool compress = params.get("compress") == "true";

    auto endpoint = server.context()->getInterserverIOHandler().getEndpoint(endpoint_name);
    /// Locked for read while query processing
    std::shared_lock lock(endpoint->rwlock);
    if (endpoint->blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Transferring part to replica was cancelled");

    if (compress)
    {
        CompressedWriteBuffer compressed_out(*output);
        endpoint->processQuery(params, request.getStream(), compressed_out, response);
        compressed_out.finalize();
    }
    else
    {
        endpoint->processQuery(params, request.getStream(), *output, response);
    }
    /// Make sure that request stream is not used after this function.
    assert(request.getStream().use_count() == 2);
}


void InterserverIOHTTPHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event)
{
    DB::setThreadName(ThreadName::INTERSERVER_HANDLER);

    /// In order to work keep-alive.
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    auto output = std::make_shared<WriteBufferFromHTTPServerResponse>(
        response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, write_event);

    try
    {
        auto [message, success] = checkAuthentication(request);
        if (success)
        {
            processQuery(request, response, output);
            output->finalize();
            LOG_DEBUG(log, "Done processing query");
        }
        else
        {
            LOG_WARNING(log, "Query processing failed request: '{}' authentication failed", request.getURI());
            output->cancelWithException(request, ErrorCodes::REQUIRED_PASSWORD, message, nullptr);
        }
    }
    catch (Exception & e)
    {
        /// Sending to remote server was cancelled due to server shutdown or drop table.
        bool is_real_error = e.code() != ErrorCodes::ABORTED;
        PreformattedMessage message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ is_real_error);
        if (is_real_error)
            LOG_ERROR(log, message);
        else
            LOG_INFO(log, message);

        output->cancelWithException(request, getCurrentExceptionCode(), message.text, nullptr);
    }
    catch (...)
    {
        PreformattedMessage message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false);
        LOG_ERROR(log, message);

        output->cancelWithException(request, getCurrentExceptionCode(), message.text, nullptr);
    }
}


}
