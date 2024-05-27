#include <Server/PrometheusBaseRequestHandler.h>

#include <Common/setThreadName.h>
#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/PrometheusRequestHandlerConfig.h>


namespace DB
{

PrometheusBaseRequestHandler::PrometheusBaseRequestHandler(IServer & server_, const PrometheusRequestHandlerConfigPtr & config_)
    : server(server_), config(config_), log(getLogger("PrometheusRequestHandler"))
{
}

PrometheusBaseRequestHandler::~PrometheusBaseRequestHandler() = default;

void PrometheusBaseRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_)
{
    setThreadName("PrometheusHndlr");

    try
    {
        write_event = write_event_;
        http_method = request.getMethod();
        chassert(!out);

        /// Make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response, config->keep_alive_timeout);

        const auto & path = request.getURI();

        if (config->metrics && (!config->detect_handler_by_endpoint || (path == config->metrics->endpoint)))
            handleMetrics(request, response);
        else if (config->remote_write && (!config->detect_handler_by_endpoint || (path == config->remote_write->endpoint)))
            handleRemoteWrite(request, response);
        else
            handlerNotFound(request, response);

        if (out)
        {
            out->finalize();
            out = nullptr;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);

        ExecutionStatus status = ExecutionStatus::fromCurrentException("", send_stacktrace);
        trySendExceptionToClient(status.message, status.code, request, response);
        tryCallOnException();

        /// `out` must be finalized already or at least tried to finalize.
        out = nullptr;
    }
}

void PrometheusBaseRequestHandler::handlerNotFound(HTTPServerRequest & request, HTTPServerResponse & response)
{
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    writeString("There is no handler " + request.getURI() + "\n", getOutputStream(response));
}

WriteBuffer & PrometheusBaseRequestHandler::getOutputStream(HTTPServerResponse & response)
{
    if (out)
        return *out;
    out = std::make_unique<WriteBufferFromHTTPServerResponse>(
        response, http_method == HTTPRequest::HTTP_HEAD, config->keep_alive_timeout, write_event);
    return *out;
}

void PrometheusBaseRequestHandler::trySendExceptionToClient(const String & exception_message, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        sendExceptionToHTTPClient(exception_message, exception_code, request, response, out.get(), log);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Couldn't send exception to client");

        if (out)
        {
            try
            {
                out->finalize();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Cannot flush data to client (after sending exception)");
            }
        }
    }
}

void PrometheusBaseRequestHandler::tryCallOnException()
{
    try
    {
        onException();
    }
    catch (...)
    {
        tryLogCurrentException(log, "onException");
    }
}

}
