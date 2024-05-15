#include <Server/PrometheusBaseRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
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
    try
    {
        write_event = write_event_;
        http_method = request.getMethod();
        chassert(!out);

        /// Make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response, config->keep_alive_timeout);

        handleMetrics(request, response);

        if (out)
        {
            out->finalize();
            out = nullptr;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
        out = nullptr;
    }
}

WriteBuffer & PrometheusBaseRequestHandler::getOutputStream(HTTPServerResponse & response)
{
    if (out)
        return *out;
    out = std::make_unique<WriteBufferFromHTTPServerResponse>(
        response, http_method == HTTPRequest::HTTP_HEAD, config->keep_alive_timeout, write_event);
    return *out;
}

}
