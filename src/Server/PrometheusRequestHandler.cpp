#include "PrometheusRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

#include <IO/WriteBufferFromHTTPServerResponse.h>


namespace DB
{

void PrometheusRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        unsigned keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

        setResponseDefaultHeaders(response, keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        auto wb = WriteBufferFromHTTPServerResponse(request, response, keep_alive_timeout);
        metrics_writer.write(wb);
        wb.finalize();
    }
    catch (...)
    {
        tryLogCurrentException("PrometheusRequestHandler");
    }
}

}
