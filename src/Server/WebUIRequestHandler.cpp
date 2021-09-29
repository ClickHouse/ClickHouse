#include "WebUIRequestHandler.h"
#include "IServer.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <IO/HTTPCommon.h>
#include <common/getResource.h>


namespace DB
{

WebUIRequestHandler::WebUIRequestHandler(IServer & server_, std::string resource_name_)
    : server(server_), resource_name(std::move(resource_name_))
{
}


void WebUIRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    auto & config = server.config();
    auto keep_alive_timeout = config.getUInt("keep_alive_timeout", 10);

    if (request.isSecure())
    {
        size_t hsts_max_age = server.config().getUInt64("hsts_max_age", 0);

        if (hsts_max_age > 0)
            response.add("Strict-Transport-Security", "max-age=" + std::to_string(hsts_max_age));
    }

    response.setContentType("text/html; charset=UTF-8");

    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response, keep_alive_timeout);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    *response.send() << getResource(resource_name);
}

}
