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
    auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", 10);

    response.setContentType("text/html; charset=UTF-8");

    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    setResponseDefaultHeaders(response, keep_alive_timeout);
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
    *response.send() << getResource(resource_name);
}

}
