#include "RootRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

void RootRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        setResponseDefaultHeaders(response);
        response.setContentType("text/html; charset=UTF-8");
        const std::string data = server.config().getString("http_server_default_response", "Ok.\n");
        response.sendBuffer(data.data(), data.size());
    }
    catch (...)
    {
        tryLogCurrentException("RootRequestHandler");
    }
}

}
