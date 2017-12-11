#include "RootRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

void RootRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest &,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        setResponseDefaultHeaders(response, config.getUInt("keep_alive_timeout", 10));

        response.setContentType("text/html; charset=UTF-8");

        const std::string data = config.getString("http_server_default_response", "Ok.\n");
        response.sendBuffer(data.data(), data.size());
    }
    catch (...)
    {
        tryLogCurrentException("RootRequestHandler");
    }
}

}
