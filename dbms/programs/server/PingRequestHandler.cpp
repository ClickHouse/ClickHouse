#include "PingRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

void PingRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest &,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        setResponseDefaultHeaders(response, config.getUInt("keep_alive_timeout", 10));

        const char * data = "Ok.\n";
        response.sendBuffer(data, strlen(data));
    }
    catch (...)
    {
        tryLogCurrentException("PingRequestHandler");
    }
}

}
