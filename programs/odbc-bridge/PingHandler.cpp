#include "PingHandler.h"
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Common/Exception.h>
#include <IO/HTTPCommon.h>

namespace DB
{
void PingHandler::handleRequest(Poco::Net::HTTPServerRequest & /*request*/, Poco::Net::HTTPServerResponse & response)
{
    try
    {
        setResponseDefaultHeaders(response, keep_alive_timeout);
        const char * data = "Ok.\n";
        response.sendBuffer(data, strlen(data));
    }
    catch (...)
    {
        tryLogCurrentException("PingHandler");
    }
}
}
