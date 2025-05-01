#include "PingHandler.h"
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Common/Exception.h>
#include <IO/HTTPCommon.h>

namespace DB
{
void PingHandler::handleRequest(HTTPServerRequest & /* request */, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    try
    {
        setResponseDefaultHeaders(response);
        const char * data = "Ok.\n";
        response.sendBuffer(data, strlen(data));
    }
    catch (...)
    {
        tryLogCurrentException("PingHandler");
    }
}
}
