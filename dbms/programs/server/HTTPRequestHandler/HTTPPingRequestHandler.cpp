#include "HTTPPingRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

void HTTPPingRequestHandler::handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response)
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
        tryLogCurrentException("HTTPPingRequestHandler");
    }
}

HTTPHandlerMatcher createPingHandlerMatcher(IServer & server, const String & key)
{
    const auto & path = server.config().getString(key, "/ping");

    return [&, path = path](const Poco::Net::HTTPServerRequest & request)
    {
        return (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD) &&
               request.getURI() == path;
    };
}

HTTPHandlerCreator createPingHandlerCreator(IServer & server, const String &)
{
    return [&]() { return new HTTPPingRequestHandler(server); };
}

}
