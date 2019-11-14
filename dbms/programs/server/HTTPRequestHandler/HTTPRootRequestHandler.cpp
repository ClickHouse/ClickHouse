#include "HTTPRootRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

void HTTPRootRequestHandler::handleRequest(Poco::Net::HTTPServerRequest &, Poco::Net::HTTPServerResponse & response)
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
        tryLogCurrentException("HTTPRootRequestHandler");
    }
}

HTTPHandlerMatcher createRootHandlerMatcher(IServer &, const String &)
{
    return [&](const Poco::Net::HTTPServerRequest & request) -> bool
    {
        return (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
            && request.getURI() == "/";
    };
}

HTTPHandlerCreator createRootHandlerCreator(IServer & server, const String &)
{
    return [&]() { return new HTTPRootRequestHandler(server); };
}

}
