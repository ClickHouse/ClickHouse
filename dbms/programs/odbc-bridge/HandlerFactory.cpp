#include "HandlerFactory.h"
#include <Common/HTMLForm.h>

#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <common/logger_useful.h>

namespace DB
{
Poco::Net::HTTPRequestHandler * HandlerFactory::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    const auto & uri = request.getURI();
    LOG_TRACE(log, "Request URI: " + uri);

    if (uri == "/ping" && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        return new PingHandler(keep_alive_timeout);

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        return new ODBCHandler(pool_map, keep_alive_timeout, context);

    return nullptr;
}
}
