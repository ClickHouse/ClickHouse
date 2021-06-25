#include "HandlerFactory.h"
#include "PingHandler.h"
#include "ColumnInfoHandler.h"
#include <Poco/URI.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <common/logger_useful.h>

namespace DB
{
Poco::Net::HTTPRequestHandler * HandlerFactory::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    Poco::URI uri{request.getURI()};
    LOG_TRACE(log, "Request URI: {}", uri.toString());

    if (uri.getPath() == "/ping" && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        return new PingHandler(keep_alive_timeout);

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {

        if (uri.getPath() == "/columns_info")
#if USE_ODBC
            return new ODBCColumnsInfoHandler(keep_alive_timeout, context);
#else
            return nullptr;
#endif
        else if (uri.getPath() == "/identifier_quote")
#if USE_ODBC
            return new IdentifierQuoteHandler(keep_alive_timeout, context);
#else
            return nullptr;
#endif
        else if (uri.getPath() == "/schema_allowed")
#if USE_ODBC
            return new SchemaAllowedHandler(keep_alive_timeout, context);
#else
            return nullptr;
#endif
        else if (uri.getPath() == "/write")
            return new ODBCHandler(pool_map, keep_alive_timeout, context, "write");
        else
            return new ODBCHandler(pool_map, keep_alive_timeout, context, "read");
    }
    return nullptr;
}
}
