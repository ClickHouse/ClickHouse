#include "ODBCHandlerFactory.h"
#include "PingHandler.h"
#include "ColumnInfoHandler.h"
#include <Common/config.h>
#include <Poco/URI.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Common/logger_useful.h>

namespace DB
{

ODBCBridgeHandlerFactory::ODBCBridgeHandlerFactory(const std::string & name_, size_t keep_alive_timeout_, ContextPtr context_)
    : WithContext(context_)
    , log(&Poco::Logger::get(name_))
    , name(name_)
    , keep_alive_timeout(keep_alive_timeout_)
{
}

std::unique_ptr<HTTPRequestHandler> ODBCBridgeHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    Poco::URI uri{request.getURI()};
    LOG_TRACE(log, "Request URI: {}", uri.toString());

    if (uri.getPath() == "/ping" && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        return std::make_unique<PingHandler>(keep_alive_timeout);

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {

        if (uri.getPath() == "/columns_info")
#if USE_ODBC
            return std::make_unique<ODBCColumnsInfoHandler>(keep_alive_timeout, getContext());
#else
            return nullptr;
#endif
        else if (uri.getPath() == "/identifier_quote")
#if USE_ODBC
            return std::make_unique<IdentifierQuoteHandler>(keep_alive_timeout, getContext());
#else
            return nullptr;
#endif
        else if (uri.getPath() == "/schema_allowed")
#if USE_ODBC
            return std::make_unique<SchemaAllowedHandler>(keep_alive_timeout, getContext());
#else
            return nullptr;
#endif
        else if (uri.getPath() == "/write")
            return std::make_unique<ODBCHandler>(keep_alive_timeout, getContext(), "write");
        else
            return std::make_unique<ODBCHandler>(keep_alive_timeout, getContext(), "read");
    }
    return nullptr;
}
}
