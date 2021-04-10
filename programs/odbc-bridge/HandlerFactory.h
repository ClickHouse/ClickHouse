#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include "ColumnInfoHandler.h"
#include "IdentifierQuoteHandler.h"
#include "MainHandler.h"
#include "SchemaAllowedHandler.h"

#include <Poco/Logger.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/Data/SessionPool.h>
#pragma GCC diagnostic pop


namespace DB
{
/** Factory for '/ping', '/', '/columns_info', '/identifier_quote', '/schema_allowed' handlers.
  * Also stores Session pools for ODBC connections
  */
class HandlerFactory : public HTTPRequestHandlerFactory
{
public:
    HandlerFactory(const std::string & name_, size_t keep_alive_timeout_, Context & context_)
        : log(&Poco::Logger::get(name_)), name(name_), keep_alive_timeout(keep_alive_timeout_), context(context_)
    {
        pool_map = std::make_shared<ODBCHandler::PoolMap>();
    }

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    Poco::Logger * log;
    std::string name;
    size_t keep_alive_timeout;
    Context & context;
    std::shared_ptr<ODBCHandler::PoolMap> pool_map;
};
}
