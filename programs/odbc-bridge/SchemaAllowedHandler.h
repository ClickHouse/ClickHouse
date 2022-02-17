#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/config.h>
#include <Poco/Logger.h>

#if USE_ODBC


namespace DB
{

class Context;

/// This handler establishes connection to database, and retrieves whether schema is allowed.
class SchemaAllowedHandler : public HTTPRequestHandler, WithContext
{
public:
    SchemaAllowedHandler(size_t keep_alive_timeout_, ContextPtr context_)
        : WithContext(context_)
        , log(&Poco::Logger::get("SchemaAllowedHandler"))
        , keep_alive_timeout(keep_alive_timeout_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    size_t keep_alive_timeout;
};

}

#endif
