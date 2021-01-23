#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

#include <Poco/Logger.h>

#if USE_ODBC

namespace DB
{

class Context;

/// This handler establishes connection to database, and retrieves whether schema is allowed.
class SchemaAllowedHandler : public HTTPRequestHandler
{
public:
    SchemaAllowedHandler(size_t keep_alive_timeout_, Context &)
        : log(&Poco::Logger::get("SchemaAllowedHandler")), keep_alive_timeout(keep_alive_timeout_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    size_t keep_alive_timeout;
};

}

#endif
