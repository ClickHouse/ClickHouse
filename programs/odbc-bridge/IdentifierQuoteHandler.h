#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>

#include <Poco/Logger.h>

#if USE_ODBC

/// This handler establishes connection to database, and retrieve quote style identifier
namespace DB
{

class IdentifierQuoteHandler : public HTTPRequestHandler
{
public:
    IdentifierQuoteHandler(size_t keep_alive_timeout_, Context &)
        : log(&Poco::Logger::get("IdentifierQuoteHandler")), keep_alive_timeout(keep_alive_timeout_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    size_t keep_alive_timeout;
};

}

#endif
