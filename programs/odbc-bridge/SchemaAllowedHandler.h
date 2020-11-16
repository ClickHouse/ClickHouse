#pragma once

#include <Poco/Logger.h>
#include <Poco/Net/HTTPRequestHandler.h>

#if USE_ODBC

namespace DB
{
class Context;


/// This handler establishes connection to database, and retrieve whether schema is allowed.
class SchemaAllowedHandler : public Poco::Net::HTTPRequestHandler
{
public:
    SchemaAllowedHandler(size_t keep_alive_timeout_, Context &)
        : log(&Poco::Logger::get("SchemaAllowedHandler")), keep_alive_timeout(keep_alive_timeout_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    size_t keep_alive_timeout;
};

}

#endif
