#pragma once

#if USE_ODBC

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/config.h>
#include <Poco/Logger.h>


namespace DB
{

class ODBCColumnsInfoHandler : public HTTPRequestHandler
{
public:
    ODBCColumnsInfoHandler(size_t keep_alive_timeout_, const Context & context_)
        : log(&Poco::Logger::get("ODBCColumnsInfoHandler"))
        , keep_alive_timeout(keep_alive_timeout_)
        , context(context_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    size_t keep_alive_timeout;
    const Context & context;
};

}

#endif
