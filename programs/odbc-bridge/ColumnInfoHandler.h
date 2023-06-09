#pragma once

#include <Common/config.h>

#if USE_ODBC

#include <Interpreters/Context_fwd.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Poco/Logger.h>


namespace DB
{

class ODBCColumnsInfoHandler : public HTTPRequestHandler, WithContext
{
public:
    ODBCColumnsInfoHandler(size_t keep_alive_timeout_, ContextPtr context_)
        : WithContext(context_)
        , log(&Poco::Logger::get("ODBCColumnsInfoHandler"))
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
