#pragma once

#include "config.h"

#if USE_ODBC

#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Poco/Logger.h>


namespace DB
{

class ODBCColumnsInfoHandler : public HTTPRequestHandler, WithContext
{
public:
    ODBCColumnsInfoHandler(size_t keep_alive_timeout_, ContextPtr context_)
        : WithContext(context_)
        , log(getLogger("ODBCColumnsInfoHandler"))
        , keep_alive_timeout(keep_alive_timeout_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
    size_t keep_alive_timeout;
};

}

#endif
