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
    explicit ODBCColumnsInfoHandler(ContextPtr context_) : WithContext(context_), log(getLogger("ODBCColumnsInfoHandler")) { }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
};

}

#endif
