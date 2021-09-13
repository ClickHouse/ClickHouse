#pragma once

#if USE_ODBC

#    include <Interpreters/Context.h>
#    include <Server/HTTP/HTTPRequestHandler.h>
#    include <Common/config.h>

#    include <Poco/Logger.h>

/** The structure of the table is taken from the query "SELECT * FROM table WHERE 1=0".
  * TODO: It would be much better to utilize ODBC methods dedicated for columns description.
  * If there is no such table, an exception is thrown.
  */
namespace DB
{

class ODBCColumnsInfoHandler : public HTTPRequestHandler
{
public:
    ODBCColumnsInfoHandler(size_t keep_alive_timeout_, Context & context_)
        : log(&Poco::Logger::get("ODBCColumnsInfoHandler")), keep_alive_timeout(keep_alive_timeout_), context(context_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    Poco::Logger * log;
    size_t keep_alive_timeout;
    Context & context;
};

}

#endif
