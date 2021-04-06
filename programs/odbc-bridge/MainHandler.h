#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Poco/Logger.h>


namespace DB
{
/** Main handler for requests to ODBC driver
  * requires connection_string and columns in request params
  * and also query in request body
  * response in RowBinary format
  */
class ODBCHandler : public HTTPRequestHandler
{
public:
    ODBCHandler(
        size_t keep_alive_timeout_,
        const Context & context_,
        const String & mode_)
        : log(&Poco::Logger::get("ODBCHandler"))
        , keep_alive_timeout(keep_alive_timeout_)
        , context(context_)
        , mode(mode_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    Poco::Logger * log;

    size_t keep_alive_timeout;
    const Context & context;
    String mode;

    static inline std::mutex mutex;

    void processError(HTTPServerResponse & response, const std::string & message);
};

}
