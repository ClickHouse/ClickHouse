#pragma once

#include <Interpreters/Context_fwd.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Poco/Logger.h>


#include <mutex>
#include <unordered_map>


namespace DB
{
/** Main handler for requests to ODBC driver
  * requires connection_string and columns in request params
  * and also query in request body
  * response in RowBinary format
  */
class ODBCHandler : public HTTPRequestHandler, WithContext
{
public:
    ODBCHandler(
        ContextPtr context_,
        const String & mode_)
        : WithContext(context_)
        , log(getLogger("ODBCHandler"))
        , mode(mode_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;

    String mode;

    static inline std::mutex mutex;

    void processError(HTTPServerResponse & response, const std::string & message);
};

}
