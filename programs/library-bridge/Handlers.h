#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <common/logger_useful.h>
#include "SharedLibraryHandler.h"


namespace DB
{


/// Handler for requests to Library Dictionary Source, returns response in RowBinary format
class LibraryRequestHandler : public HTTPRequestHandler
{
public:

    LibraryRequestHandler(
        size_t keep_alive_timeout_,
        Context & context_)
        : log(&Poco::Logger::get("LibraryRequestHandler"))
        , keep_alive_timeout(keep_alive_timeout_)
        , context(context_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    void processError(HTTPServerResponse & response, const std::string & message);

    Poco::Logger * log;
    size_t keep_alive_timeout;
    Context & context;
};


class PingHandler : public HTTPRequestHandler
{
public:
    explicit PingHandler(size_t keep_alive_timeout_)
        : keep_alive_timeout(keep_alive_timeout_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    size_t keep_alive_timeout;
};

}
