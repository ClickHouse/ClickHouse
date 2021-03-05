#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Poco/Logger.h>
#include "SharedLibraryHandler.h"


namespace DB
{


/** Handler for requests to Library Dictionary Source, returns response in RowBinary format
  */
class LibraryRequestHandler : public HTTPRequestHandler
{
public:

    LibraryRequestHandler(
        size_t keep_alive_timeout_,
        Context & context_,
        SharedLibraryHandlerPtr library_handler_)
        : log(&Poco::Logger::get("LibraryRequestHandler"))
        , keep_alive_timeout(keep_alive_timeout_)
        , context(context_)
        , library_handler(library_handler_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    void processError(HTTPServerResponse & response, const std::string & message);

    Poco::Logger * log;

    size_t keep_alive_timeout;

    Context & context;

    SharedLibraryHandlerPtr library_handler;
};


/// Simple ping handler, answers "Ok." to GET request
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
