#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Common/logger_useful.h>


namespace DB
{

class SharedLibraryHandler;
using SharedLibraryHandlerPtr = std::shared_ptr<SharedLibraryHandler>;

/// Factory for '/ping', '/' handlers.
class LibraryBridgeHandlerFactory : public HTTPRequestHandlerFactory, WithContext
{
public:
    LibraryBridgeHandlerFactory(
            const std::string & name_,
            size_t keep_alive_timeout_,
            ContextPtr context_)
        : WithContext(context_)
        , log(&Poco::Logger::get(name_))
        , name(name_)
        , keep_alive_timeout(keep_alive_timeout_)
    {
    }

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    Poco::Logger * log;
    std::string name;
    size_t keep_alive_timeout;
};

}
