#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Common/logger_useful.h>


namespace DB
{

class LibraryBridgeHandlerFactory : public HTTPRequestHandlerFactory, WithContext
{
public:
    LibraryBridgeHandlerFactory(
        const std::string & name_,
        ContextPtr context_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    LoggerPtr log;
    const std::string name;
};

}
