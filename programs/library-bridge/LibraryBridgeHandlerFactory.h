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
        std::string name_,
        ContextPtr context_,
        std::string libraries_path_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    LoggerPtr log;
    const std::string name;
    const std::string libraries_path;
};

}
