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
        std::vector<std::string> libraries_paths_);

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    LoggerPtr log;
    const std::string name;
    const std::vector<std::string> libraries_paths;
};

}
