#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>

#include <vector>

namespace DB
{

/// Handle request using child handlers
class HTTPRequestHandlerFactoryMain : public HTTPRequestHandlerFactory
{
public:
    explicit HTTPRequestHandlerFactoryMain(const std::string & name_);

    void addHandler(HTTPRequestHandlerFactoryPtr child_factory) { child_factories.emplace_back(child_factory); }

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    Poco::Logger * log;
    std::string name;

    std::vector<HTTPRequestHandlerFactoryPtr> child_factories;
};

}
