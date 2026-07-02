#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTPPathHints.h>

#include <vector>

namespace DB
{

/// Handle request using child handlers
class HTTPRequestHandlerFactoryMain : public HTTPRequestHandlerFactory
{
public:
    explicit HTTPRequestHandlerFactoryMain(const std::string & name_);

    void addHandler(HTTPRequestHandlerFactoryPtr child_factory) { child_factories.emplace_back(child_factory); }

    void addPathToHints(const std::string & http_path) { hints->add(http_path); }

    /// Exposes the shared hints registry so request handlers (e.g. the dynamic query handler)
    /// can include "Maybe you meant /dashboard?" suggestions in their own error responses.
    HTTPPathHintsPtr getPathHints() const { return hints; }

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    LoggerPtr log;
    std::string name;
    HTTPPathHintsPtr hints;

    std::vector<HTTPRequestHandlerFactoryPtr> child_factories;
};

}
