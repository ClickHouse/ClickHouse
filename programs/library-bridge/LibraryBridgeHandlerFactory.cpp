#include "LibraryBridgeHandlerFactory.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Server/HTTP/HTMLForm.h>
#include "LibraryBridgeHandlers.h"


namespace DB
{
LibraryBridgeHandlerFactory::LibraryBridgeHandlerFactory(
    std::string name_,
    ContextPtr context_,
    std::vector<std::string> libraries_paths_)
    : WithContext(context_)
    , log(getLogger(name_))
    , name(name_)
    , libraries_paths(std::move(libraries_paths_))
{
}

std::unique_ptr<HTTPRequestHandler> LibraryBridgeHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    Poco::URI uri{request.getURI()};
    LOG_DEBUG(log, "Request URI: {}", uri.toString());

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
    {
        if (uri.getPath() == "/extdict_ping")
            return std::make_unique<ExternalDictionaryLibraryBridgeExistsHandler>(getContext());
        else if (uri.getPath() == "/catboost_ping")
            return std::make_unique<CatBoostLibraryBridgeExistsHandler>(getContext());
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        if (uri.getPath() == "/extdict_request")
            return std::make_unique<ExternalDictionaryLibraryBridgeRequestHandler>(getContext(), libraries_paths);
        else if (uri.getPath() == "/catboost_request")
            return std::make_unique<CatBoostLibraryBridgeRequestHandler>(getContext(), libraries_paths);
    }

    return nullptr;
}
}
