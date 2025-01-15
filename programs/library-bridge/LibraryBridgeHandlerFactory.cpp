#include "LibraryBridgeHandlerFactory.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Server/HTTP/HTMLForm.h>
#include "LibraryBridgeHandlers.h"


namespace DB
{
LibraryBridgeHandlerFactory::LibraryBridgeHandlerFactory(
    const std::string & name_,
    ContextPtr context_)
    : WithContext(context_)
    , log(getLogger(name_))
    , name(name_)
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
        if (uri.getPath() == "/catboost_ping")
            return std::make_unique<CatBoostLibraryBridgeExistsHandler>(getContext());
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        if (uri.getPath() == "/extdict_request")
            return std::make_unique<ExternalDictionaryLibraryBridgeRequestHandler>(getContext());
        if (uri.getPath() == "/catboost_request")
            return std::make_unique<CatBoostLibraryBridgeRequestHandler>(getContext());
    }

    return nullptr;
}
}
