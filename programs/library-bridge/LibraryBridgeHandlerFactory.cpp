#include "LibraryBridgeHandlerFactory.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Server/HTTP/HTMLForm.h>
#include "LibraryBridgeHandlers.h"


namespace DB
{
LibraryBridgeHandlerFactory::LibraryBridgeHandlerFactory(
    const std::string & name_,
    size_t keep_alive_timeout_,
    ContextPtr context_)
    : WithContext(context_)
    , log(&Poco::Logger::get(name_))
    , name(name_)
    , keep_alive_timeout(keep_alive_timeout_)
{
}

std::unique_ptr<HTTPRequestHandler> LibraryBridgeHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    Poco::URI uri{request.getURI()};
    LOG_DEBUG(log, "Request URI: {}", uri.toString());

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
    {
        if (uri.getPath() == "/extdict_ping")
            return std::make_unique<ExternalDictionaryLibraryBridgeExistsHandler>(keep_alive_timeout, getContext());
        else if (uri.getPath() == "/catboost_ping")
            return std::make_unique<CatBoostLibraryBridgeExistsHandler>(keep_alive_timeout, getContext());
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        if (uri.getPath() == "/extdict_request")
            return std::make_unique<ExternalDictionaryLibraryBridgeRequestHandler>(keep_alive_timeout, getContext());
        else if (uri.getPath() == "/catboost_request")
            return std::make_unique<CatBoostLibraryBridgeRequestHandler>(keep_alive_timeout, getContext());
    }

    return nullptr;
}
}
