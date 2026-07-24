#include <Server/HTTPRequestHandlerFactoryMain.h>
#include <Server/NotFoundHandler.h>

#include <Common/logger_useful.h>

namespace DB
{

HTTPRequestHandlerFactoryMain::HTTPRequestHandlerFactoryMain(const std::string & name_)
    : log(getLogger(name_)), name(name_)
{
}

std::unique_ptr<HTTPRequestHandler> HTTPRequestHandlerFactoryMain::createRequestHandler(const HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP Request for {}. Method: {}, Address: {}, User-Agent: {}{}, Content Type: {}, Transfer Encoding: {}, X-Forwarded-For: {}",
        name, request.getMethod(), request.clientAddress().toString(), request.get("User-Agent", "(none)"),
        (request.hasContentLength() ? (", Length: " + std::to_string(request.getContentLength())) : ("")),
        request.getContentType(), request.getTransferEncoding(), request.get("X-Forwarded-For", "(none)"));

    for (auto & handler_factory : child_factories)
    {
        auto handler = handler_factory->createRequestHandler(request);
        if (handler)
            return handler;
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        return std::unique_ptr<HTTPRequestHandler>(new NotFoundHandler(hints.getHints(request.getURI())));
    }

    return nullptr;
}

}
