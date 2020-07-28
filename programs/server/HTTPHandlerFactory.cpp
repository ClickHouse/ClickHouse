#include "HTTPHandlerFactory.h"


namespace DB
{

HTTPRequestHandlerFactoryMain::HTTPRequestHandlerFactoryMain(IServer & server_, const std::string & name_)
    : server(server_), log(&Logger::get(name_)), name(name_)
{
}

Poco::Net::HTTPRequestHandler * HTTPRequestHandlerFactoryMain::createRequestHandler(
    const Poco::Net::HTTPServerRequest & request) // override
{
    LOG_TRACE(log, "HTTP Request for " << name << ". "
        << "Method: "
        << request.getMethod()
        << ", Address: "
        << request.clientAddress().toString()
        << ", User-Agent: "
        << (request.has("User-Agent") ? request.get("User-Agent") : "none")
        << (request.hasContentLength() ? (", Length: " + std::to_string(request.getContentLength())) : (""))
        << ", Content Type: " << request.getContentType()
        << ", Transfer Encoding: " << request.getTransferEncoding());

    for (auto & handlerFactory: child_handler_factories)
    {
        auto handler = handlerFactory->createRequestHandler(request);
        if (handler != nullptr)
            return handler;
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        return new NotFoundHandler;
    }

    return nullptr;
}

}
