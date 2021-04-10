#include "HandlerFactory.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Server/HTTP/HTMLForm.h>
#include "Handlers.h"


namespace DB
{
    std::unique_ptr<HTTPRequestHandler> LibraryBridgeHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
    {
        Poco::URI uri{request.getURI()};
        LOG_DEBUG(log, "Request URI: {}", uri.toString());

        if (uri == "/ping" && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
            return std::make_unique<PingHandler>(keep_alive_timeout);

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
            return std::make_unique<LibraryRequestHandler>(keep_alive_timeout, getContext());

        return nullptr;
    }
}
