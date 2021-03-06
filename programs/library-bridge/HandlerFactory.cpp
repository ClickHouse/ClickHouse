#include "HandlerFactory.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <common/logger_useful.h>
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
        {
            /// Remove '/' in the beginning.
            auto dictionary_id = uri.getPath().substr(1);
            return std::make_unique<LibraryRequestHandler>(keep_alive_timeout, context, dictionary_id);
        }

        return std::make_unique<LibraryErrorResponseHandler>("Unknown request");
    }
}
