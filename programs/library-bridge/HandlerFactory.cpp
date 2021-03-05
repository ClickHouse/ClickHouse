#include "HandlerFactory.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <common/logger_useful.h>
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
            auto library_handler = library_handlers.find(dictionary_id);

            if (library_handler == library_handlers.end())
            {
                auto library_handler_ptr = std::make_shared<SharedLibraryHandler>(dictionary_id);
                library_handlers[dictionary_id] = library_handler_ptr;

                return std::make_unique<LibraryRequestHandler>(keep_alive_timeout, context, library_handler_ptr);
            }

            return std::make_unique<LibraryRequestHandler>(keep_alive_timeout, context, library_handler->second);
        }

        return nullptr;
    }

}
