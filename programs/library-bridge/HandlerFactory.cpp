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

            /// Keep a map: dictionary_id -> SharedLibraryHandler.
            auto library_handler = library_handlers.find(dictionary_id);

            HTMLForm params(request);
            params.read(request.getStream());
            std::string method = params.get("method");

            if (!params.has("method"))
                return std::make_unique<LibraryErrorResponseHandler>("No 'method' in request URL");

            /// If such handler exists, then current method can be: loadAll, loadIds, loadKeys, isModified, supportsSelectiveLoad.
            if (library_handler != library_handlers.end())
                return std::make_unique<LibraryRequestHandler>(keep_alive_timeout, context, library_handler->second);

            /// If there is no such dictionary_id in map, then current method is either libNew or libClone or libDelete.
            if (method == "libNew")
            {
                auto library_handler_ptr = std::make_shared<SharedLibraryHandler>();
                library_handlers[dictionary_id] = library_handler_ptr;
                return std::make_unique<LibraryRequestHandler>(keep_alive_timeout, context, library_handler_ptr);
            }

            if (method == "libClone")
            {
                if (!params.has("other_dictionary_id"))
                    return std::make_unique<LibraryErrorResponseHandler>("No 'other_dictionary_id' in request URL");

                std::string other_dictionary_id = params.get("other_dictionary_id");
                LOG_INFO(log, "libClone from dictionaryID {} to {}", other_dictionary_id, dictionary_id);
                auto other_library_handler = library_handlers.find(other_dictionary_id);

                if (other_library_handler != library_handlers.end())
                {
                    /// libClone method for lib_data will be called in copy constructor.
                    auto other_library_handler_ptr = other_library_handler->second;
                    auto library_handler_ptr = std::make_shared<SharedLibraryHandler>(*other_library_handler_ptr);
                    library_handlers[dictionary_id] = library_handler_ptr;
                    return std::make_unique<LibraryRequestHandler>(keep_alive_timeout, context, library_handler_ptr);
                }

                /// cloneLibrary is called in copy constructor for LibraryDictionarySource.
                /// SharedLibraryHandler is removed from map only in LibraryDictionarySource desctructor.
                /// Therefore other_library_handler is supposed to always exist at this moment.
                return std::make_unique<LibraryErrorResponseHandler>("SharedLibraryHandler for dictionary to clone from does not exist");
            }

            return std::make_unique<LibraryErrorResponseHandler>("Unknown 'method' in request URL");
        }

        return std::make_unique<LibraryErrorResponseHandler>("Unknown request");
    }
}
