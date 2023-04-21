#include "WebSocketHandler.h"
#include "IServer.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/WebSocket.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/HTTPHandlerFactory.h>


#include <IO/HTTPCommon.h>

namespace DB
{

using Poco::IOException;

WebSocketRequestHandler::WebSocketRequestHandler(DB::IServer & server_)
    : server(server_)
{
}

void WebSocketRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    try {
        auto webSocket = Poco::Net::WebSocket(
            reinterpret_cast<Poco::Net::HTTPServerRequest &>(request),
            reinterpret_cast<Poco::Net::HTTPServerResponse &>(response)
        );

        std::cout << "Socket fd is " << webSocket.sockfd() << std::endl;
        webSocket.shutdown();
    } catch (const IOException& e) {
        std::cerr << "EXCEP " << e.what() << std::endl;
    }
}

///probably here should be "createWebSocketHandlerFactory" similary to prometeus handler

HTTPRequestHandlerFactoryPtr
createWebSocketMainHandlerFactory(IServer & server, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);


    auto main_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<WebSocketRequestHandler>>(server);
    main_handler->attachNonStrictPath("/");
    main_handler->allowGetAndHeadRequest();
    factory->addHandler(main_handler);

    return factory;
}
}
