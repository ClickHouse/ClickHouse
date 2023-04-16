#include "WebSocketHandler.h"
#include "IServer.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/WebSocket.h>
#include <Poco/Util/LayeredConfiguration.h>

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
}
