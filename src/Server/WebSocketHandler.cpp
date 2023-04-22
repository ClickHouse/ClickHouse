#include "WebSocketHandler.h"
#include "IServer.h"

//#include <Poco/Net/HTTPServerRequest.h>
//#include <Poco/Net/HTTPServerResponse.h>
//#include <Poco/Net/WebSocket.h>
#include <Server/WebSocket/WebSocket.h>
//#include <Poco/Util/LayeredConfiguration.h>
#include <Server/HTTPHandlerFactory.h>

#include "Poco/Util/ServerApplication.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/NetException.h"


#include <IO/HTTPCommon.h>

using Poco::Net::WebSocketException;
using Poco::Util::Application;


namespace DB
{

using Poco::IOException;

WebSocketRequestHandler::WebSocketRequestHandler(DB::IServer & server_)
    : server(server_)
{
}

void WebSocketRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{

    /// TODO handle timeout disconnect Exception
    Application& app = Application::instance();
    try
    {
        WebSocket ws(request, response);
        app.logger().information("WebSocket connection established.");
        char buffer[1024];
        int flags;
        int n;
        do
        {
            n = ws.receiveFrame(buffer, sizeof(buffer), flags);
            app.logger().information(Poco::format("Frame received (length=%d, flags=0x%x).", n, unsigned(flags)));
            ws.sendFrame(buffer, n, flags);
        }
        while (n > 0 && (flags & WebSocket::FRAME_OP_BITMASK) != WebSocket::FRAME_OP_CLOSE);
        app.logger().information("WebSocket connection closed.");
    }
    catch (WebSocketException& exc)
    {
        app.logger().log(exc);
        switch (exc.code())
        {
            case WebSocket::WS_ERR_HANDSHAKE_UNSUPPORTED_VERSION:
                response.set("Sec-WebSocket-Version", WebSocket::WEBSOCKET_VERSION);
                break;
            case WebSocket::WS_ERR_NO_HANDSHAKE:
            case WebSocket::WS_ERR_HANDSHAKE_NO_VERSION:
            case WebSocket::WS_ERR_HANDSHAKE_NO_KEY:
                response.setStatusAndReason(HTTPResponse::HTTP_BAD_REQUEST);
                response.setContentLength(0);
                response.send();
                break;
        }
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
