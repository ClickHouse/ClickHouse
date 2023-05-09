#pragma once

#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>

#include <Poco/JSON/Object.h>
#include <boost/noncopyable.hpp>

namespace DB
{

using Poco::JSON::Object;
class WebSocketControlFramesHandler : public WebSocketRequestHandler
{
public:
    void handleRequest(Object & request, WebSocket & webSocket) override;
};
}
