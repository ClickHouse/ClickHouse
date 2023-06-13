#pragma once

#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>

#include <Poco/JSON/Object.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class WebSocketControlFramesHandler
{
public:
    void handleRequest(int opcode, std::string & payload, WebSocket & webSocket);
};
}
