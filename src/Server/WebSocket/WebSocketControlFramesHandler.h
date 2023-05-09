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
    void handleRequest(std::string & /*request*/, WebSocket & /*webSocket*/);
};
}
