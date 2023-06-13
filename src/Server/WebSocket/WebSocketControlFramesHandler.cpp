#include <Server/WebSocket/WebSocketControlFramesHandler.h>

namespace DB
{

void WebSocketControlFramesHandler::handleRequest(int opcode, std::string &payload, WebSocket & webSocket)
{
    switch (opcode)
    {
        case WebSocket::FRAME_OP_CLOSE:
            webSocket.shutdown();
            return;
        case WebSocket::FRAME_OP_PING:
            webSocket.sendFrame(payload.c_str(), static_cast<int>(payload.length()), WebSocket::FRAME_PONG);
            return;
        default:
            throw Poco::Exception("unknown control frame type");
    }
}

}
