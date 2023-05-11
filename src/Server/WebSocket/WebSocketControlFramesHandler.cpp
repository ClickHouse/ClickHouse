#include <Server/WebSocket/WebSocketControlFramesHandler.h>

namespace DB
{

void WebSocketControlFramesHandler::handleRequest(int opcode, std::string &payload, WebSocket & webSocket)
{
    Poco::Buffer<char> pong_frame(0);
    switch (opcode)
    {
        case WebSocket::FRAME_OP_CLOSE:
            webSocket.close();
            return;
        case WebSocket::FRAME_OP_PING:
            pong_frame.append(payload.c_str(), payload.length());
            webSocket.sendFrame(pong_frame.begin(), static_cast<int>(pong_frame.size()), WebSocket::FRAME_OP_PONG);
            return;
        default:
            throw Poco::Exception("unknown control frame type");
    }
}

}
