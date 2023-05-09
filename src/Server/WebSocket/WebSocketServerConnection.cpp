#include <Server/WebSocket/WebSocketServerConnection.h>

#include <Poco/Util/ServerApplication.h>


namespace DB
{

void WebSocketServerConnection::run()
{
    using Poco::Util::Application;

    int flags_and_opcode = 0;
    int received_bytes = -1;

    Application& app = Application::instance();
    while (received_bytes != 0 && (flags_and_opcode & WebSocket::FRAME_OP_BITMASK) != WebSocket::FRAME_OP_CLOSE)
    {
        try {
            received_bytes = webSocket.receiveFrame(frame_buffer, flags_and_opcode);
        } catch (const Exception& e) {
            //TODO: add a reasonable exception wrapper here
            throw Exception(e);
        }
        auto opcode = flags_and_opcode & WebSocket::FRAME_OP_BITMASK;
        auto flag = flags_and_opcode & WebSocket::FRAME_FLAG_BITMASK;

        app.logger().information(
            Poco::format("Frame received (length=%d, flags=0x%x, op_flags=0x%x, frame_flags=0x%x).",
             received_bytes,
             unsigned(flags_and_opcode),
             unsigned(opcode),
             unsigned(flag)
        ));

        switch (opcode) {
            case WebSocket::FRAME_OP_CONT:
            case WebSocket::FRAME_OP_TEXT:
                message_buffer.append(frame_buffer);
                break;

            case WebSocket::FRAME_OP_PING:
            case WebSocket::FRAME_OP_CLOSE:
                message_buffer.assign(frame_buffer.begin(), frame_buffer.size());
                // TODO: call control frames handler
                control_frames_handler.handleRequest(, webSocket);
                break;

            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary data processing is not implemented yet");
        }

        if (flag == WebSocket::FRAME_FLAG_FIN) {
            // TODO: call regular message handler
            // TODO: parse JSON
            regular_handler.handleRequest(, webSocket);
        }
    }
}


WebSocket& WebSocketServerConnection::getSocket()
{
    return webSocket;
}

}
