#include <Server/WebSocket/WebSocketServerConnection.h>

#include <Poco/JSON/Object.h>


namespace DB
{

void WebSocketServerConnection::run()
{
    using Poco::JSON::Object;
    int flags_and_opcode = 0;
    int received_bytes = -1;

    while (!connection_closed && received_bytes != 0 && (flags_and_opcode & WebSocket::FRAME_OP_BITMASK) != WebSocket::FRAME_OP_CLOSE)
    {
        try {
            received_bytes = webSocket.receiveFrame(frame_buffer, flags_and_opcode);
        } catch (const Exception& e) {
            //TODO: add a reasonable exception wrapper here
            throw Exception(e);
        }
        auto opcode = flags_and_opcode & WebSocket::FRAME_OP_BITMASK;
        auto flag = flags_and_opcode & WebSocket::FRAME_FLAG_BITMASK;


        auto str1 = std::string(message_buffer.begin(), message_buffer.end());
        logger_.information(
            Poco::format("Frame received (length=%d, flags_and_opcode=0x%x, opсode=0x%x, frame_flag=0x%x).",
             received_bytes,
             unsigned(flags_and_opcode),
             unsigned(opcode),
             unsigned(flag),
             str1
        ));

        bool handling_control_message = false;
        switch (opcode) {
            case WebSocket::FRAME_OP_CONT:
            case WebSocket::FRAME_OP_TEXT:
                message_buffer.append(frame_buffer);
                break;

            case WebSocket::FRAME_OP_CLOSE:
                connection_closed = true;
                FMT_FALLTHROUGH;
            case WebSocket::FRAME_OP_PING:
                handling_control_message = true;
                message_buffer.assign(frame_buffer.begin(), frame_buffer.size());
                try {
                    std::string request(message_buffer.begin(), message_buffer.size());
                    control_frames_handler.handleRequest(opcode, request, webSocket);
                } catch (const Exception& e) {
                    //TODO: add a reasonable exception wrapper here
                    throw Exception(e);
                }
                break;

            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary data processing is not implemented yet");
        }

        if (!handling_control_message && flag == WebSocket::FRAME_FLAG_FIN) {
            try {
                Object::Ptr request = validateRequest(std::string(message_buffer.begin(), message_buffer.end()));
                regular_handler.handleRequest(request, webSocket);

                /// DO NOT CHANGE SETCAPACITY TO CLEAR, CLEAR SETTS BUFFER TO FINILIZED STATE AND BUFFER CAN NOT BE REUSED IN FUTURE
                message_buffer.setCapacity(0);

            } catch (const Exception& e) {
                int err_code = e.code();
                logger_.information(Poco::format("err code: %d", err_code));
                // INVALID_JSON_FORMAT
                if (err_code == 692) {
                    sendErrorMessage("invalid json format");
                }
                // UNKNOWN_MESSAGE_TYPE
                else if (err_code == 693) {
                    sendErrorMessage("unknown message type");
                } else {
                    //TODO: add a reasonable exception wrapper here
                    throw Exception(e);
                }
            }
        }

        /// DO NOT CHANGE SETCAPACITY TO CLEAR, CLEAR SETTS BUFFER TO FINILIZED STATE AND BUFFER CAN NOT BE REUSED IN FUTURE
        frame_buffer.setCapacity(0);
    }
}

void WebSocketServerConnection::start()
{
    try {
        run();
    } catch (Exception& e) {
        webSocket.shutdown();
        throw Exception(e);
    }
}


WebSocket& WebSocketServerConnection::getSocket()
{
    return webSocket;
}

Poco::SharedPtr<Poco::JSON::Object> WebSocketServerConnection::validateRequest(std::string rawRequest)
{
    using Poco::JSON::Object;

    Poco::SharedPtr<Poco::JSON::Object> ret;
    try {
        logger_.information(Poco::format("Raw request: %s", rawRequest));
        ret = parser.parse(rawRequest).extract<Object::Ptr>();
    } catch(...) {
        throw Exception(ErrorCodes::INVALID_JSON_FORMAT, "invalid json format");
    }

    if (!ret->has("type")) {
        throw Exception(ErrorCodes::UNKNOWN_MESSAGE_TYPE, "unknown message type");
    }

    std::string raw_msg_type = ret->getValue<std::string>("type");
    if (Message::getMessageType(raw_msg_type) == Message::Types::Unknown) {
        throw Exception(ErrorCodes::UNKNOWN_MESSAGE_TYPE, "unknown message type");
    }

    return ret;
}

void WebSocketServerConnection::sendErrorMessage(std::string msg)
{
    WriteBufferFromWebSocket error_report(webSocket,"error");
    error_report.write(msg.c_str(), static_cast<int>(msg.size()));
    error_report.next();
    error_report.finalize();
    //webSocket.sendFrame(stringified_json.c_str(), static_cast<int>(stringified_json.size()), WebSocket::FRAME_TEXT);
}

}
