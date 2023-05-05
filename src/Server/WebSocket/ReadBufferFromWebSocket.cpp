#include <Server/WebSocket/ReadBufferFromWebSocket.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/JSON/Parser.h>
#include "Poco/Util/ServerApplication.h"

using Poco::Util::Application;

namespace DB
{

bool ReadBufferFromWebSocket::GetNextMessageFromWebSocket(){
    int flags;
    int recived_bytes;
    int data_type_mask = WebSocket::FRAME_OP_TEXT | WebSocket::FRAME_OP_BINARY;
    int data_frame_mask = data_type_mask | WebSocket::FRAME_OP_CONT;


    int data_type = 0;
    Application& app = Application::instance();
    do
    {
        recived_bytes = ws.receiveFrame(poco_buffer, flags);

        int op_flags = flags & WebSocket::FRAME_OP_BITMASK;
        int frame_flags = flags & 0xf0;

        app.logger().information(Poco::format("Frame received (length=%d, flags=0x%x, op_flags=0x%x, frame_flags=0x%x).", recived_bytes, unsigned(flags), unsigned(op_flags), unsigned(frame_flags)));

        if (op_flags == WebSocket::FRAME_OP_PING)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WebSocket PONG not implemented yet");
        else
        {
            if ((op_flags ==  WebSocket::FRAME_OP_TEXT) || (op_flags == WebSocket::FRAME_OP_BINARY))
                data_type = data_type_mask & flags;
            if (frame_flags == WebSocket::FRAME_FLAG_FIN && (op_flags & data_frame_mask))
                break;
        }


    }while(recived_bytes > 0 && (flags & WebSocket::FRAME_OP_BITMASK) != WebSocket::FRAME_OP_CLOSE);

    if (data_type != 0){
        if (data_type == WebSocket::FRAME_OP_TEXT)
        {
//            Poco::JSON::Parser parser;
//            parser.parse(buffer.begin());
            ///TODO: implement some multimessage system
            working_buffer = Buffer(poco_buffer.begin(), poco_buffer.end());

            return true;
        } else {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Binary data processing is not implemented yet");
        }
    } else {
        throw Exception(WebSocket::WS_ERR_INCOMPLETE_FRAME, "Data type is undefined");
    }

}

    bool ReadBufferFromWebSocket::nextImpl(){
    if (working_buffer.empty() and !last_message_recived) {

        poco_buffer.setCapacity(0,false);

        last_message_recived = GetNextMessageFromWebSocket();

    } else if (last_message_recived) {
        return false;
    }

    return !working_buffer.empty();
}

}
