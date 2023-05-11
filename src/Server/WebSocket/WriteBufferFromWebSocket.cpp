#include "WriteBufferFromWebSocket.h"
#include <sstream>
#include <iostream>
#include <Common/Exception.h>



namespace DB
{

WriteBufferFromWebSocket::WriteBufferFromWebSocket(WebSocket & ws_, bool send_progress_) : ws(ws_){

    out = std::make_unique<WriteBufferFromOStream>(data_stream);

    buffer() = out->buffer();
    position() = out->position();

    /// payload size must be specified before this class creation
    max_payload_size = ws.getMaxPayloadSize() - 8;

    send_progress = send_progress_;
}

WriteBufferFromWebSocket::~WriteBufferFromWebSocket() {
    finalize();
}

void WriteBufferFromWebSocket::closeWithException(int exception_bitcode, std::string exception_text)
{
    // TODO: remove (shutdown works from client-side only)
    ws.shutdown(exception_bitcode, exception_text);
}


void WriteBufferFromWebSocket::onProgress(const Progress & progress)
{
    std::lock_guard lock(mutex);

    accumulated_progress.incrementPiecewiseAtomically(progress);

    if (progress_watch.elapsed() >= send_progress_interval_ms * 1000000)
    {
        progress_watch.restart();

        SendDataMessage();

        if (send_progress)
            SendProgressMessage();
    }
};

void WriteBufferFromWebSocket::nextImpl(){
    if (out) {
        out->buffer() = buffer();
        out->position() = position();
        out->next();
    }
}
void WriteBufferFromWebSocket::finalizeImpl(){
    std::lock_guard lock(mutex);
    out->finalize();
    SendDataMessage(true);
    if (send_progress)
        SendProgressMessage();
}

void WriteBufferFromWebSocket::SendProgressMessage()
{
    WriteBufferFromOwnString progress_msg;
    ConstructProgressMessage(progress_msg);
    SendMessage(progress_msg.str());
}

void WriteBufferFromWebSocket::SendDataMessage(bool is_last_message)
{
    if (working_buffer.empty() and !is_last_message)
        return;
    WriteBufferFromOwnString data_msg;
    ConstructDataMessage(data_msg ,is_last_message);
    SendMessage(data_msg.str());

    data_stream.clear();
    out->buffer().resize(0);
    buffer() = out->buffer();
    position() = out->position();
}

void WriteBufferFromWebSocket::ConstructProgressMessage(WriteBuffer & msg){
    WriteBufferFromOwnString progress_msg;
    writeCString("{\"type\":\"", msg);
    writeText("\"Metrics\"", msg);
    if (!query_id.empty()){
        writeCString(",\"query_id\":\"", msg);
        writeText(query_id, msg);
    }
    writeCString(",\"data\":", msg);
    accumulated_progress.writeJSON(msg);
    writeCString("}", msg);
}

void WriteBufferFromWebSocket::ConstructDataMessage(WriteBuffer & msg, bool is_last_message){
    writeCString("{\"type\":", msg);
    writeText("\"Data\"", msg);
    if (!query_id.empty()){
        writeCString(",\"query_id\":\"", msg);
        writeText(query_id, msg);
    }
    writeCString(",\"data\":\"", msg);

    msg.write(data_stream.str().c_str(), data_stream.str().size());
    writeCString("\",\"last_message\":\"", msg);
    writeText(is_last_message, msg);
    writeCString("\"}", msg);
}

void WriteBufferFromWebSocket::SendMessage(std::string & message)
{
    bool will_be_framed = false;
    int flag = 0;
    int op = WebSocket::FRAME_OP_TEXT;
    int bytes_to_send = 0;
    do {
        int buffer_size = static_cast<int>(std::min(message.size(), static_cast<size_t>(INT_MAX)));


        ///max_payload_size is INT => will read less or equal INTMAX => static cast is safe
        bytes_to_send = std::min(max_payload_size, static_cast<int>(message.size()));

        will_be_framed = buffer_size > max_payload_size;

        ///sending last frame with FIN_FLAG
        if (!will_be_framed)
            flag = WebSocket::FRAME_FLAG_FIN;


        auto bytes_sent = ws.sendFrame(message.c_str(), bytes_to_send, op | flag);

        if (bytes_sent != bytes_to_send)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Probably Web Socket has send incomplete data, here should be some reasonable handler for this case");

        message = message.substr(bytes_sent, message.size());
        /// all next frames of this message will be sent as continue frame
        op = WebSocket::FRAME_OP_CONT;

    } while (will_be_framed);
}

}
