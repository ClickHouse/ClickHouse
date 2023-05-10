#include "WriteBufferFromWebSocket.h"
#include <sstream>
#include <iostream>
#include <Common/Exception.h>



namespace DB
{

WriteBufferFromWebSocket::WriteBufferFromWebSocket(WebSocket & ws_, bool send_progress_) : ws(ws_){
    out->buffer() = buffer();
    out->position() = position();

    /// payload size must be specified before this class creation
    max_payload_size = ws.getMaxPayloadSize() - 8;

    send_progress = send_progress_;
}

void WriteBufferFromWebSocket::closeWithException(int exception_bitcode, std::string exception_text)
{
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
    SendDataMessage(true);
    if (send_progress)
        SendProgressMessage();
}

void WriteBufferFromWebSocket::SendProgressMessage()
{
    WriteBufferFromOwnString progress_msg;
    ConstructProgressMessage(progress_msg);
    SendMessage(progress_msg);
}

void WriteBufferFromWebSocket::SendDataMessage(bool is_last_message)
{
    if (working_buffer.empty() and !is_last_message)
        return;
    WriteBufferFromOwnString data_msg;
    ConstructDataMessage(data_msg, *out,is_last_message);
    SendMessage(data_msg);

    out.reset();
    working_buffer.resize(0);
    out->buffer() = buffer();
    out->position() = position();
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

void WriteBufferFromWebSocket::ConstructDataMessage(WriteBuffer & msg, WriteBuffer & dataBuffer, bool is_last_message){
    writeCString("{\"type\":\"", msg);
    writeText("\"Metrics\"", msg);
    if (!query_id.empty()){
        writeCString(",\"query_id\":\"", msg);
        writeText(query_id, msg);
    }
    writeCString(",\"data\":\"", msg);

    ///TODO: not necessary copy operation can be done better
    msg.write(dataBuffer.buffer().begin(), dataBuffer.buffer().size());
    writeCString("\",\"last_message\":\"", msg);
    writeText(is_last_message, msg);
    writeCString("\"}", msg);
}

void WriteBufferFromWebSocket::SendMessage(WriteBuffer & writeBufferWithData)
{
    ReadBuffer data_to_send(writeBufferWithData.buffer().begin(), writeBufferWithData.buffer().size());
    bool first_frame = true;
    bool will_be_framed = false;
    int flag = 0;
    int op = WebSocket::FRAME_OP_TEXT;
    int bytes_to_send = 0;
    do {
        int buffer_size = static_cast<int>(std::min(data_to_send.buffer().size(), static_cast<size_t>(INT_MAX)));

        char * begin_ptr = nullptr;

        ///max_payload_size is INT => will read less or equal INTMAX => static cast is safe
        bytes_to_send = static_cast<int>(data_to_send.read(begin_ptr, max_payload_size));

        will_be_framed = buffer_size > max_payload_size;

        ///sending first and last frames with FIN_FLAG
        if (!will_be_framed or first_frame)
            op = WebSocket::FRAME_FLAG_FIN;


        auto bytes_sent = ws.sendFrame(begin_ptr, bytes_to_send, op | flag);

        if (bytes_sent != bytes_to_send)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Probably Web Socket has send incomplete data, here should be some reasonable handler for this case");


        /// all next frames of this message will be sent as continue frame
        flag = WebSocket::FRAME_OP_CONT;
        first_frame = false;

    } while (!data_to_send.eof());
}

}
