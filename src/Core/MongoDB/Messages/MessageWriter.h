#pragma once
#include <IO/WriteBufferFromPocoSocket.h>
#include <Poco/Exception.h>
#include <Common/logger_useful.h>
#include "Message.h"
#include "OpMsgMessage.h"
#include "OpReply.h"
#include "QueryRequest.h"


namespace DB
{
namespace MongoDB
{


class MessageWriter
{
public:
    explicit MessageWriter(WriteBuffer & writer_) : writer(writer_) { }


    void updateRequestId() { ++request_id; }

    void write(BSON::Document::Ptr content, Int32 response_to)
    {
        LOG_INFO(getLogger("MessageWriter"), "Sending message to response_to: {}, content: {}", response_to, content->toString());

        MessageHeader header(MessageHeader::OP_MSG);
        header.setRequestID(request_id);
        updateRequestId();
        header.setResponseTo(response_to);
        OpMsgMessage message(header);
        message.setBody(content);
        message.send(writer);
        writer.next();
    }

    void write_op_reply(BSON::Document::Ptr content, Int32 response_to)
    {
        LOG_INFO(getLogger("MessageWriter"), "Sending message to response_to: {}, content: {}", response_to, content->toString());
        MessageHeader header(MessageHeader::OP_REPLY);
        header.setRequestID(request_id);
        updateRequestId();
        header.setResponseTo(response_to);
        OpReply reply(header);
        reply.setResponseFlags(8).setCursorId(0).setStartingFrom(0).setNumberReturned(1).addDocument(content);
        reply.send(writer);
        writer.next();
    }

private:
    WriteBuffer & writer;
    Int32 request_id = 10; //FIXME ???
};


}
}
