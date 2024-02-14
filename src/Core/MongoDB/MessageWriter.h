#include "QueryRequest.h"
#include "Message.h"
#include "OpReply.h"
#include <Poco/Exception.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Common/logger_useful.h>


namespace DB {
namespace MongoDB {
    

class MessageWriter {
    public:

    explicit MessageWriter(WriteBuffer& writer_) : writer(writer_) {}


    void updateRequestId() {
        ++request_id;
    }

    void writeHelloCmd(BSON::Document::Ptr content, Int32 response_to) {
        MessageHeader header(MessageHeader::OP_REPLY);
        header.setRequestID(request_id);
        updateRequestId();
        header.setResponseTo(response_to);
        OpReply reply(header);
        reply.setResponseFlags(0)
            .setCursorId(0)
            .setStartingFrom(0)
            .setNumberReturned(1)
            .addDocument(content);
        reply.send(writer);
        writer.next();
    }

    private:
        WriteBuffer& writer;
        Int32 request_id = 10; //FIXME ???
};


}} // namespace DB::MongoDB
